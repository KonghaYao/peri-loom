use super::{DatabaseKeyProvider, InstalledKeyLease, ResolvedDatabaseKey};
use crate::authority_rpc::{
    client::random_id,
    provider::{same_identity, RemoteAuthorityProvider},
};
use crate::namespace::Namespace;
use crate::ownership::types::{validate_label, AccessRole, LeaseBinding, LeaseClock};
use crate::runtime::DatabaseId;
use anyhow::{ensure, Context, Result};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    sync::{Arc, Mutex},
    time::Duration,
};
use zeroize::Zeroizing;

pub struct BrokerDatabaseKeyProvider {
    remote: Arc<RemoteAuthorityProvider>,
    endpoint: String,
    token: Zeroizing<String>,
    clock: Arc<dyn LeaseClock>,
    http: Client,
    domains: Mutex<BTreeMap<Namespace, Domain>>,
}

struct Domain {
    root: String,
    descriptor: String,
    revision: u64,
    domain: String,
    dek: String,
    fingerprint: Zeroizing<[u8; 32]>,
}

#[derive(Serialize)]
struct KeyRequest<'a> {
    version: u32,
    request_id: &'a str,
    database: &'a DatabaseId,
    binding: &'a LeaseBinding,
    access_role: AccessRole,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyResponse {
    version: u32,
    request_id: String,
    ok: bool,
    result: Option<KeyResult>,
    error: Option<BrokerError>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerError {
    code: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyResult {
    descriptor_id: String,
    descriptor_revision: u64,
    domain_id: String,
    dek_id: String,
    cipher: String,
    dek_hex: Zeroizing<String>,
    validated_binding: LeaseBinding,
    access_role: AccessRole,
    valid_for_ms: u64,
}

impl BrokerDatabaseKeyProvider {
    pub fn new(
        remote: Arc<RemoteAuthorityProvider>,
        endpoint: &str,
        token: String,
        clock: Arc<dyn LeaseClock>,
    ) -> Result<Arc<Self>> {
        let token = Zeroizing::new(token);
        let mut url = url::Url::parse(endpoint).context("invalid key broker endpoint")?;
        let loopback = match url.host() {
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            _ => false,
        };
        ensure!(
            url.scheme() == "http" && loopback,
            "key broker requires loopback HTTP"
        );
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/",
            "invalid key broker endpoint components"
        );
        ensure!(
            token.len() >= 32
                && token.len() <= 4096
                && token.bytes().all(|byte| byte.is_ascii_graphic()),
            "invalid worker key credential"
        );
        clock.now_ms()?;
        url.set_path("/internal/key");
        let http = Client::builder()
            .no_proxy()
            .http1_only()
            .pool_max_idle_per_host(0)
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Arc::new(Self {
            remote,
            endpoint: url.to_string(),
            token,
            clock,
            http,
            domains: Mutex::new(BTreeMap::new()),
        }))
    }

    fn fresh(&self, lease: &InstalledKeyLease) -> Result<LeaseBinding> {
        let start = self.clock.now_ms()?;
        let reply = self
            .remote
            .validate_lease(&lease.binding, lease.access_role)?;
        self.deadline(start, reply.remaining_ttl_ms)?;
        Ok(reply.binding)
    }

    fn deadline(&self, start: u64, ttl: u64) -> Result<()> {
        let elapsed = self
            .clock
            .now_ms()?
            .checked_sub(start)
            .context("key clock regressed")?;
        ensure!(
            elapsed.checked_add(5).is_some_and(|spent| spent < ttl),
            "key delivery expired"
        );
        Ok(())
    }

    fn same_installed(&self, original: &InstalledKeyLease) -> Result<InstalledKeyLease> {
        let current = self
            .remote
            .installed_key_lease(&original.binding.database, original.binding.epoch)?;
        ensure!(
            same_identity(&original.binding, &current.binding)
                && current.access_role == original.access_role
                && current.binding.renewal_sequence >= original.binding.renewal_sequence,
            "installed key lease changed"
        );
        Ok(current)
    }

    fn pin_domain(&self, key: &ResolvedDatabaseKey) -> Result<()> {
        let mut domains = self
            .domains
            .lock()
            .map_err(|_| anyhow::anyhow!("key domain registry unavailable"))?;
        let fingerprint = Zeroizing::new(*blake3::hash(&key.dek).as_bytes());
        ensure!(
            domains.iter().all(|(namespace, domain)| {
                namespace == &key.binding.database.namespace
                    || (domain.descriptor != key.descriptor_id
                        && domain.domain != key.domain_id
                        && domain.dek != key.dek_id)
            }),
            "key domain belongs to another namespace"
        );
        if let Some(domain) = domains.get_mut(&key.binding.database.namespace) {
            ensure!(
                domain.root == key.binding.root_incarnation
                    && domain.descriptor == key.descriptor_id
                    && domain.domain == key.domain_id
                    && domain.dek == key.dek_id
                    && domain.revision <= key.descriptor_revision
                    && *domain.fingerprint == *fingerprint,
                "key domain mapping changed"
            );
            domain.revision = key.descriptor_revision;
        } else {
            domains.insert(
                key.binding.database.namespace.clone(),
                Domain {
                    root: key.binding.root_incarnation.clone(),
                    descriptor: key.descriptor_id.clone(),
                    revision: key.descriptor_revision,
                    domain: key.domain_id.clone(),
                    dek: key.dek_id.clone(),
                    fingerprint,
                },
            );
        }
        Ok(())
    }
}

impl DatabaseKeyProvider for BrokerDatabaseKeyProvider {
    fn resolve(&self, database: &DatabaseId, epoch: u64) -> Result<ResolvedDatabaseKey> {
        let mut lease = self.remote.installed_key_lease(database, epoch)?;
        lease.binding = self.fresh(&lease)?;
        let installed = self.same_installed(&lease)?;
        lease.binding.renewal_sequence = lease
            .binding
            .renewal_sequence
            .max(installed.binding.renewal_sequence);
        let request_id = random_id()?;
        let request = KeyRequest {
            version: 1,
            request_id: &request_id,
            database,
            binding: &lease.binding,
            access_role: lease.access_role,
        };
        let start = self.clock.now_ms()?;
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&*self.token)
            .json(&request)
            .send()
            .map_err(|_| anyhow::anyhow!("key broker transport failed"))?;
        ensure!(
            response.status() == reqwest::StatusCode::OK,
            "key broker refused delivery"
        );
        let mut bytes = Zeroizing::new(Vec::new());
        response
            .take(16385)
            .read_to_end(&mut bytes)
            .map_err(|_| anyhow::anyhow!("key broker response read failed"))?;
        ensure!(bytes.len() <= 16384, "key broker response too large");
        let response: KeyResponse = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid key broker response"))?;
        ensure!(
            response.version == 1 && response.request_id == request_id,
            "key broker correlation mismatch"
        );
        if let Some(error) = &response.error {
            let _ = &error.code;
        }
        ensure!(
            response.ok && response.error.is_none(),
            "key broker failed delivery"
        );
        let result = response.result.context("key broker result missing")?;
        ensure!(
            same_identity(&lease.binding, &result.validated_binding)
                && result.validated_binding.renewal_sequence >= lease.binding.renewal_sequence
                && result.access_role == lease.access_role,
            "key broker lease mismatch"
        );
        ensure!(
            result.cipher == "aegis256"
                && result.dek_hex.len() == 64
                && result.dek_hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid database cipher or key size"
        );
        for identifier in [&result.descriptor_id, &result.domain_id, &result.dek_id] {
            validate_label(identifier)?;
        }
        ensure!(
            result.descriptor_revision >= 2 && result.valid_for_ms <= 1000,
            "invalid key descriptor freshness"
        );
        self.deadline(start, result.valid_for_ms)?;
        let mut decoded = Zeroizing::new([0u8; 32]);
        for (index, pair) in result
            .dek_hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .enumerate()
        {
            decoded[index] = ((pair[0] as char).to_digit(16).unwrap() * 16
                + (pair[1] as char).to_digit(16).unwrap()) as u8;
        }
        let current = self.same_installed(&lease)?;
        let final_binding = self.fresh(&current)?;
        ensure!(
            final_binding.renewal_sequence >= result.validated_binding.renewal_sequence,
            "key validation renewal regressed"
        );
        self.same_installed(&current)?;
        self.deadline(start, result.valid_for_ms)?;
        let key = ResolvedDatabaseKey {
            descriptor_id: result.descriptor_id,
            descriptor_revision: result.descriptor_revision,
            domain_id: result.domain_id,
            dek_id: result.dek_id,
            binding: final_binding,
            access_role: lease.access_role,
            dek: *decoded,
        };
        self.pin_domain(&key)?;
        Ok(key)
    }
}
