#[path = "admission.rs"]
mod admission;
#[path = "admission_gate.rs"]
mod admission_gate;
#[path = "namespace_backend.rs"]
mod backend;
#[path = "ownership_catalog.rs"]
pub(super) mod catalog_rules;
#[path = "lease_commands.rs"]
mod commands;
#[path = "fenced_store.rs"]
mod fenced;
#[path = "ownership_maintenance.rs"]
mod maintenance;

use self::admission::{Guard, Permission};
use super::{unique_id, Authority, CatalogRoot};
use crate::{
    authority_rpc::protocol::MaintenanceOperation, namespace::Namespace, object_store::ObjectStore,
    ownership::types::*, runtime::DatabaseId,
};
use anyhow::{anyhow, ensure, Context, Result};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
};

pub struct OwnershipAuthority {
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn LeaseClock>,
    namespaces: Mutex<BTreeMap<Namespace, Arc<NamespaceCell>>>,
}

type NamespaceCell = OnceLock<std::result::Result<Arc<Service>, String>>;

struct Service {
    authority: Arc<Authority>,
    backend: Arc<backend::NamespaceBackend>,
    clock: Arc<dyn LeaseClock>,
    gate: admission_gate::Admission,
    memory: Mutex<Memory>,
    authority_term: u64,
    incarnation: String,
}

#[derive(Default)]
struct Memory {
    blocked: bool,
    last_clock: Option<u64>,
    leases: BTreeMap<String, LiveLease>,
    nonces: BTreeMap<String, Nonce>,
    gc_marks: BTreeMap<String, u64>,
}

#[derive(Clone)]
struct LiveLease {
    binding: LeaseBinding,
    deadline: u64,
}

struct Nonce {
    digest: String,
    expires: u64,
    used: bool,
    branch_incarnation: u64,
    root_incarnation: String,
    owner: Option<OwnerRecord>,
}

impl OwnershipAuthority {
    pub fn new(store: Arc<dyn ObjectStore>, clock: Arc<dyn LeaseClock>) -> Arc<Self> {
        Arc::new(Self {
            store,
            clock,
            namespaces: Mutex::new(BTreeMap::new()),
        })
    }

    fn service(&self, identifier: &DatabaseId) -> Result<Arc<Service>> {
        identifier.validate()?;
        let cell = self
            .namespaces
            .lock()
            .map_err(|_| anyhow!("Unavailable namespace registry"))?
            .entry(identifier.namespace.clone())
            .or_default()
            .clone();
        cell.get_or_init(|| {
            (|| {
                ensure!(
                    self.store.supports_strict_fencing(),
                    "UncertainUnsupported backend may hide unsettled mutation retries"
                );
                let backend = backend::NamespaceBackend::new(self.store.clone());
                let authority = Authority::open(backend.clone(), identifier.namespace.clone())?;
                backend.available()?;
                let snapshot = authority.boot_ownership()?;
                backend.available()?;
                let catalog = snapshot
                    .root
                    .ownership
                    .as_ref()
                    .context("ownership migration missing")?;
                let clock_anchor = self.clock.now_ms()?;
                Ok(Arc::new(Service {
                    authority,
                    backend,
                    clock: self.clock.clone(),
                    gate: admission_gate::Admission::default(),
                    memory: Mutex::new(Memory {
                        last_clock: Some(clock_anchor),
                        gc_marks: snapshot
                            .root
                            .tombstones
                            .keys()
                            .map(|blob| (blob.clone(), clock_anchor))
                            .collect(),
                        ..Memory::default()
                    }),
                    authority_term: catalog.authority_term,
                    incarnation: snapshot.root.incarnation,
                }))
            })()
            .map_err(|error: anyhow::Error| format!("{error:#}"))
        })
        .clone()
        .map_err(|error| anyhow!(error))
    }

    pub fn challenge(&self, intent: LeaseIntent) -> Result<NonceReply> {
        let identifier = match &intent {
            LeaseIntent::Grant(intent) => &intent.database,
            LeaseIntent::Renew(intent) => &intent.binding.database,
        };
        let service = self.service(identifier)?;
        let _ticket = service.gate.read()?;
        service.available()?;
        commands::validate_intent(&service, &intent)?;
        let root = service.authority.snapshot()?.root;
        let branch_incarnation = root
            .branches
            .get(&identifier.branch)
            .context("branch missing")?
            .incarnation;
        let owner = root
            .ownership
            .as_ref()
            .context("ownership missing")?
            .branches
            .get(&identifier.branch)
            .cloned();
        let now = service.now()?;
        let expires = match now.checked_add(5000) {
            Some(expires) => expires,
            None => {
                service.block();
                anyhow::bail!("Unavailable nonce deadline overflow");
            }
        };
        let nonce = unique_id()?;
        let mut memory = service
            .memory
            .lock()
            .map_err(|_| anyhow!("Unavailable lease memory"))?;
        ensure!(!memory.blocked, "UnknownOutcome namespace blocked");
        memory.nonces.retain(|_, nonce| nonce.expires > now);
        ensure!(memory.nonces.len() < 4096, "nonce capacity exceeded");
        memory.nonces.insert(
            nonce.clone(),
            Nonce {
                digest: digest(&intent)?,
                expires,
                used: false,
                branch_incarnation,
                root_incarnation: root.incarnation,
                owner,
            },
        );
        Ok(NonceReply {
            nonce,
            expires_in_ms: 5000,
        })
    }

    pub fn grant(&self, request: GrantRequest) -> Result<GrantReply> {
        commands::grant(self, request)
    }
    pub fn renew(&self, request: RenewRequest) -> Result<GrantReply> {
        commands::renew(self, request)
    }
    pub fn revoke(&self, request: RevokeRequest) -> Result<DatabaseOwnership> {
        commands::revoke(self, request)
    }

    pub fn state(&self, identifier: &DatabaseId) -> Result<DatabaseOwnership> {
        let service = self.service(identifier)?;
        let _ticket = service.gate.read()?;
        service.state(identifier)
    }

    pub fn open_session(
        &self,
        binding: &LeaseBinding,
        role: AccessRole,
    ) -> Result<Arc<dyn ObjectStore>> {
        ensure!(
            role == AccessRole::ReadOnly || binding.role == AccessRole::Writer,
            "ReadOnly grant cannot open Writer session"
        );
        let service = self.session_service(&binding.database)?;
        let _ticket = service.gate.read()?;
        service.check_binding(binding)?;
        let scoped = service.scoped(Permission::Session(binding.clone(), role));
        let inner = scoped.branch_store(&binding.database.branch)?;
        inner.suppress_drop_cleanup();
        Ok(Arc::new(fenced::FencedStore::new(
            service.clone(),
            binding.clone(),
            role,
            inner,
        )))
    }

    pub fn maintenance(
        &self,
        identifier: &DatabaseId,
        request_id: &str,
        operation: MaintenanceOperation,
    ) -> Result<serde_json::Value> {
        maintenance::run(self, identifier, request_id, operation)
    }

    pub fn validate_lease(&self, binding: &LeaseBinding, role: AccessRole) -> Result<GrantReply> {
        ensure!(
            role == AccessRole::ReadOnly || binding.role == AccessRole::Writer,
            "ReadOnly grant cannot validate Writer role"
        );
        let service = self.session_service(&binding.database)?;
        let _ticket = service.gate.read()?;
        service.check_binding(binding)?;
        let live = service.live(binding)?;
        let remaining = live
            .deadline
            .checked_sub(service.now()?)
            .context("LeaseExpired")?;
        ensure!(remaining > 0, "LeaseExpired");
        Ok(GrantReply {
            binding: live.binding,
            remaining_ttl_ms: remaining,
        })
    }

    pub fn quiescent(&self) -> bool {
        let Ok(namespaces) = self.namespaces.lock() else {
            return false;
        };
        for cell in namespaces.values() {
            let Some(Ok(service)) = cell.get() else {
                return false;
            };
            if service.available().is_err() || !service.gate.quiescent() {
                return false;
            }
        }
        true
    }

    fn session_service(&self, identifier: &DatabaseId) -> Result<Arc<Service>> {
        identifier.validate()?;
        let namespaces = self
            .namespaces
            .lock()
            .map_err(|_| anyhow!("Unavailable namespace registry"))?;
        let cell = namespaces
            .get(&identifier.namespace)
            .context("Fenced unknown namespace")?;
        cell.get()
            .context("Fenced namespace not admitted")?
            .clone()
            .map_err(|error| anyhow!(error))
    }
}

impl Service {
    fn block(&self) {
        self.memory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .blocked = true;
    }

    fn available(&self) -> Result<()> {
        self.backend.available()?;
        ensure!(
            !self
                .memory
                .lock()
                .map_err(|_| anyhow!("Unavailable lease memory"))?
                .blocked,
            "UnknownOutcome namespace blocked"
        );
        Ok(())
    }

    fn now(&self) -> Result<u64> {
        let mut memory = self
            .memory
            .lock()
            .map_err(|_| anyhow!("Unavailable lease memory"))?;
        ensure!(!memory.blocked, "UnknownOutcome namespace blocked");
        let now = match self.clock.now_ms() {
            Ok(now) => now,
            Err(error) => {
                memory.blocked = true;
                return Err(error.context("Unavailable lease clock"));
            }
        };
        if memory.last_clock.is_some_and(|previous| now < previous) {
            memory.blocked = true;
            anyhow::bail!("Unavailable monotonic clock regression");
        }
        memory.last_clock = Some(now);
        Ok(now)
    }

    fn live(&self, binding: &LeaseBinding) -> Result<LiveLease> {
        self.available()?;
        let now = self.now()?;
        let memory = self
            .memory
            .lock()
            .map_err(|_| anyhow!("Unavailable lease memory"))?;
        ensure!(!memory.blocked, "UnknownOutcome namespace blocked");
        let live = memory
            .leases
            .get(&binding.database.branch)
            .context("Fenced lease unavailable")?;
        ensure!(same_lease(&live.binding, binding), "Fenced lease identity");
        ensure!(now < live.deadline, "LeaseExpired");
        Ok(live.clone())
    }

    fn deadline(&self, ttl_ms: u64) -> Result<u64> {
        match self.now()?.checked_add(ttl_ms) {
            Some(deadline) => Ok(deadline),
            None => {
                self.block();
                anyhow::bail!("Unavailable lease deadline overflow");
            }
        }
    }

    fn check_binding(&self, binding: &LeaseBinding) -> Result<()> {
        self.live(binding)?;
        live_binding(&self.authority.snapshot()?.root, binding)?;
        self.live(binding)?;
        Ok(())
    }

    fn scoped(self: &Arc<Self>, permission: Permission) -> Arc<Authority> {
        self.authority.scoped(Arc::new(Guard {
            service: self.clone(),
            permission,
        }))
    }

    fn state(&self, identifier: &DatabaseId) -> Result<DatabaseOwnership> {
        self.available()?;
        let root = self.authority.snapshot()?.root;
        let record = root
            .ownership
            .as_ref()
            .context("ownership missing")?
            .branches
            .get(&identifier.branch);
        let Some(record) = record else {
            return Ok(DatabaseOwnership {
                database: identifier.clone(),
                mode: OwnershipMode::Detached,
                binding: None,
                epoch: 0,
                remaining_ttl_ms: 0,
            });
        };
        let now = self.now()?;
        let memory = self
            .memory
            .lock()
            .map_err(|_| anyhow!("Unavailable lease memory"))?;
        let live = memory
            .leases
            .get(&identifier.branch)
            .filter(|live| live.deadline > now && live_binding(&root, &live.binding).is_ok());
        Ok(DatabaseOwnership {
            database: identifier.clone(),
            mode: if live.is_some() {
                if live.is_some_and(|live| live.binding.role == AccessRole::ReadOnly) {
                    OwnershipMode::ReadOnly
                } else {
                    OwnershipMode::Writer
                }
            } else {
                OwnershipMode::Revoked
            },
            binding: live.map(|live| live.binding.clone()),
            epoch: record.epoch,
            remaining_ttl_ms: live.map_or(0, |live| live.deadline - now),
        })
    }
}

fn live_binding(root: &CatalogRoot, binding: &LeaseBinding) -> Result<()> {
    ensure!(
        root.namespace == binding.database.namespace
            && root.incarnation == binding.root_incarnation,
        "Fenced root identity"
    );
    let catalog = root
        .ownership
        .as_ref()
        .context("Fenced ownership missing")?;
    let record = catalog
        .branches
        .get(&binding.database.branch)
        .context("Fenced branch ownership missing")?;
    ensure!(
        catalog.authority_term == binding.authority_term
            && record.state == GrantState::Active
            && record.epoch == binding.epoch
            && record.worker_id == binding.worker_id
            && record.lease_id == binding.lease_id
            && record.grant_id == binding.grant_id
            && record.role == binding.role
            && record.branch_incarnation == binding.branch_incarnation
            && root
                .branches
                .get(&binding.database.branch)
                .is_some_and(|branch| branch.incarnation == binding.branch_incarnation),
        "Fenced ownership identity"
    );
    Ok(())
}

fn same_lease(first: &LeaseBinding, second: &LeaseBinding) -> bool {
    let mut normalized = second.clone();
    normalized.renewal_sequence = first.renewal_sequence;
    &normalized == first && second.renewal_sequence <= first.renewal_sequence
}

fn digest(value: &impl serde::Serialize) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}
