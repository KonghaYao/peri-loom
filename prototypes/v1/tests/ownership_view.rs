#[allow(dead_code)]
#[path = "../src/authority/p4_test_support.rs"]
mod support;

use anyhow::{bail, Result};
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority, CatalogRoot},
    capabilities::Capabilities,
    gc::GarbageCollector,
    object_store::{ObjectStore, PutCondition, StoredObject},
    ownership::types::*,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

type RootMutation = Box<dyn Fn(&mut CatalogRoot)>;

#[derive(Default)]
struct ObservedStore {
    inner: support::Store,
    reads: Mutex<Vec<String>>,
    fail_get: AtomicBool,
}

impl ObjectStore for ObservedStore {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.reads.lock().unwrap().push(key.into());
        if self.fail_get.load(Ordering::SeqCst) {
            bail!("unknown backend GET failure");
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

struct Fixture {
    store: Arc<ObservedStore>,
    clock: Arc<support::Clock>,
    service: Arc<OwnershipAuthority>,
    authority: Arc<Authority>,
    binding: LeaseBinding,
}

impl Fixture {
    fn new() -> Result<Self> {
        let store = Arc::new(ObservedStore::default());
        let clock = Arc::new(support::Clock::default());
        let service = OwnershipAuthority::new(store.clone(), clock.clone());
        let binding = support::grant(&service, 1, 0)?.binding;
        let authority = Authority::open(store.clone(), binding.database.namespace.clone())?;
        Ok(Self {
            store,
            clock,
            service,
            authority,
            binding,
        })
    }

    fn root_key(&self) -> Result<String> {
        self.binding.database.namespace.key("authority/root")
    }

    fn replace(&self, root: &CatalogRoot) -> Result<()> {
        self.store.inner.memory.put(
            &self.root_key()?,
            &envelope(&serde_json::to_string(root)?),
            PutCondition::Any,
        )?;
        Ok(())
    }

    fn validate(&self) -> Result<GrantReply> {
        self.service
            .validate_lease(&self.binding, AccessRole::Writer)
    }
}

fn envelope(serialized: &str) -> Vec<u8> {
    format!(
        "{{\"root\":{serialized},\"checksum\":\"{}\"}}",
        blake3::hash(serialized.as_bytes()).to_hex()
    )
    .into_bytes()
}

#[test]
fn lease_proof_is_fresh_root_only_and_full_tree_checks_still_fail() -> Result<()> {
    let fixture = Fixture::new()?;
    let branch = fixture.authority.branch_store("main")?;
    support::initialize(branch.clone())?;
    let capabilities = Capabilities::new(fixture.authority.clone());
    let historical = capabilities.snapshot("main", "historical")?;
    let head_key = fixture.binding.database.namespace.key("HEAD")?;
    let head = branch.get(&head_key)?.unwrap();
    branch.put(&head_key, &head.bytes, PutCondition::Any)?;
    let old_head = historical.head.unwrap();
    let snapshot = fixture.authority.snapshot()?;
    assert_ne!(
        snapshot.root.branches["main"].head.as_ref(),
        Some(&old_head)
    );
    fixture.store.reads.lock().unwrap().clear();
    fixture.validate()?;
    fixture.validate()?;
    assert_eq!(
        *fixture.store.reads.lock().unwrap(),
        vec![fixture.root_key()?, fixture.root_key()?]
    );
    fixture.store.inner.memory.put(
        &fixture
            .binding
            .database
            .namespace
            .key(&format!("authority/blobs/{}", old_head.blob))?,
        b"tampered historical HEAD",
        PutCondition::Any,
    )?;
    fixture.store.reads.lock().unwrap().clear();
    fixture.validate()?;
    assert_eq!(
        *fixture.store.reads.lock().unwrap(),
        vec![fixture.root_key()?]
    );
    assert!(fixture.authority.snapshot().is_err());
    assert!(Authority::open(
        fixture.store.clone(),
        fixture.binding.database.namespace.clone()
    )
    .is_err());
    assert!(capabilities.clone("historical", "clone").is_err());
    let collector = GarbageCollector::new(fixture.authority.clone(), Duration::ZERO);
    assert!(collector.mark().is_err());
    assert!(collector.sweep().is_err());
    assert!(fixture.authority.read_blob(&old_head).is_err());
    assert!(!fixture
        .store
        .inner
        .mutations
        .lock()
        .unwrap()
        .iter()
        .any(|entry| entry.starts_with("delete:")));
    Ok(())
}

#[test]
fn invalid_inline_identity_and_catalog_never_authorize() -> Result<()> {
    let fixture = Fixture::new()?;
    let original = fixture.authority.snapshot()?.root;
    let mutations: Vec<RootMutation> = vec![
        Box::new(|root| root.version = 1),
        Box::new(|root| root.version = 3),
        Box::new(|root| root.revision = 0),
        Box::new(|root| root.namespace.tenant = "../tenant".into()),
        Box::new(|root| root.namespace.database = "other".into()),
        Box::new(|root| root.incarnation = "invalid".into()),
        Box::new(|root| root.incarnation = "0".repeat(64)),
        Box::new(|root| root.ownership = None),
        Box::new(|root| root.ownership.as_mut().unwrap().authority_term = 0),
        Box::new(|root| root.ownership.as_mut().unwrap().authority_term += 1),
        Box::new(|root| root.branches.get_mut("main").unwrap().incarnation += 1),
        Box::new(|root| {
            root.branches.remove("main");
        }),
        Box::new(|root| {
            root.ownership.as_mut().unwrap().branches.remove("main");
        }),
        Box::new(|root| {
            let catalog = root.ownership.as_mut().unwrap();
            let mut record = catalog.branches["main"].clone();
            record.state = GrantState::Revoked;
            record.worker_id = "bad/label".into();
            catalog.branches.insert("other".into(), record);
        }),
        Box::new(|root| {
            root.ownership.as_mut().unwrap().commands.insert(
                "receipt".into(),
                CommandReceipt {
                    digest: "bad".into(),
                    result: serde_json::Value::Null,
                },
            );
        }),
        Box::new(|root| {
            root.ownership.as_mut().unwrap().commands.insert(
                "bad/label".into(),
                CommandReceipt {
                    digest: "0".repeat(64),
                    result: serde_json::Value::Null,
                },
            );
        }),
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut candidate = original.clone();
        mutate(&mut candidate);
        fixture.replace(&candidate)?;
        assert!(fixture.validate().is_err(), "inline case {index}");
    }
    fixture.replace(&original)?;
    for field in [
        "epoch",
        "worker",
        "lease",
        "grant",
        "role",
        "incarnation",
        "revoked",
    ] {
        let mut candidate = original.clone();
        let record = candidate
            .ownership
            .as_mut()
            .unwrap()
            .branches
            .get_mut("main")
            .unwrap();
        match field {
            "epoch" => record.epoch += 1,
            "worker" => record.worker_id = "other".into(),
            "lease" => record.lease_id = "other".into(),
            "grant" => record.grant_id = "other".into(),
            "role" => record.role = AccessRole::ReadOnly,
            "incarnation" => record.branch_incarnation += 1,
            "revoked" => record.state = GrantState::Revoked,
            _ => unreachable!(),
        }
        fixture.replace(&candidate)?;
        assert!(fixture.validate().is_err(), "record case {field}");
    }
    fixture.replace(&original)?;
    fixture.validate()?;
    Ok(())
}

#[test]
fn envelope_missing_root_and_backend_errors_have_no_cached_fallback() -> Result<()> {
    let fixture = Fixture::new()?;
    let key = fixture.root_key()?;
    let original = fixture.store.inner.memory.get(&key)?.unwrap();
    fixture.validate()?;
    let root = fixture.authority.snapshot()?.root;
    let mut unknown_role = serde_json::to_value(&root)?;
    unknown_role["ownership"]["branches"]["main"]["role"] = "administrator".into();
    let mut checksum_mismatch: serde_json::Value = serde_json::from_slice(&original.bytes)?;
    checksum_mismatch["root"]["revision"] = 0.into();
    let mut unknown_envelope: serde_json::Value = serde_json::from_slice(&original.bytes)?;
    unknown_envelope["unexpected"] = true.into();
    let malformed = [
        envelope(&serde_json::to_string(&unknown_role)?),
        serde_json::to_vec(&checksum_mismatch)?,
        serde_json::to_vec(&unknown_envelope)?,
        envelope(&serde_json::to_string(&root)?.replacen(
            "\"version\":4",
            "\"version\":4,\"version\":4",
            1,
        )),
    ];
    for bytes in malformed {
        fixture
            .store
            .inner
            .memory
            .put(&key, &bytes, PutCondition::Any)?;
        assert!(fixture.validate().is_err());
    }
    for bytes in [
        b"{}".as_slice(),
        b"{invalid",
        b"{\"root\":{},\"checksum\":\"bad\"}",
    ] {
        fixture
            .store
            .inner
            .memory
            .put(&key, bytes, PutCondition::Any)?;
        assert!(fixture.validate().is_err());
    }
    fixture.store.inner.memory.delete(&key, None)?;
    assert!(fixture.validate().is_err());
    assert!(fixture.store.inner.memory.get(&key)?.is_none());
    fixture
        .store
        .inner
        .memory
        .put(&key, &original.bytes, PutCondition::Any)?;
    fixture.store.fail_get.store(true, Ordering::SeqCst);
    assert!(fixture.validate().is_err());
    fixture.store.fail_get.store(false, Ordering::SeqCst);
    fixture.validate()?;
    Ok(())
}

#[test]
fn revoke_observed_during_fresh_root_read_rejects_old_live_memory() -> Result<()> {
    let fixture = Fixture::new()?;
    let mut candidate = fixture.authority.snapshot()?.root;
    candidate
        .ownership
        .as_mut()
        .unwrap()
        .branches
        .get_mut("main")
        .unwrap()
        .state = GrantState::Revoked;
    let bytes = envelope(&serde_json::to_string(&candidate)?);
    let store = fixture.store.clone();
    let key = fixture.root_key()?;
    *fixture.store.inner.before_get.lock().unwrap() = Some(support::Hook {
        needle: "authority/root".into(),
        skip: 0,
        action: Box::new(move || {
            store
                .inner
                .memory
                .put(&key, &bytes, PutCondition::Any)
                .unwrap();
        }),
    });
    fixture.store.reads.lock().unwrap().clear();
    assert!(fixture.validate().is_err());
    assert_eq!(
        *fixture.store.reads.lock().unwrap(),
        vec![fixture.root_key()?]
    );
    Ok(())
}

#[test]
fn expiry_and_clock_regression_during_root_read_fail_post_read_check() -> Result<()> {
    for instant in [10100, 99] {
        let fixture = Fixture::new()?;
        let clock = fixture.clock.clone();
        *fixture.store.inner.before_get.lock().unwrap() = Some(support::Hook {
            needle: "authority/root".into(),
            skip: 0,
            action: Box::new(move || clock.set(instant)),
        });
        fixture.store.reads.lock().unwrap().clear();
        assert!(fixture.validate().is_err());
        assert_eq!(
            *fixture.store.reads.lock().unwrap(),
            vec![fixture.root_key()?]
        );
    }
    Ok(())
}

#[test]
fn renew_preserves_old_session_identity_but_rejects_future_sequence() -> Result<()> {
    let fixture = Fixture::new()?;
    let intent = RenewIntent {
        binding: fixture.binding.clone(),
        sequence: 1,
        ttl_ms: 10000,
    };
    let nonce = fixture
        .service
        .challenge(LeaseIntent::Renew(intent.clone()))?
        .nonce;
    let reply = fixture.service.renew(RenewRequest {
        request_id: "renew".into(),
        nonce,
        intent,
    })?;
    fixture.validate()?;
    fixture
        .service
        .validate_lease(&reply.binding, AccessRole::Writer)?;
    let mut forged = reply.binding.clone();
    forged.renewal_sequence += 1;
    assert!(fixture
        .service
        .validate_lease(&forged, AccessRole::Writer)
        .is_err());
    let rewind = RenewIntent {
        binding: fixture.binding.clone(),
        sequence: 1,
        ttl_ms: 10000,
    };
    assert!(fixture
        .service
        .challenge(LeaseIntent::Renew(rewind))
        .is_err());
    Ok(())
}

#[test]
fn ambiguous_mutation_and_unsupported_backend_cannot_authorize() -> Result<()> {
    let fixture = Fixture::new()?;
    *fixture.store.inner.failure.lock().unwrap() = Some(("authority/root".into(), true));
    assert!(fixture
        .service
        .revoke(RevokeRequest {
            request_id: "revoke".into(),
            database: fixture.binding.database.clone(),
            expected_epoch: 1,
        })
        .is_err());
    assert!(fixture.validate().is_err());
    assert!(!fixture.service.quiescent());
    let store = Arc::new(ObservedStore::default());
    store.inner.unsupported.store(true, Ordering::SeqCst);
    let service = OwnershipAuthority::new(store.clone(), Arc::new(support::Clock::default()));
    assert!(service.state(&support::identifier("main")).is_err());
    assert!(store.reads.lock().unwrap().is_empty());
    Ok(())
}
