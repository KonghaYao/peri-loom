use super::*;
use peri_loom_v1::authority_rpc::{
    auth::AuthConfig, protocol::AuthorityCommand, server::Server, startup::AuthorityStartup,
    AuthorityClient,
};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn namespace_init_t10_t11_shutdown_and_same_backend_seal_regression() -> Result<()> {
    for unknown in [false, true] {
        let directory = tempfile::tempdir()?;
        let state_dir = directory.path().join("state");
        let store = Arc::new(FaultStore::default());
        let id = identifier("rpc-init");
        Authority::open(store.clone(), id.namespace.clone())?;
        let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
        // Generate per-run credentials rather than embedding reusable secrets.
        let token = blake3::hash(
            &SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_nanos()
                .to_le_bytes(),
        )
        .to_hex()
        .to_string();
        let worker_token = blake3::hash(token.as_bytes()).to_hex().to_string();
        let auth = || {
            AuthConfig::new(
                token.clone(),
                BTreeMap::from([("worker".into(), worker_token.clone())]),
            )
        };
        let server = Server::bind(
            "127.0.0.1:0".parse()?,
            authority.clone(),
            auth()?,
            AuthorityStartup::acquire(&state_dir, false)?,
        )?;
        let client =
            AuthorityClient::new(&format!("http://{}/", server.local_addr()), token.clone())?;
        let handle = std::thread::spawn(move || server.run());
        if unknown {
            store.lose_boot_ack.store(true, Ordering::SeqCst);
        } else {
            store.fail(id.namespace.key("authority/root")?, 0);
        }
        assert!(authority.state(&id).is_err());
        assert_eq!(client.call(AuthorityCommand::Shutdown).is_ok(), !unknown);
        assert_eq!(handle.join().unwrap().is_ok(), !unknown);
        let marker = std::fs::read_to_string(state_dir.join("authority.lock"))?;
        assert_eq!(marker, if unknown { "dirty\n" } else { "clean\n" });
        if unknown {
            assert!(AuthorityStartup::acquire(&state_dir, false).is_err());
        }
        let restart = Server::bind(
            "127.0.0.1:0".parse()?,
            OwnershipAuthority::new(store, Clock::new(1000)),
            auth()?,
            AuthorityStartup::acquire(&state_dir, true)?,
        );
        if unknown {
            assert!(restart
                .err()
                .unwrap()
                .to_string()
                .contains("dirty backend authority seal"));
        } else {
            let server = restart?;
            let client = AuthorityClient::new(&format!("http://{}/", server.local_addr()), token)?;
            let handle = std::thread::spawn(move || server.run());
            client.call(AuthorityCommand::Shutdown)?;
            handle.join().unwrap()?;
        }
    }
    Ok(())
}
