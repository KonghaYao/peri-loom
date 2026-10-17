
#[test]
fn rw02_incremental_copy_scaling() -> Result<()> {
    for frames in [64usize, 1024, 8192] {
        let store = WalStore::open(&format!("rw02-{frames}"))?;
        let salts = [91, 92];
        let mut builder = WalBuilder::new(salts);
        let first = builder.image(frames);
        let index = store.append(write_at(0, &first), Some(WalPosition { salts, frame_count: frames as u64 }))?;
        let previous = validate_index_state(&index, &store.namespace, |key| store.load(key))?;
        reset_recovery_observations();
        assert!(validate_index_append(&previous, &index, &store.namespace, |key| store.load(key)).is_err());
        let no_new = recovery_observations();
        assert_eq!(no_new.copied_bytes, 0);
        println!("RW02 {}", serde_json::json!({"hypothesis":"H4","arm":"no_new_index_rejected","frames":frames,"copied_bytes":no_new.copied_bytes,"scanned_bytes":no_new.scanned_bytes}));
        let frame = builder.frame((frames + 1) as u32, PAGE_SIZE as u32);
        let next = store.append(write_at(first.len(), &frame), Some(WalPosition {salts, frame_count:(frames + 1) as u64}))?;
        reset_recovery_observations();
        let appended = validate_index_append(&previous, &next, &store.namespace, |key| store.load(key))?;
        let observed = recovery_observations();
        assert_eq!(observed.copied_bytes, first.len() as u64);
        assert_eq!(observed.scanned_frames, 1);
        assert_eq!(observed.scanned_bytes, (WAL_HEADER + FRAME_SIZE) as u64);
        let cold = validate_index_state(&next, &store.namespace, |key| store.load(key))?;
        assert_eq!(appended.image.bytes(), cold.image.bytes());
        assert_eq!(appended.commit(), cold.commit());
        assert_eq!(previous.image.bytes(), first.as_slice());
        let metadata: serde_json::Value = serde_json::from_slice(&next)?;
        println!("RW02 {}",serde_json::json!({"hypothesis":"H4","arm":"incremental","old_frames":frames,"page_bytes":PAGE_SIZE,"frame_bytes":FRAME_SIZE,"image_bytes":first.len(),"new_frames":1,"copied_bytes":observed.copied_bytes,"scanned_bytes":observed.scanned_bytes,"scanned_frames":observed.scanned_frames,"index_json_bytes":next.len(),"entries":metadata["entries"].as_array().unwrap().len(),"receipts":metadata["receipts"].as_object().unwrap().len(),"receipt_digests":metadata["receipt_digests"].as_object().unwrap().len()}));
    }
    Ok(())
}
