use super::*;
use crate::manifest::SEGMENT_SIZE;
use crate::object_store::{MemoryObjectStore, PutCondition, StoredObject};
use std::sync::{atomic::{AtomicBool, Ordering}, mpsc};
use std::time::Duration;
use turso_core::io::Buffer;

const BOUND: Duration = Duration::from_secs(10);
struct GateStore {
    inner: Arc<dyn ObjectStore>,
    target: String,
    armed: AtomicBool,
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
impl ObjectStore for GateStore {
    fn get(&self,key:&str)->Result<Option<StoredObject>> {
        if key == self.target && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(())?;
            self.release.lock().unwrap().recv_timeout(BOUND)?;
        }
        self.inner.get(key)
    }
    fn put(&self,key:&str,bytes:&[u8],condition:PutCondition)->Result<String>{self.inner.put(key,bytes,condition)}
    fn delete(&self,key:&str,expected:Option<&str>)->Result<()>{self.inner.delete(key,expected)}
    fn list(&self,prefix:&str)->Result<Vec<String>>{self.inner.list(prefix)}
}
struct FinallyRelease(Option<mpsc::SyncSender<()>>);
impl FinallyRelease {fn release(&mut self) {if let Some(tx)=self.0.take(){let _=tx.send(());}}}
impl Drop for FinallyRelease {fn drop(&mut self){self.release();}}
fn read(file: Arc<dyn File>, offset:u64)->Result<()> {
    let b=Arc::new(Buffer::new_temporary(32));
    ensure!(file.pread(offset,Completion::new_read(b, |_|None))?.finished(),"not finished");
    Ok(())
}
#[test]
fn rw02_file_lock_and_controls()->Result<()> {
    let patched = std::env::var_os("RW02_PATCHED").is_some();
    let backing:Arc<dyn ObjectStore>=Arc::new(MemoryObjectStore::default());
    let ns=Namespace::new("rw02","h3")?;
    let seed_cache=tempfile::tempdir()?;
    let seed=RemoteStorage::open(backing.clone(),ns.clone(),Arc::new(SegmentCache::new(seed_cache.path(),0,0)?))?;
    seed.write_at(0,&vec![1;SEGMENT_SIZE])?;
    seed.write_at(SEGMENT_SIZE as u64,&vec![2;SEGMENT_SIZE])?;
    seed.publish(WalPosition{salts:[1,2],frame_count:1},0)?;
    let target=ns.key(&format!("segments/{}",crate::manifest::digest(&vec![1;SEGMENT_SIZE])))?;
    let (enter_tx,enter_rx)=mpsc::sync_channel(1);
    let (release_tx,release_rx)=mpsc::sync_channel(1);
    let store:Arc<dyn ObjectStore>=Arc::new(GateStore{inner:backing,target,armed:AtomicBool::new(true),entered:enter_tx,release:Mutex::new(release_rx)});
    let cold=tempfile::tempdir()?;
    let other_cache=tempfile::tempdir()?;
    let io=RemoteIO::open(store.clone(),ns.clone(),cold.path(),0,0,0)?;
    let independent=RemoteIO::open(store,ns,other_cache.path(),0,0,0)?;
    assert!(!Arc::ptr_eq(&io.shared,&independent.shared));
    let main=io.open_file(&io.database_path(),OpenFlags::Create,false)?;
    let wal=io.open_file(&format!("{}-wal",io.database_path()),OpenFlags::Create,false)?;
    let other_main=main.clone();
    let independent_file=independent.open_file(&independent.database_path(),OpenFlags::Create,false)?;
    let mut cleanup=FinallyRelease(Some(release_tx));
    let slow=std::thread::spawn(move ||read(main,0).map_err(|e|format!("{e:#}")));
    let entered=enter_rx.recv_timeout(BOUND);
    if entered.is_err(){cleanup.release(); let _=slow.join(); anyhow::bail!("GET entry timeout: {entered:?}");}
    let shared_available=io.shared.gate.try_lock().is_ok();
    let mut handles=Vec::new();
    let mut outputs=Vec::new();
    for (name,file,offset) in [("same_io_wal",wal,0),("same_io_other_segment",other_main,SEGMENT_SIZE as u64),("independent_io",independent_file,SEGMENT_SIZE as u64)] {
        let (started_tx,started_rx)=mpsc::sync_channel(1);
        let (tx,rx)=mpsc::sync_channel(1);
        handles.push(std::thread::spawn(move||{let _=started_tx.send(()); let _=tx.send(read(file,offset).map_err(|e|format!("{e:#}")));}));
        outputs.push((name,started_rx,rx));
    }
    let storage=io.storage();
    let (started_tx,started_rx)=mpsc::sync_channel(1);
    let (tx,rx)=mpsc::sync_channel(1);
    handles.push(std::thread::spawn(move||{let _=started_tx.send(());let _=tx.send(storage.read_at(SEGMENT_SIZE as u64,&mut [0u8;32]).map(|_|()).map_err(|e|format!("{e:#}")));}));
    outputs.push(("direct_storage",started_rx,rx));
    let mut observations=Vec::new();
    for (name,started,rx) in &outputs {
        let started_ok=started.recv_timeout(BOUND).is_ok();
        let expected_blocked=!patched&&name.starts_with("same_io");
        let outcome=rx.recv_timeout(if expected_blocked{Duration::from_secs(1)}else{BOUND});
        observations.push((*name,started_ok,outcome));
    }
    cleanup.release();
    let slow_outcome=slow.join();
    for handle in handles {assert!(handle.join().is_ok());}
    assert!(slow_outcome.unwrap().is_ok());
    assert_eq!(shared_available,patched);
    println!("RW02 {}",serde_json::json!({"hypothesis":"H3","patched":patched,"shared_available_during_GET":shared_available,"independent_shared":true}));
    for (name,started,outcome) in observations {
        println!("RW02 {}",serde_json::json!({"hypothesis":"H3","patched":patched,"arm":name,"started":started,"completed_before_release":outcome.is_ok(),"outcome":format!("{outcome:?}")}));
        assert!(started);
        if !patched&&name.starts_with("same_io") {assert!(matches!(outcome,Err(mpsc::RecvTimeoutError::Timeout)));}
        else {assert!(matches!(outcome,Ok(Ok(()))));}
    }
    // Receivers for old blocked arms remain alive through joins, so bounded sends cannot hang.
    for (name,_,rx) in outputs {if !patched&&name.starts_with("same_io"){assert!(matches!(rx.recv_timeout(BOUND),Ok(Ok(()))));}}
    Ok(())
}

#[derive(Default)]
struct Counts { rows:Vec<serde_json::Value> }
struct CountStore {inner:Arc<dyn ObjectStore>,counts:Mutex<Counts>}
fn class(key:&str)->&'static str {if key.contains("segments/"){"segment"}else if key.contains("manifests/"){"manifest"}else if key.ends_with("/HEAD"){"HEAD"}else{"other"}}
impl ObjectStore for CountStore {
    fn get(&self,key:&str)->Result<Option<StoredObject>>{let result=self.inner.get(key)?;self.counts.lock().unwrap().rows.push(serde_json::json!({"op":"GET","class":class(key),"key":key,"bytes":result.as_ref().map_or(0,|o|o.bytes.len()),"found":result.is_some()}));Ok(result)}
    fn put(&self,key:&str,bytes:&[u8],condition:PutCondition)->Result<String>{let result=self.inner.put(key,bytes,condition)?;self.counts.lock().unwrap().rows.push(serde_json::json!({"op":"PUT","class":class(key),"key":key,"bytes":bytes.len()}));Ok(result)}
    fn delete(&self,key:&str,expected:Option<&str>)->Result<()>{self.inner.delete(key,expected)}
    fn list(&self,prefix:&str)->Result<Vec<String>>{self.inner.list(prefix)}
}
#[test]
fn rw02_publish_sparse_dedup_full()->Result<()> {
    for arm in ["sparse_distinct","sparse_identical","already_exists","full_contiguous"] {
        let store=Arc::new(CountStore{inner:Arc::new(MemoryObjectStore::default()),counts:Mutex::new(Counts::default())});
        let cache=tempfile::tempdir()?;
        let storage=RemoteStorage::open(store.clone(),Namespace::new("rw02",arm)?,Arc::new(SegmentCache::new(cache.path(),0,0)?))?;
        let position=WalPosition{salts:[1,2],frame_count:1};
        let full=arm=="full_contiguous";
        let mut changed=0;
        for i in 0..8 {
            let size=if full{SEGMENT_SIZE}else{4096};
            let value=if arm=="sparse_identical"{7}else{i as u8+1};
            storage.write_at((i*SEGMENT_SIZE)as u64,&vec![value;size])?;
            changed+=size;
        }
        if arm=="already_exists" {
            storage.publish(position,0)?;
            for i in 0..8 {storage.write_at((i*SEGMENT_SIZE)as u64,&vec![i as u8+1;4096])?;}
        }
        let dirty=storage.stats().dirty_segments;
        store.counts.lock().unwrap().rows.clear();
        storage.publish(position,0)?;
        let rows=std::mem::take(&mut store.counts.lock().unwrap().rows);
        let mut totals=serde_json::Map::new();
        for category in ["segment","manifest","HEAD","other"] {for operation in ["GET","PUT"] {
            let matching:Vec<_>=rows.iter().filter(|r|r["class"]==category&&r["op"]==operation).collect();
            totals.insert(format!("{category}_{operation}_count"),serde_json::json!(matching.len()));
            totals.insert(format!("{category}_{operation}_bytes"),serde_json::json!(matching.iter().map(|r|r["bytes"].as_u64().unwrap()).sum::<u64>()));
        }}
        let expected_puts=match arm{"sparse_identical"=>1,"already_exists"=>0,_=>8};
        assert_eq!(totals["segment_PUT_count"],expected_puts);
        assert_eq!(totals["segment_GET_count"],8);
        println!("RW02 {}",serde_json::json!({"hypothesis":"H5","arm":arm,"dirty_segments":dirty,"input_bytes":changed,"totals":totals,"events":rows}));
    }
    Ok(())
}
