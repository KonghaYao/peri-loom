from pathlib import Path
import json,shutil
root=Path(__file__).resolve().parent
lab=Path(json.loads((root.parent/'identity.json').read_text())['lab'])
h=lab/'rw01-rust'
(h/'src').mkdir(parents=True,exist_ok=True)
manifest='''[package]
name="rw01-parser"
version="0.0.0"
edition="2021"
[workspace]
[dependencies]
serde={version="1", features=["derive"]}
serde_json="1"
'''
(root/'01-rust-Cargo.toml').write_text(manifest)
(h/'Cargo.toml').write_text(manifest)
protocol=(lab/'prototypes/v1/src/authority_rpc/protocol.rs').read_text()
start=protocol.index('#[derive(Clone, Debug, Serialize, Deserialize)]\n#[serde(deny_unknown_fields)]\npub struct RpcResponse')
struct=protocol[start:protocol.index('\n}',start)+2]
(root/'01-frozen-RpcResponse.rs').write_text(struct+'\n')
source=r'''use serde::{Serialize,Deserialize,Deserializer};
use serde_json::Value;
use std::alloc::{GlobalAlloc,Layout,System};
use std::sync::atomic::{AtomicBool,AtomicU64,Ordering};
use std::hint::black_box;
use std::time::Instant;
const PROTOCOL_VERSION:u32=1;
static COUNT:AtomicBool=AtomicBool::new(false);
static BYTES:AtomicU64=AtomicU64::new(0);
static ALLOCS:AtomicU64=AtomicU64::new(0);
struct Meter;
unsafe impl GlobalAlloc for Meter {
 unsafe fn alloc(&self,l:Layout)->*mut u8 {let p=System.alloc(l);if COUNT.load(Ordering::Relaxed)&&!p.is_null(){BYTES.fetch_add(l.size() as u64,Ordering::Relaxed);ALLOCS.fetch_add(1,Ordering::Relaxed);}p}
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){System.dealloc(p,l)}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8 {let p=System.realloc(p,l,n);if COUNT.load(Ordering::Relaxed)&&!p.is_null(){BYTES.fetch_add(n as u64,Ordering::Relaxed);ALLOCS.fetch_add(1,Ordering::Relaxed);}p}
}
#[global_allocator] static GLOBAL:Meter=Meter;
__STRUCT__

// Same response fields, but deserialize_with forces result presence even for null.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictResponse {
 version:u32,request_id:String,ok:bool,
 #[serde(deserialize_with="present_nullable")]
 result:Option<Value>,
 error:Option<String>,precondition_failed:bool,
}
fn present_nullable<'de,D:Deserializer<'de>>(d:D)->Result<Option<Value>,D::Error>{Option::<Value>::deserialize(d)}
impl From<StrictResponse> for RpcResponse {
 fn from(r:StrictResponse)->Self{Self{version:r.version,request_id:r.request_id,ok:r.ok,result:r.result,error:r.error,precondition_failed:r.precondition_failed}}
}
#[derive(Debug,PartialEq)]
enum Outcome {Success(Value),Precondition,KnownRejection,Invalid}
fn validate(response:RpcResponse,request_id:&str)->Outcome {
 // Copied frozen version/identity and success/error constraints; error text/types
 // become explicit enum outcomes so only serde and serde_json are needed.
 if !(response.version==PROTOCOL_VERSION && response.request_id==request_id){return Outcome::Invalid}
 if response.ok {
  if !(!response.precondition_failed && response.error.is_none()){return Outcome::Invalid}
  return Outcome::Success(response.result.unwrap_or(Value::Null));
 }
 if !(response.result.is_none() && response.error.is_some()){return Outcome::Invalid}
 if response.precondition_failed{return Outcome::Precondition}
 Outcome::KnownRejection
}
fn dual(bytes:&[u8])->Outcome {
 let value:Value=match serde_json::from_slice(bytes){Ok(x)=>x,Err(_)=>return Outcome::Invalid};
 if !value.as_object().is_some_and(|object|object.contains_key("result")){return Outcome::Invalid}
 let response:RpcResponse=match serde_json::from_slice(bytes){Ok(x)=>x,Err(_)=>return Outcome::Invalid};
 validate(response,"request-01")
}
fn single(bytes:&[u8])->Outcome {
 let response:StrictResponse=match serde_json::from_slice(bytes){Ok(x)=>x,Err(_)=>return Outcome::Invalid};
 validate(response.into(),"request-01")
}
fn cases(){
 let base=r#"{"version":1,"request_id":"request-01","ok":true,"result":null,"error":null,"precondition_failed":false}"#;
 let variants=vec![
 ("success_null",base.to_string(),"success"),
 ("success_value",base.replace("\"result\":null","\"result\":{\"data\":\"abc\"}"),"success"),
 ("missing_result",base.replace("\"result\":null,",""),"invalid"),
 ("missing_error_success",base.replace("\"error\":null,",""),"success"),
 ("missing_precondition",base.replace(",\"precondition_failed\":false",""),"invalid"),
 ("missing_version",base.replace("\"version\":1,",""),"invalid"),
 ("missing_ok",base.replace("\"ok\":true,",""),"invalid"),
 ("version_mismatch",base.replace("\"version\":1","\"version\":2"),"invalid"),
 ("identity_mismatch",base.replace("request-01","other"),"invalid"),
 ("null_identity",base.replace("\"request-01\"","null"),"invalid"),
 ("unknown_field",base.replace("{","{\"extra\":1,"),"invalid"),
 ("duplicate_result",base.replace("\"result\":null","\"result\":17,\"result\":null"),"invalid"),
 ("duplicate_result_null_first",base.replace("\"result\":null","\"result\":null,\"result\":17"),"invalid"),
 ("duplicate_version",base.replace("\"version\":1","\"version\":1,\"version\":1"),"invalid"),
 ("duplicate_error",base.replace("\"error\":null","\"error\":null,\"error\":null"),"invalid"),
 ("nested_duplicate_result",base.replace("\"result\":null","\"result\":{\"x\":1,\"x\":2}"),"success"),
 ("error_success",base.replace("\"error\":null","\"error\":\"bad\""),"invalid"),
 ("precondition_success",base.replace("\"precondition_failed\":false","\"precondition_failed\":true"),"invalid"),
 ("error_null",base.replace("\"ok\":true","\"ok\":false"),"invalid"),
 ("known_error",base.replace("\"ok\":true","\"ok\":false").replace("\"error\":null","\"error\":\"bad\""),"known"),
 ("precondition_error",base.replace("\"ok\":true","\"ok\":false").replace("\"error\":null","\"error\":\"bad\"").replace("\"precondition_failed\":false","\"precondition_failed\":true"),"precondition"),
 ("nonnull_error_result",base.replace("\"ok\":true","\"ok\":false").replace("\"error\":null","\"error\":\"bad\"").replace("\"result\":null","\"result\":{}"),"invalid"),
 ("malformed","{bad".into(),"invalid"),
 ("trailing_json",format!("{base} null"),"invalid"),
 ("array","[]".into(),"invalid"),
 ];
 for (name,bytes,want) in variants {
  let a=dual(bytes.as_bytes());let b=single(bytes.as_bytes());
  assert_eq!(a,b,"{name}");
  let kind=match a {Outcome::Success(_)=>"success",Outcome::Precondition=>"precondition",Outcome::KnownRejection=>"known",Outcome::Invalid=>"invalid"};
  assert_eq!(kind,want,"{name}");
  println!("contract,{name},{kind},equal");
 }
}
fn main(){
 cases();
 println!("round,arm,response_bytes,iterations,ns_per_op,allocated_bytes_per_op,allocations_per_op");
 for target in [112usize,1024,1048576] {
  let shell=r#"{"version":1,"request_id":"request-01","ok":true,"result":{"data":""},"error":null,"precondition_failed":false}"#;
  let bytes=shell.replace("\"data\":\"\"",&format!("\"data\":\"{}\"","x".repeat(target.saturating_sub(shell.len())))).into_bytes();
  let n=if target>10000{100}else{50000};
  assert_eq!(dual(&bytes),single(&bytes));
  for round in 1..=6 {
   let arms:[(&str,fn(&[u8])->Outcome);2]=if round%2==1{[("dual",dual),("single",single)]}else{[("single",single),("dual",dual)]};
   for (arm,parse) in arms {
    for _ in 0..50{black_box(parse(black_box(&bytes)));}
    let start=Instant::now();
    for _ in 0..n{black_box(parse(black_box(&bytes)));}
    let ns=start.elapsed().as_nanos() as f64/n as f64;
    BYTES.store(0,Ordering::Relaxed);ALLOCS.store(0,Ordering::Relaxed);COUNT.store(true,Ordering::Relaxed);
    for _ in 0..n{black_box(parse(black_box(&bytes)));}
    COUNT.store(false,Ordering::Relaxed);
    println!("{round},{arm},{},{n},{ns:.3},{:.3},{:.3}",bytes.len(),BYTES.load(Ordering::Relaxed) as f64/n as f64,ALLOCS.load(Ordering::Relaxed) as f64/n as f64);
   }
  }
 }
}
'''.replace('__STRUCT__',struct)
(root/'01-rust-main.rs').write_text(source)
(h/'src/main.rs').write_text(source)
