#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcResponse {
    pub version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub precondition_failed: bool,
}
