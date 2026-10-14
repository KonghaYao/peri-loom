use std::fmt;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Value;

use super::client::{random_id, Client};
use super::types::SdkError;

struct State {
    closed: bool,
    unusable: bool,
}

pub struct Session {
    client: Arc<Client>,
    identifier: String,
    readonly: bool,
    state: Mutex<State>,
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GatewaySession { identity: [redacted] }")
    }
}

impl Session {
    pub(super) fn new(client: Arc<Client>, identifier: String, readonly: bool) -> Self {
        Self {
            client,
            identifier,
            readonly,
            state: Mutex::new(State {
                closed: false,
                unusable: false,
            }),
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    pub fn execute(&self, sql: &str, request_id: Option<&str>) -> Result<(), SdkError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Executed {
            executed: bool,
        }
        self.sql::<Executed>("execute", sql, request_id, |reply| reply.executed)
            .map(|_| ())
    }

    pub fn query(&self, sql: &str, request_id: Option<&str>) -> Result<Vec<Vec<Value>>, SdkError> {
        self.sql("query", sql, request_id, |_| true)
    }

    fn sql<Output: serde::de::DeserializeOwned>(
        &self,
        action: &str,
        sql: &str,
        request_id: Option<&str>,
        valid: impl FnOnce(&Output) -> bool,
    ) -> Result<Output, SdkError> {
        let identifier = request_id
            .map(String::from)
            .map(Ok)
            .unwrap_or_else(random_id)?;
        if !super::types::label(&identifier) {
            return Err(SdkError::new("invalid_request_id", "", false));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| SdkError::new("client_unavailable", &identifier, false))?;
        if state.closed || state.unusable {
            return Err(SdkError::new("session_unusable", &identifier, false));
        }
        if sql.is_empty() {
            return Err(SdkError::new("invalid_sql", &identifier, false));
        }
        let reply = self.client.call(
            &format!("/v1/sql/{action}"),
            &identifier,
            &serde_json::json!({"session":self.identifier,"sql":sql,"request_id":identifier}),
        );
        let reply = reply.and_then(|reply| {
            if valid(&reply) {
                Ok(reply)
            } else {
                Err(SdkError::new("outcome_unknown", &identifier, true))
            }
        });
        if reply
            .as_ref()
            .is_err_and(|failure| failure.outcome_unknown || failure.code == "session_moved")
        {
            state.unusable = true;
        }
        reply
    }

    pub fn close(&self) -> Result<(), SdkError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SdkError::new("client_unavailable", "", false))?;
        if state.closed {
            return Ok(());
        }
        let identifier = random_id()?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Closed {
            closed: bool,
        }
        let reply: Closed = self
            .client
            .call(
                "/v1/sql/close",
                &identifier,
                &serde_json::json!({"session":self.identifier,"request_id":identifier}),
            )
            .inspect_err(|failure| {
                if failure.outcome_unknown || failure.code == "session_moved" {
                    state.unusable = true;
                }
            })?;
        if !reply.closed {
            state.unusable = true;
            return Err(SdkError::new("outcome_unknown", &identifier, true));
        }
        state.closed = true;
        Ok(())
    }
}
