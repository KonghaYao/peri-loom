        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid authority response"))?;
        ensure!(
            value
                .as_object()
                .is_some_and(|object| object.contains_key("result")),
            "authority result field missing"
        );
        let response: RpcResponse = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid authority response"))?;
        ensure!(
            response.version == PROTOCOL_VERSION && response.request_id == request_id,
            "authority response identity mismatch"
        );
        if response.ok {
            ensure!(
                !response.precondition_failed && response.error.is_none(),
                "inconsistent authority response"
            );
            return Ok(response.result.unwrap_or(serde_json::Value::Null));
        }
        ensure!(
            response.result.is_none() && response.error.is_some(),
            "inconsistent authority error response"
        );
        if response.precondition_failed {
            return Err(PreconditionFailed.into());
        }
        if let Some(observation) = observation {
            observation.reject(response.error.as_deref().unwrap_or_default());
        }
        Err(KnownAuthorityRejection.into())
