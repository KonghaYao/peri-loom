use std::collections::BTreeMap;

use anyhow::{ensure, Result};

use crate::ownership::types::validate_label;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    Control,
    Worker(String),
}

pub struct AuthConfig {
    control: String,
    workers: BTreeMap<String, String>,
}

impl AuthConfig {
    pub fn new(control: String, workers: BTreeMap<String, String>) -> Result<Self> {
        valid_token(&control)?;
        ensure!(
            !workers.is_empty() && workers.len() <= 1024,
            "invalid worker credential count"
        );
        let mut tokens = std::collections::BTreeSet::from([control.clone()]);
        for (worker, token) in &workers {
            validate_label(worker)?;
            valid_token(token)?;
            ensure!(
                tokens.insert(token.clone()),
                "credentials must be unique across principals"
            );
        }
        Ok(Self { control, workers })
    }

    pub fn authenticate(&self, authorization: &str) -> Result<Principal> {
        let supplied = authorization.strip_prefix("Bearer ").unwrap_or("");
        if token_matches(supplied, &self.control) {
            return Ok(Principal::Control);
        }
        for (worker, expected) in &self.workers {
            if token_matches(supplied, expected) {
                return Ok(Principal::Worker(worker.clone()));
            }
        }
        anyhow::bail!("Unauthorized");
    }
}

fn valid_token(token: &str) -> Result<()> {
    ensure!(
        (32..=256).contains(&token.len()) && token.bytes().all(|byte| byte.is_ascii_graphic()),
        "credentials must contain 32..256 non-whitespace ASCII bytes"
    );
    Ok(())
}

fn token_matches(supplied: &str, expected: &str) -> bool {
    let supplied = blake3::hash(supplied.as_bytes());
    let expected = blake3::hash(expected.as_bytes());
    supplied
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}
