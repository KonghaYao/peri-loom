use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Namespace {
    pub tenant: String,
    pub database: String,
}

impl Namespace {
    pub fn new(tenant: impl Into<String>, database: impl Into<String>) -> Result<Self> {
        let namespace = Self {
            tenant: tenant.into(),
            database: database.into(),
        };
        validate_component(&namespace.tenant)?;
        validate_component(&namespace.database)?;
        Ok(namespace)
    }

    pub fn prefix(&self) -> String {
        format!("{}/{}/", self.tenant, self.database)
    }

    pub fn key(&self, relative: &str) -> Result<String> {
        validate_component(&self.tenant)?;
        validate_component(&self.database)?;
        ensure!(!relative.is_empty(), "empty object key");
        for component in relative.split('/') {
            ensure!(
                component != "." && component != ".." && !component.is_empty(),
                "invalid object path"
            );
            ensure!(
                component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
                "invalid object path"
            );
        }
        Ok(format!("{}{relative}", self.prefix()))
    }
}

fn validate_component(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= 128,
        "invalid namespace length"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "invalid namespace component"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_cannot_escape_or_alias() -> Result<()> {
        assert!(Namespace::new("../tenant", "db").is_err());
        let namespace = Namespace::new("tenant", "db")?;
        assert_eq!(namespace.key("segments/abc")?, "tenant/db/segments/abc");
        assert!(namespace.key("../other/HEAD").is_err());
        assert!(namespace.key("/HEAD").is_err());
        Ok(())
    }
}
