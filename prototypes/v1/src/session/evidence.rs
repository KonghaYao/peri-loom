#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::fmt;

use anyhow::Error;

use super::UnsettledSqlOutcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlEvidence {
    NotExecuted,
    Settled,
}

impl fmt::Display for SqlEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotExecuted => "SQL execution not entered",
            Self::Settled => "SQL execution settled",
        })
    }
}

impl std::error::Error for SqlEvidence {}

#[derive(Default)]
pub(super) struct ExecutionEvidence {
    entered: Cell<bool>,
    pending: Cell<bool>,
}

impl ExecutionEvidence {
    pub fn enter(&self) {
        self.entered.set(true);
        self.pending.set(true);
    }

    pub fn settle(&self) {
        self.pending.set(false);
    }

    pub fn failure(&self, error: Error) -> Error {
        // 不以错误类别猜测执行状态；未完成的原生清理始终覆盖局部证据。
        if error.downcast_ref::<UnsettledSqlOutcome>().is_some() || self.pending.get() {
            return error.context(UnsettledSqlOutcome);
        }
        if error.downcast_ref::<SqlEvidence>().is_some() {
            return error;
        }
        error.context(if self.entered.get() {
            SqlEvidence::Settled
        } else {
            SqlEvidence::NotExecuted
        })
    }
}
