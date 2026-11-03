"""Statuses, terminal vocabulary and exit codes for the retail contract."""
from __future__ import annotations

from enum import Enum


class Status(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    INVALID = "INVALID"
    INCONCLUSIVE = "INCONCLUSIVE"
    BLOCKED = "BLOCKED"
    NOT_RUN = "NOT_RUN"


class Terminal(str, Enum):
    READ_OK = "read_ok"
    COMMITTED_ACK = "committed_ack"
    KNOWN_FAILURE_ROLLED_BACK = "known_failure/rolled_back"
    SESSION_RETIRED = "session_retired"
    SERVICE_UNKNOWN = "service_unknown"
    TRANSPORT_UNKNOWN = "transport_unknown"
    CLEANUP_UNCONFIRMED = "cleanup_unconfirmed"
    UNEXPECTED_ERROR = "unexpected_error"


BUSINESS_CLASSES = (
    "product_stock",
    "order_detail",
    "customer_orders",
    "stock_report",
    "checkout",
)

SUCCESS_TERMINALS = {Terminal.READ_OK.value, Terminal.COMMITTED_ACK.value}
UNKNOWN_TERMINALS = {
    Terminal.SERVICE_UNKNOWN.value,
    Terminal.TRANSPORT_UNKNOWN.value,
    Terminal.CLEANUP_UNCONFIRMED.value,
}

# Exit codes carry the acceptance rule: only PASS may exit zero.  ``summary``
# deliberately has a different, non-authoritative command name.
EXIT_CODES = {
    Status.PASS: 0,
    Status.FAIL: 1,
    Status.INVALID: 2,
    Status.INCONCLUSIVE: 3,
    Status.BLOCKED: 4,
    Status.NOT_RUN: 5,
}


class EvidenceError(RuntimeError):
    """Evidence cannot be trusted; the affected dimension is INVALID."""

    def __init__(self, message: str):
        super().__init__(message)
        self.status = Status.INVALID


class ContractError(RuntimeError):
    """The contract, the package or the effective configuration is wrong."""

    def __init__(self, message: str):
        super().__init__(message)
        self.status = Status.INVALID


class PrerequisiteError(RuntimeError):
    """A prerequisite needed for the requested execution is unavailable."""

    def __init__(self, message: str):
        super().__init__(message)
        self.status = Status.BLOCKED
