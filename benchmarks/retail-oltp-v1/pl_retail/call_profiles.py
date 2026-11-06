"""Application call-shape profiles for the five fixed businesses.

A profile changes only how a business is driven over Hrana: whether a read
runs inside an explicit transaction and whether the five checkout stock
decrements travel as one statement.  The SQL results, business checks and
failure handling are shared, so a profile cannot change the data a run
produces; it changes the number of HTTP requests and the success tail.

``baseline-v1`` is the frozen v1.0 shape (3 calls per read, 13 per checkout)
and stays the default everywhere.  The application-shaped profiles are opt-in
through ``run --call-profile`` and are recorded in ``run.json`` so the scorer
checks the call counts against the profile the evidence was produced under.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import BUSINESS_CLASSES, ContractError

READ_MODES = ("transactional", "autocommit")
STOCK_MODES = ("per_item", "merged")
DEFAULT_PROFILE_NAME = "baseline-v1"


@dataclass(frozen=True)
class CallProfile:
    name: str
    read_mode: str
    stock_mode: str
    expected_http_calls: dict[str, int]

    def as_dict(self) -> dict[str, Any]:
        """The run-level record; the call table is checked by the scorer."""
        return {
            "name": self.name,
            "read_mode": self.read_mode,
            "stock_mode": self.stock_mode,
        }


PROFILES: dict[str, CallProfile] = {
    DEFAULT_PROFILE_NAME: CallProfile(
        name=DEFAULT_PROFILE_NAME,
        read_mode="transactional",
        stock_mode="per_item",
        expected_http_calls={
            "product_stock": 3,
            "order_detail": 3,
            "customer_orders": 3,
            "stock_report": 3,
            "checkout": 13,
        },
    ),
    "app-read-v1": CallProfile(
        name="app-read-v1",
        read_mode="autocommit",
        stock_mode="per_item",
        expected_http_calls={
            "product_stock": 1,
            "order_detail": 1,
            "customer_orders": 1,
            "stock_report": 1,
            "checkout": 13,
        },
    ),
    "app-stock-v1": CallProfile(
        name="app-stock-v1",
        read_mode="transactional",
        stock_mode="merged",
        expected_http_calls={
            "product_stock": 3,
            "order_detail": 3,
            "customer_orders": 3,
            "stock_report": 3,
            "checkout": 9,
        },
    ),
    "app-read-stock-v1": CallProfile(
        name="app-read-stock-v1",
        read_mode="autocommit",
        stock_mode="merged",
        expected_http_calls={
            "product_stock": 1,
            "order_detail": 1,
            "customer_orders": 1,
            "stock_report": 1,
            "checkout": 9,
        },
    ),
}


def _checked(profile: CallProfile) -> None:
    """Reject a profile that the business layer or the scorer cannot honour."""
    if profile.read_mode not in READ_MODES:
        raise ContractError(
            "profile %s: read_mode %r is not one of %s"
            % (profile.name, profile.read_mode, ", ".join(READ_MODES))
        )
    if profile.stock_mode not in STOCK_MODES:
        raise ContractError(
            "profile %s: stock_mode %r is not one of %s"
            % (profile.name, profile.stock_mode, ", ".join(STOCK_MODES))
        )
    counts = profile.expected_http_calls
    missing = [name for name in BUSINESS_CLASSES if name not in counts]
    if missing:
        raise ContractError(
            "profile %s: no call count for %s" % (profile.name, ", ".join(missing))
        )
    extra = sorted(set(counts) - set(BUSINESS_CLASSES))
    if extra:
        raise ContractError(
            "profile %s: call counts for unknown businesses %s"
            % (profile.name, ", ".join(extra))
        )
    for name in BUSINESS_CLASSES:
        value = counts[name]
        # bool is an int subclass; True would silently mean one call.
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ContractError(
                "profile %s: call count for %s must be a positive integer, got %r"
                % (profile.name, name, value)
            )


def _validate_table() -> None:
    if DEFAULT_PROFILE_NAME not in PROFILES:
        raise ContractError("the default call profile %r is missing" % DEFAULT_PROFILE_NAME)
    for name, profile in PROFILES.items():
        if profile.name != name:
            raise ContractError(
                "profile table key %r does not match the profile name %r"
                % (name, profile.name)
            )
        _checked(profile)


def resolve_profile(name: str | None) -> CallProfile:
    """Look up a profile by name; ``None`` means the baseline default."""
    if name is None:
        return PROFILES[DEFAULT_PROFILE_NAME]
    profile = PROFILES.get(name)
    if profile is None:
        raise ContractError(
            "unknown call profile %r; known profiles: %s"
            % (name, ", ".join(sorted(PROFILES)))
        )
    return profile


def expected_http_calls(profile: CallProfile) -> dict[str, int]:
    """The profile's call table, copied so a caller cannot mutate the table."""
    _checked(profile)
    return dict(profile.expected_http_calls)


def success_tail_label(profile: CallProfile, business: str) -> str:
    """The last phase a successful attempt must carry under this profile.

    An autocommit read is one SELECT with no COMMIT, so its last (and only)
    phase is the query itself; every other business still ends on COMMIT.
    """
    _checked(profile)
    if business not in BUSINESS_CLASSES:
        raise ContractError("unknown business %r" % business)
    if profile.read_mode == "autocommit" and business != "checkout":
        return "query"
    return "commit"


_validate_table()
