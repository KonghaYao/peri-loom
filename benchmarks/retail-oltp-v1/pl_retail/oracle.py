"""Independent oracle for PL-RETAIL-OLTP 1.0.0.

These values come from the contract, not from the generator.  The generator
asserts them before emitting a row and the verifier compares the loaded
database with them plus the confirmed ledger.
"""
from __future__ import annotations

SEED = 20261103

WAREHOUSES = 4
PRODUCTS = 10_000
STOCK_KEYS = WAREHOUSES * PRODUCTS
CUSTOMERS = 10_000
ORDERS = 100_000
LINES_PER_ORDER = 5
ORDER_LINES = ORDERS * LINES_PER_ORDER
PAYMENTS = ORDERS

STOCK_UNITS_PER_KEY = 1_000_000
CUSTOMER_INITIAL_BALANCE_CENTS = 1_000_000_000

PAYMENTS_TOTAL_CENTS = 47_551_602_679
SOLD_UNITS = 1_000_000
AVAILABLE_STOCK_UNITS = STOCK_KEYS * STOCK_UNITS_PER_KEY - SOLD_UNITS
CUSTOMER_BALANCE_TOTAL_CENTS = CUSTOMERS * CUSTOMER_INITIAL_BALANCE_CENTS - PAYMENTS_TOTAL_CENTS
INITIAL_TEXT_BYTES = 55_040_000

PRODUCT_DESCRIPTION_BYTES = 256
CUSTOMER_NOTE_BYTES = 128
ORDER_NOTE_BYTES = 128
ORDER_LINE_NOTE_BYTES = 64
PAYMENT_NOTE_BYTES = 64

PRICE_BASE_CENTS = 44_000

HOTSPOT_WAREHOUSE = 1
HOTSPOT_PRODUCTS = 100
HOTSPOT_CUSTOMERS = 100
HOTSPOT_CHECKOUT_SHARE = 0.8


def price_cents(product_id: int) -> int:
    if product_id < 1 or product_id > PRODUCTS:
        raise ValueError("product id out of range")
    return PRICE_BASE_CENTS + product_id


class OracleMismatch(RuntimeError):
    pass


def assert_initial_oracle() -> None:
    if AVAILABLE_STOCK_UNITS != 39_999_000_000:
        raise OracleMismatch("available stock total does not match the contract")
    if CUSTOMER_BALANCE_TOTAL_CENTS != 9_952_448_397_321:
        raise OracleMismatch("customer balance total does not match the contract")
    text = (
        PRODUCTS * PRODUCT_DESCRIPTION_BYTES
        + CUSTOMERS * CUSTOMER_NOTE_BYTES
        + ORDERS * ORDER_NOTE_BYTES
        + ORDER_LINES * ORDER_LINE_NOTE_BYTES
        + PAYMENTS * PAYMENT_NOTE_BYTES
    )
    if text != INITIAL_TEXT_BYTES:
        raise OracleMismatch("text byte total %d != %d" % (text, INITIAL_TEXT_BYTES))
    if ORDER_LINES != 500_000 or PAYMENTS != 100_000 or STOCK_KEYS != 40_000:
        raise OracleMismatch("row counts do not match the contract")
    if SOLD_UNITS % LINES_PER_ORDER != 0:
        raise OracleMismatch("sold unit total is not a multiple of five")
    if PAYMENTS_TOTAL_CENTS <= 0:
        raise OracleMismatch("payment total must be positive")
