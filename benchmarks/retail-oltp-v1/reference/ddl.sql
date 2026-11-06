-- PL-RETAIL-OLTP 1.0.0 fixed schema.
-- Money is integer cents.  Indexes are part of the contract: a run that adds
-- or removes one is INVALID, not faster.

CREATE TABLE warehouses (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  cash_cents INTEGER NOT NULL CHECK (cash_cents >= 0)
);

CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  price_cents INTEGER NOT NULL CHECK (price_cents > 0),
  description TEXT NOT NULL
);

CREATE TABLE stock (
  warehouse_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  initial_qty INTEGER NOT NULL,
  available_qty INTEGER NOT NULL,
  PRIMARY KEY (warehouse_id, product_id),
  CHECK (available_qty >= 0 AND available_qty <= initial_qty)
);

CREATE TABLE customers (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  initial_balance_cents INTEGER NOT NULL,
  balance_cents INTEGER NOT NULL,
  note TEXT NOT NULL
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  warehouse_id INTEGER NOT NULL,
  request_key TEXT NOT NULL UNIQUE,
  total_cents INTEGER NOT NULL,
  note TEXT NOT NULL
);

CREATE TABLE order_lines (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  unit_price_cents INTEGER NOT NULL,
  note TEXT NOT NULL
);

CREATE TABLE payments (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL UNIQUE,
  customer_id INTEGER NOT NULL,
  amount_cents INTEGER NOT NULL,
  note TEXT NOT NULL
);

CREATE INDEX orders_customer_id_id ON orders (customer_id, id);
CREATE INDEX order_lines_order_id ON order_lines (order_id);
CREATE INDEX stock_product_id ON stock (product_id);
CREATE INDEX payments_customer_id ON payments (customer_id);
