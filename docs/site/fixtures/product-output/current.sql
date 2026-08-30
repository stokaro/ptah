CREATE TABLE products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  price_cents INTEGER NOT NULL,
  legacy_code TEXT
);

CREATE TABLE legacy_inventory (
  id INTEGER PRIMARY KEY,
  product_id INTEGER NOT NULL,
  available INTEGER NOT NULL
);
