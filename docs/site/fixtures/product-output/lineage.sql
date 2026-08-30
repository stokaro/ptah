CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  email TEXT NOT NULL
);

CREATE TABLE orders (
  id INTEGER NOT NULL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers (id),
  total_cents INTEGER NOT NULL
);

CREATE VIEW order_totals AS
SELECT customer_id AS buyer, total_cents AS cents FROM orders;

CREATE VIEW customer_orders AS
SELECT c.email, o.total_cents
FROM customers c JOIN orders o ON o.customer_id = c.id;
