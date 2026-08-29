CREATE EXTENSION vector;

CREATE TABLE docs (
  id         BIGINT PRIMARY KEY,
  title      TEXT NOT NULL,
  body       TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO docs (id, title, body) VALUES
  (1, 'Pricing', 'We bill monthly.'),
  (2, 'Support', 'Email support@example.com.'),
  (3, 'Billing', 'Invoices are issued on the first.');
