-- render: the desired schema
CREATE TABLE orders (
  id NUMBER(19) NOT NULL PRIMARY KEY,
  user_id NUMBER(19) NOT NULL,
  total NUMBER(10,2)
);

CREATE TABLE users (
  id NUMBER(19) NOT NULL PRIMARY KEY,
  email VARCHAR2(255) NOT NULL,
  name VARCHAR2(100),
  created_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS users_email_uq ON users (email);

CREATE INDEX IF NOT EXISTS users_created_ix ON users (created_at);

CREATE INDEX IF NOT EXISTS orders_user_ix ON orders (user_id);
-- plan: the current schema to the desired one
CREATE TABLE orders (
  id NUMBER(19) NOT NULL PRIMARY KEY,
  user_id NUMBER(19) NOT NULL,
  total NUMBER(10,2)
);
-- Modify table: users
ALTER TABLE users ADD (created_at TIMESTAMP);
-- Modify column users.id: type: integer -> number(19)
ALTER TABLE users MODIFY (id NUMBER(19));
CREATE INDEX orders_user_ix ON orders (user_id);
CREATE INDEX users_created_ix ON users (created_at);
CREATE UNIQUE INDEX users_email_uq ON users (email);
DROP INDEX IF EXISTS users_name_ix;
-- render: a covering index
-- refused: oracle does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
-- plan: the current schema to a covering index
-- refused: oracle does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
