-- render: the desired schema
CREATE TABLE "orders" (
  "id" BIGINT PRIMARY KEY,
  "user_id" BIGINT NOT NULL,
  "total" DECIMAL(10,2)
);

CREATE TABLE "users" (
  "id" BIGINT PRIMARY KEY,
  "email" TEXT NOT NULL,
  "name" TEXT,
  "created_at" TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS "users_email_uq" ON "users" ("email");

CREATE INDEX IF NOT EXISTS "users_created_ix" ON "users" ("created_at");

CREATE INDEX IF NOT EXISTS "orders_user_ix" ON "orders" ("user_id");
-- plan: the current schema to the desired one
CREATE TABLE "orders" (
  "id" BIGINT PRIMARY KEY,
  "user_id" BIGINT NOT NULL,
  "total" DECIMAL(10,2)
);
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
CREATE INDEX IF NOT EXISTS "orders_user_ix" ON "orders" ("user_id");
CREATE INDEX IF NOT EXISTS "users_created_ix" ON "users" ("created_at");
CREATE UNIQUE INDEX IF NOT EXISTS "users_email_uq" ON "users" ("email");
DROP INDEX IF EXISTS "users_name_ix";
-- render: a covering index
-- refused: sqlite does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
-- plan: the current schema to a covering index
-- refused: sqlite does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
