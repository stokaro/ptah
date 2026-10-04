-- render: the desired schema
-- COCKROACHDB TABLE: orders --
CREATE TABLE "orders" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "user_id" BIGINT NOT NULL,
  "total" DECIMAL(10,2)
);


-- COCKROACHDB TABLE: users --
CREATE TABLE "users" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "email" VARCHAR(255) NOT NULL,
  "name" VARCHAR(100),
  "created_at" TIMESTAMP
);


CREATE UNIQUE INDEX IF NOT EXISTS "users_email_uq" ON "users" ("email");

CREATE INDEX IF NOT EXISTS "users_created_ix" ON "users" ("created_at");

CREATE INDEX IF NOT EXISTS "orders_user_ix" ON "orders" ("user_id");
-- plan: the current schema to the desired one
-- COCKROACHDB TABLE: orders --
CREATE TABLE "orders" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "user_id" BIGINT NOT NULL,
  "total" DECIMAL(10,2)
);

-- Add/modify columns for table: users
-- ALTER statements: --
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;

CREATE INDEX IF NOT EXISTS "orders_user_ix" ON "orders" ("user_id");
CREATE INDEX IF NOT EXISTS "users_created_ix" ON "users" ("created_at");
CREATE UNIQUE INDEX IF NOT EXISTS "users_email_uq" ON "users" ("email");
DROP INDEX IF EXISTS "users"@"users_name_ix";
-- render: a covering index
-- COCKROACHDB TABLE: users --
CREATE TABLE "users" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "email" VARCHAR(255) NOT NULL,
  "name" VARCHAR(100)
);


CREATE INDEX IF NOT EXISTS "users_name_ix" ON "users" ("name") INCLUDE ("email");
-- plan: the current schema to a covering index
DROP INDEX IF EXISTS "users"@"users_name_ix";
CREATE INDEX IF NOT EXISTS "users_name_ix" ON "users" ("name") INCLUDE ("email");
