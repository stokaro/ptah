-- render: the desired schema
CREATE TABLE orders (
  id Int64,
  user_id Int64,
  total Nullable(Decimal(10,2))
) ENGINE = MergeTree ORDER BY (id);


CREATE TABLE users (
  id Int64,
  email String,
  name Nullable(String),
  created_at Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (id);


-- CLICKHOUSE: UNIQUE index "users_email_uq" downgraded to a minmax skipping index; uniqueness is not enforced by ClickHouse
ALTER TABLE `users` ADD INDEX `users_email_uq` email TYPE minmax GRANULARITY 8192;

ALTER TABLE `users` ADD INDEX `users_created_ix` created_at TYPE minmax GRANULARITY 8192;

ALTER TABLE `orders` ADD INDEX `orders_user_ix` user_id TYPE minmax GRANULARITY 8192;
-- plan: the current schema to the desired one
CREATE TABLE orders (
  id Int64,
  user_id Int64,
  total Nullable(Decimal(10,2))
) ENGINE = MergeTree ORDER BY (id);

ALTER TABLE users ADD COLUMN created_at Nullable(DateTime64(3));
ALTER TABLE `orders` ADD INDEX `orders_user_ix` user_id TYPE minmax GRANULARITY 8192;
ALTER TABLE `users` ADD INDEX `users_created_ix` created_at TYPE minmax GRANULARITY 8192;
-- CLICKHOUSE: UNIQUE index "users_email_uq" downgraded to a minmax skipping index; uniqueness is not enforced by ClickHouse
ALTER TABLE `users` ADD INDEX `users_email_uq` email TYPE minmax GRANULARITY 8192;
ALTER TABLE `users` DROP INDEX `users_name_ix`;
-- render: a covering index
-- refused: clickhouse does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
-- plan: the current schema to a covering index
-- refused: clickhouse does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
