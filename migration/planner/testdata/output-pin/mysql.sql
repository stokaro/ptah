-- render: the desired schema
-- MYSQL TABLE: orders --
CREATE TABLE `orders` (
  `id` BIGINT PRIMARY KEY,
  `user_id` BIGINT NOT NULL,
  `total` DECIMAL(10,2)
);


-- MYSQL TABLE: users --
CREATE TABLE `users` (
  `id` BIGINT PRIMARY KEY,
  `email` VARCHAR(255) NOT NULL,
  `name` VARCHAR(100),
  `created_at` TIMESTAMP
);


CREATE UNIQUE INDEX `users_email_uq` ON `users` (`email`);

CREATE INDEX `users_created_ix` ON `users` (`created_at`);

CREATE INDEX `orders_user_ix` ON `orders` (`user_id`);
-- plan: the current schema to the desired one
-- MYSQL TABLE: orders --
CREATE TABLE `orders` (
  `id` BIGINT PRIMARY KEY,
  `user_id` BIGINT NOT NULL,
  `total` DECIMAL(10,2)
);

-- Modify table: users
-- ALTER statements: --
ALTER TABLE `users` ADD COLUMN `created_at` TIMESTAMP;

CREATE INDEX `orders_user_ix` ON `orders` (`user_id`);
CREATE INDEX `users_created_ix` ON `users` (`created_at`);
CREATE UNIQUE INDEX `users_email_uq` ON `users` (`email`);
DROP INDEX `users_name_ix` ON `users`;
-- render: a covering index
-- refused: mysql does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
-- plan: the current schema to a covering index
-- refused: mysql does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
