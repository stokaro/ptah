-- render: the desired schema
CREATE TABLE [orders] (
  [id] BIGINT PRIMARY KEY,
  [user_id] BIGINT NOT NULL,
  [total] DECIMAL(10,2)
);

CREATE TABLE [users] (
  [id] BIGINT PRIMARY KEY,
  [email] NVARCHAR(255) NOT NULL,
  [name] NVARCHAR(100),
  [created_at] DATETIME2
);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'users_email_uq' AND object_id = OBJECT_ID('users'))
CREATE UNIQUE INDEX [users_email_uq] ON [users] ([email]);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'users_created_ix' AND object_id = OBJECT_ID('users'))
CREATE INDEX [users_created_ix] ON [users] ([created_at]);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'orders_user_ix' AND object_id = OBJECT_ID('orders'))
CREATE INDEX [orders_user_ix] ON [orders] ([user_id]);
-- plan: the current schema to the desired one
CREATE TABLE [orders] (
  [id] BIGINT PRIMARY KEY,
  [user_id] BIGINT NOT NULL,
  [total] DECIMAL(10,2)
);
-- Modify table: users
ALTER TABLE [users] ADD [created_at] DATETIME2;
CREATE INDEX [orders_user_ix] ON [orders] ([user_id]);
CREATE INDEX [users_created_ix] ON [users] ([created_at]);
CREATE UNIQUE INDEX [users_email_uq] ON [users] ([email]);
DROP INDEX IF EXISTS [users_name_ix] ON [users];
-- render: a covering index
-- refused: sqlserver does not support INCLUDE columns on index "users_name_ix"; target cockroachdb, postgres, spanner, ydb, or yugabytedb
-- plan: the current schema to a covering index

