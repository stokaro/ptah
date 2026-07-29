package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/devclean"
)

func TestReplayGuardPostgresFamily_HappyPath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.Postgres,
		Schema:  "public",
	})
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "schema qualified table",
			statement: `CREATE TABLE archive.users (id bigint PRIMARY KEY)`,
		},
		{
			name:      "database extension",
			statement: `CREATE EXTENSION IF NOT EXISTS citext`,
		},
		{
			name:      "copy into managed table",
			statement: `COPY public.users FROM STDIN`,
		},
		{
			name:      "drop local foreign table",
			statement: `DROP FOREIGN TABLE app.remote_users`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestReplayGuardPostgresFamily_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.Postgres,
		Schema:  "public",
	})
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "role",
			statement: `CREATE ROLE app_user`,
			wantErr:   `postgres migration replay rejects CREATE ROLE .*`,
		},
		{
			name:      "alter system",
			statement: `ALTER SYSTEM SET work_mem = '64MB'`,
			wantErr:   `postgres migration replay rejects ALTER SYSTEM .*`,
		},
		{
			name:      "copy program",
			statement: `COPY public.users TO PROGRAM 'cat > /tmp/users'`,
			wantErr:   `postgres migration replay rejects external COPY .*`,
		},
		{
			name:      "copy file",
			statement: `COPY public.users TO '/tmp/users'`,
			wantErr:   `postgres migration replay rejects external COPY .*`,
		},
		{
			name:      "event trigger",
			statement: `CREATE EVENT TRIGGER ddl_watch ON ddl_command_end EXECUTE FUNCTION watch_ddl()`,
			wantErr:   `postgres migration replay rejects CREATE EVENT .*`,
		},
		{
			name:      "foreign data wrapper",
			statement: `CREATE FOREIGN DATA WRAPPER remote_db`,
			wantErr:   `postgres migration replay rejects CREATE FOREIGN .*`,
		},
		{
			name:      "temporary table",
			statement: `CREATE TEMP TABLE users (id bigint)`,
			wantErr:   `postgres migration replay rejects TEMP object .*`,
		},
		{
			name:      "session authorization",
			statement: `SET SESSION AUTHORIZATION admin`,
			wantErr:   `postgres migration replay rejects SET SESSION AUTHORIZATION .*`,
		},
		{
			name:      "function definition",
			statement: `CREATE FUNCTION public.answer() RETURNS integer LANGUAGE SQL AS $$ SELECT 42 $$`,
			wantErr:   `postgres migration replay rejects CREATE routine definition .*`,
		},
		{
			name:      "anonymous block",
			statement: `DO $$ BEGIN UPDATE public.users SET active = true; END $$`,
			wantErr:   `postgres migration replay rejects DO sublanguage .*`,
		},
		{
			name:      "procedure call",
			statement: `CALL public.refresh_materialized_data()`,
			wantErr:   `postgres migration replay rejects CALL sublanguage .*`,
		},
		{
			name:      "search path mutation",
			statement: `SET search_path TO pg_catalog`,
			wantErr:   `postgres migration replay rejects SET search_path .*`,
		},
		{
			name:      "foreign table",
			statement: `CREATE FOREIGN TABLE public.remote_users (id bigint) SERVER upstream`,
			wantErr:   `postgres migration replay rejects CREATE FOREIGN .*`,
		},
		{
			name:      "foreign schema import",
			statement: `IMPORT FOREIGN SCHEMA remote FROM SERVER upstream INTO public`,
			wantErr:   `postgres migration replay rejects IMPORT FOREIGN SCHEMA .*`,
		},
		{
			name:      "alter foreign table",
			statement: `ALTER FOREIGN TABLE app.remote_users ADD COLUMN label text`,
			wantErr:   `postgres migration replay rejects ALTER FOREIGN .*`,
		},
		{
			name:      "cte select into protected namespace",
			statement: `WITH source AS (SELECT 1 AS id) SELECT id INTO pg_catalog.ptah_leak FROM source`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second aggregate in protected namespace",
			statement: `DROP AGGREGATE app.keep(integer), pg_catalog.ptah_leak(integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second collation in protected namespace",
			statement: `DROP COLLATION app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second conversion in protected namespace",
			statement: `DROP CONVERSION app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second domain in protected namespace",
			statement: `DROP DOMAIN app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second foreign table in protected namespace",
			statement: `DROP FOREIGN TABLE app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second function in protected namespace",
			statement: `DROP FUNCTION app.keep(integer), pg_catalog.ptah_leak(integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second index in protected namespace",
			statement: `DROP INDEX app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second materialized view in protected namespace",
			statement: `DROP MATERIALIZED VIEW app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second operator in protected namespace",
			statement: `DROP OPERATOR app.=== (integer, integer), pg_catalog.=== (integer, integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second procedure in protected namespace",
			statement: `DROP PROCEDURE app.keep(integer), pg_catalog.ptah_leak(integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second routine in protected namespace",
			statement: `DROP ROUTINE app.keep(integer), pg_catalog.ptah_leak(integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second schema in protected namespace",
			statement: `DROP SCHEMA app, pg_catalog`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second sequence in protected namespace",
			statement: `DROP SEQUENCE app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second statistics in protected namespace",
			statement: `DROP STATISTICS app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second type in protected namespace",
			statement: `DROP TYPE app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second view in protected namespace",
			statement: `DROP VIEW app.keep, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReplayGuardMySQLFamily_HappyPath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.MySQL,
		Schema:  "ptah_dev",
	})
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "qualified table",
			statement: "CREATE TABLE `ptah_dev`.`users` (id bigint PRIMARY KEY)",
		},
		{
			name:      "qualified index target",
			statement: "CREATE UNIQUE INDEX `users_email` ON `ptah_dev`.`users` (`email`)",
		},
		{
			name:      "qualified update target",
			statement: "UPDATE `ptah_dev`.`users` SET `active` = 1",
		},
		{
			name:      "insert with table modifier",
			statement: "INSERT INTO TABLE `ptah_dev`.`users` VALUES (1)",
		},
		{
			name:      "table named temp",
			statement: "CREATE TABLE `ptah_dev`.`temp` (id bigint)",
		},
		{
			name:      "ordinary block comment",
			statement: "/* migration metadata */ CREATE TABLE comments (id bigint)",
		},
		{
			name:      "executable marker in string",
			statement: "INSERT INTO comments VALUES ('/*! CREATE USER escaped */')",
		},
		{
			name:      "definer view without stored body",
			statement: "CREATE DEFINER = CURRENT_USER VIEW active_users AS SELECT id FROM users WHERE active = 1",
		},
		{
			name:      "table as select with engine column",
			statement: "CREATE TABLE copied_users AS SELECT engine FROM users",
		},
		{
			name:      "local update with leading cte",
			statement: "WITH source AS (SELECT 1 AS id) UPDATE ptah_dev.users SET active = 1",
		},
		{
			name:      "local multi-table update",
			statement: "UPDATE ptah_dev.users AS users, ptah_dev.audit AS audit SET users.active = 1",
		},
		{
			name:      "local multi-table drop",
			statement: "DROP TABLE ptah_dev.users, ptah_dev.audit",
		},
		{
			name: "local multi-table rename",
			statement: "RENAME TABLE ptah_dev.users TO ptah_dev.users_old, " +
				"ptah_dev.audit TO ptah_dev.audit_old",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestReplayGuardMySQLFamily_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.MySQL,
		Schema:  "ptah_dev",
	})
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "switch database",
			statement: "USE production",
			wantErr:   `mysql migration replay rejects USE .*`,
		},
		{
			name:      "database",
			statement: "CREATE DATABASE production",
			wantErr:   `mysql migration replay rejects CREATE DATABASE .*`,
		},
		{
			name:      "temporary table",
			statement: "CREATE TEMPORARY TABLE users (id bigint)",
			wantErr:   `mysql migration replay rejects TEMP object .*`,
		},
		{
			name:      "cross database table",
			statement: "CREATE TABLE production.users (id bigint)",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "cross database index target",
			statement: "CREATE INDEX users_email ON production.users (email)",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "cross database update",
			statement: "UPDATE production.users SET active = 1",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "global set",
			statement: "SET GLOBAL sql_mode = 'STRICT_ALL_TABLES'",
			wantErr:   `mysql migration replay rejects global or persistent SET .*`,
		},
		{
			name:      "role grant",
			statement: "GRANT app_role TO app_user",
			wantErr:   `mysql migration replay rejects privilege or role mutation .*`,
		},
		{
			name:      "object grant",
			statement: "GRANT SELECT ON `ptah_dev`.`users` TO `reader`@`%`",
			wantErr:   `mysql migration replay rejects privilege or role mutation .*`,
		},
		{
			name:      "object revoke",
			statement: "REVOKE SELECT ON `ptah_dev`.`users` FROM `reader`@`%`",
			wantErr:   `mysql migration replay rejects privilege or role mutation .*`,
		},
		{
			name:      "prepared statement",
			statement: "PREPARE replay_stmt FROM 'DROP DATABASE production'",
			wantErr:   `mysql migration replay rejects prepared statement .*`,
		},
		{
			name:      "stored procedure call",
			statement: "CALL production.rotate_users()",
			wantErr:   `mysql migration replay rejects CALL sublanguage .*`,
		},
		{
			name:      "load data",
			statement: "LOAD DATA INFILE '/tmp/users.csv' INTO TABLE production.users",
			wantErr:   `mysql migration replay rejects LOAD external data operation .*`,
		},
		{
			name:      "load XML",
			statement: "LOAD XML INFILE '/tmp/users.xml' INTO TABLE production.users",
			wantErr:   `mysql migration replay rejects LOAD external data operation .*`,
		},
		{
			name:      "lock tables",
			statement: "LOCK TABLES users WRITE",
			wantErr:   `mysql migration replay rejects LOCK TABLES .*`,
		},
		{
			name:      "alter instance",
			statement: "ALTER INSTANCE ROTATE INNODB MASTER KEY",
			wantErr:   `mysql migration replay rejects ALTER INSTANCE .*`,
		},
		{
			name:      "flush",
			statement: "FLUSH PRIVILEGES",
			wantErr:   `mysql migration replay rejects FLUSH .*`,
		},
		{
			name:      "rename destination",
			statement: "RENAME TABLE ptah_dev.users TO production.users",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "alter table rename destination",
			statement: "ALTER TABLE ptah_dev.users RENAME TO production.users",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "insert table modifier into another database",
			statement: "INSERT INTO TABLE production.users VALUES (1)",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "outfile",
			statement: "SELECT * FROM users INTO OUTFILE '/tmp/users'",
			wantErr:   `mysql migration replay rejects external file operation .*`,
		},
		{
			name:      "native function",
			statement: "CREATE FUNCTION sys_exec RETURNS integer SONAME 'lib_mysqludf_sys.so'",
			wantErr:   `mysql migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "event body",
			statement: "CREATE EVENT `ptah_dev`.`prune_users` ON SCHEDULE EVERY 1 DAY DO DELETE FROM users",
			wantErr:   `mysql migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "trigger body",
			statement: "CREATE TRIGGER users_audit AFTER INSERT ON users FOR EACH ROW INSERT INTO audit VALUES (NEW.id)",
			wantErr:   `mysql migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "procedure body",
			statement: "CREATE PROCEDURE rebuild_users() BEGIN DELETE FROM users; END",
			wantErr:   `mysql migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "definer trigger body",
			statement: "CREATE DEFINER = 'migration'@'localhost' TRIGGER users_audit AFTER INSERT ON users FOR EACH ROW INSERT INTO audit VALUES (NEW.id)",
			wantErr:   `mysql migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "alter event body",
			statement: "ALTER EVENT prune_users DO DELETE FROM users",
			wantErr:   `mysql migration replay rejects ALTER executable stored body .*`,
		},
		{
			name:      "version executable comment",
			statement: "/*!50000 CREATE USER escaped */",
			wantErr:   `mysql migration replay rejects executable comment .*`,
		},
		{
			name:      "federated engine",
			statement: "CREATE TABLE remote_users (id bigint) ENGINE = FEDERATED CONNECTION = 'mysql://remote/users'",
			wantErr:   `mysql migration replay rejects FEDERATED storage engine .*`,
		},
		{
			name:      "cross database update with leading cte",
			statement: "WITH source AS (SELECT 1 AS id) UPDATE production.users SET active = 1",
			wantErr:   `mysql migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "second multi-table update target",
			statement: "UPDATE ptah_dev.users AS users, production.audit AS audit SET users.active = 1",
			wantErr:   `mysql migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name: "joined multi-table update target",
			statement: "UPDATE LOW_PRIORITY ptah_dev.users AS users " +
				"JOIN production.audit AS audit ON audit.id = users.id " +
				"SET users.active = 1",
			wantErr: `mysql migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name:      "second drop target",
			statement: "DROP TABLE ptah_dev.users, production.audit",
			wantErr:   `mysql migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name: "second rename source",
			statement: "RENAME TABLE ptah_dev.users TO ptah_dev.users_old, " +
				"production.audit TO ptah_dev.audit",
			wantErr: `mysql migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name: "second rename destination",
			statement: "RENAME TABLE ptah_dev.users TO ptah_dev.users_old, " +
				"ptah_dev.audit TO production.audit",
			wantErr: `mysql migration replay rejects cross-database target "production.audit" .*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReplayGuardMariaDB_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.MariaDB,
		Schema:  "ptah_dev",
	})
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "MariaDB executable comment",
			statement: "/*M!100100 CREATE USER escaped */",
			wantErr:   `mariadb migration replay rejects executable comment .*`,
		},
		{
			name:      "connect engine",
			statement: "CREATE TABLE remote_users (id bigint) ENGINE CONNECT TABLE_TYPE MYSQL",
			wantErr:   `mariadb migration replay rejects CONNECT storage engine .*`,
		},
		{
			name:      "cross database update with leading cte",
			statement: "WITH source AS (SELECT 1 AS id) UPDATE production.users SET active = 1",
			wantErr:   `mariadb migration replay rejects cross-database target "production.users" .*`,
		},
		{
			name:      "second drop target",
			statement: "DROP TABLE ptah_dev.users, production.audit",
			wantErr:   `mariadb migration replay rejects cross-database target "production.audit" .*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReplayGuardSQLServer_HappyPath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.SQLServer,
		Schema:  "dbo",
	})
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "schema qualified table",
			statement: "CREATE TABLE [tenant].[users] ([id] bigint PRIMARY KEY)",
		},
		{
			name:      "schema qualified index target",
			statement: "CREATE INDEX [users_email] ON [tenant].[users] ([email])",
		},
		{
			name:      "drop local function",
			statement: "DROP FUNCTION IF EXISTS [tenant].[answer]",
		},
		{
			name:      "drop local synonym",
			statement: "DROP SYNONYM IF EXISTS [tenant].[users]",
		},
		{
			name:      "drop local external table",
			statement: "DROP EXTERNAL TABLE IF EXISTS [tenant].[events]",
		},
		{
			name:      "check identity",
			statement: "DBCC CHECKIDENT ('tenant.users', RESEED, 0)",
		},
		{
			name:      "check table",
			statement: "DBCC CHECKTABLE ('tenant.users')",
		},
		{
			name:      "check constraints",
			statement: "DBCC CHECKCONSTRAINTS ('tenant.users')",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestReplayGuardSQLServer_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.SQLServer,
		Schema:  "dbo",
	})
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "switch database",
			statement: "USE production",
			wantErr:   `sqlserver migration replay rejects USE .*`,
		},
		{
			name:      "login",
			statement: "CREATE LOGIN app_user WITH PASSWORD = 'secret'",
			wantErr:   `sqlserver migration replay rejects CREATE LOGIN .*`,
		},
		{
			name:      "server role",
			statement: "CREATE SERVER ROLE operators",
			wantErr:   `sqlserver migration replay rejects CREATE SERVER .*`,
		},
		{
			name:      "temporary table",
			statement: "CREATE TABLE #users (id bigint)",
			wantErr:   `sqlserver migration replay rejects temporary object .*`,
		},
		{
			name:      "cross database table",
			statement: "CREATE TABLE [production].[dbo].[users] ([id] bigint)",
			wantErr:   `sqlserver migration replay rejects cross-database target "production.dbo.users" .*`,
		},
		{
			name:      "remote execute",
			statement: "EXECUTE ('DROP TABLE users') AT production_server",
			wantErr:   `sqlserver migration replay rejects remote EXECUTE .*`,
		},
		{
			name:      "external table",
			statement: "CREATE EXTERNAL TABLE [tenant].[events] ([id] bigint) WITH (LOCATION = '/events')",
			wantErr:   `sqlserver migration replay rejects CREATE EXTERNAL TABLE .*`,
		},
		{
			name:      "synonym",
			statement: "CREATE SYNONYM [tenant].[users] FOR [production].[dbo].[users]",
			wantErr:   `sqlserver migration replay rejects CREATE SYNONYM .*`,
		},
		{
			name:      "trigger body",
			statement: "CREATE TRIGGER [tenant].[audit] ON [tenant].[users] AFTER INSERT AS SELECT 1",
			wantErr:   `sqlserver migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "procedure body",
			statement: "CREATE PROCEDURE [tenant].[rebuild] AS DELETE FROM [tenant].[users]",
			wantErr:   `sqlserver migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "function body",
			statement: "CREATE FUNCTION [tenant].[answer]() RETURNS int AS BEGIN RETURN 42 END",
			wantErr:   `sqlserver migration replay rejects CREATE executable stored body .*`,
		},
		{
			name:      "second multi-object drop target",
			statement: "DROP TABLE dbo.local_copy, otherdb.dbo.remote_copy",
			wantErr:   `sqlserver migration replay rejects cross-database target "otherdb.dbo.remote_copy" .*`,
		},
		{
			name: "second drop index target",
			statement: "DROP INDEX ix_local ON dbo.local_copy, " +
				"ix_remote ON otherdb.dbo.remote_copy",
			wantErr: `sqlserver migration replay rejects cross-database target "otherdb.dbo.remote_copy" .*`,
		},
		{
			name:      "other DBCC command",
			statement: "DBCC CHECKDB",
			wantErr:   `sqlserver migration replay rejects DBCC server operation .*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReplayGuardClickHouse_HappyPath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.ClickHouse,
		Schema:  "ptah_dev",
	})
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "qualified table",
			statement: "CREATE TABLE `ptah_dev`.`events` (id UInt64) ENGINE = MergeTree ORDER BY id",
		},
		{
			name:      "qualified dictionary",
			statement: "CREATE DICTIONARY `ptah_dev`.`countries` (id UInt64, name String) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT())",
		},
		{
			name:      "qualified materialized view",
			statement: "CREATE MATERIALIZED VIEW `ptah_dev`.`events_daily` ENGINE = SummingMergeTree ORDER BY day AS SELECT toDate(ts) AS day FROM events",
		},
		{
			name:      "table named temp",
			statement: "CREATE TABLE `ptah_dev`.`temp` (id UInt64) ENGINE = Memory",
		},
		{
			name:      "embedded local engine",
			statement: "CREATE TABLE `ptah_dev`.`kv` (id UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY id",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestReplayGuardClickHouse_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{
		Dialect: platform.ClickHouse,
		Schema:  "ptah_dev",
	})
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "switch database",
			statement: "USE production",
			wantErr:   `clickhouse migration replay rejects USE .*`,
		},
		{
			name:      "database",
			statement: "CREATE DATABASE production",
			wantErr:   `clickhouse migration replay rejects CREATE DATABASE .*`,
		},
		{
			name:      "function",
			statement: "CREATE FUNCTION normalize AS x -> lower(x)",
			wantErr:   `clickhouse migration replay rejects CREATE FUNCTION .*`,
		},
		{
			name:      "named collection",
			statement: "CREATE NAMED COLLECTION remote AS url = 'https://example.test'",
			wantErr:   `clickhouse migration replay rejects CREATE NAMED .*`,
		},
		{
			name:      "system command",
			statement: "SYSTEM DROP DNS CACHE",
			wantErr:   `clickhouse migration replay rejects SYSTEM .*`,
		},
		{
			name:      "cluster ddl",
			statement: "CREATE TABLE ptah_dev.events ON CLUSTER production (id UInt64) ENGINE = Memory",
			wantErr:   `clickhouse migration replay rejects ON CLUSTER .*`,
		},
		{
			name:      "workload",
			statement: "CREATE WORKLOAD analytics",
			wantErr:   `clickhouse migration replay rejects CREATE WORKLOAD .*`,
		},
		{
			name:      "resource",
			statement: "CREATE RESOURCE io_read (READ DISK disk1 FOR INTERVAL 1 SECOND MAX 10M)",
			wantErr:   `clickhouse migration replay rejects CREATE RESOURCE .*`,
		},
		{
			name:      "temporary table",
			statement: "CREATE TEMPORARY TABLE events (id UInt64)",
			wantErr:   `clickhouse migration replay rejects TEMP object .*`,
		},
		{
			name:      "cross database table",
			statement: "CREATE TABLE production.events (id UInt64) ENGINE = Memory",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
		{
			name:      "implicit default table engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64)",
			wantErr:   `clickhouse migration replay rejects implicit/default table engine .*`,
		},
		{
			name:      "table as select with implicit default engine",
			statement: "CREATE TABLE ptah_dev.copied AS SELECT engine FROM ptah_dev.source",
			wantErr:   `clickhouse migration replay rejects implicit/default table engine .*`,
		},
		{
			name:      "external dictionary source",
			statement: "CREATE DICTIONARY ptah_dev.remote (id UInt64) PRIMARY KEY id SOURCE(HTTP(URL 'https://example.test/data')) LAYOUT(FLAT())",
			wantErr:   `clickhouse migration replay rejects HTTP dictionary source .*`,
		},
		{
			name:      "executable dictionary source",
			statement: "CREATE DICTIONARY ptah_dev.remote (id UInt64) PRIMARY KEY id SOURCE(EXECUTABLE(COMMAND 'touch /tmp/escaped')) LAYOUT(FLAT())",
			wantErr:   `clickhouse migration replay rejects EXECUTABLE dictionary source .*`,
		},
		{
			name:      "persistent table snapshot",
			statement: "ALTER TABLE ptah_dev.events FREEZE",
			wantErr:   `clickhouse migration replay rejects persistent table snapshot .*`,
		},
		{
			name:      "distributed engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = Distributed(cluster, production, events)",
			wantErr:   `clickhouse migration replay rejects DISTRIBUTED table engine .*`,
		},
		{
			name:      "remote database engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = PostgreSQL('host:5432', 'db', 'events', 'user', 'pass')",
			wantErr:   `clickhouse migration replay rejects POSTGRESQL table engine .*`,
		},
		{
			name:      "replicated engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = ReplicatedMergeTree('/tables/events', '{replica}') ORDER BY id",
			wantErr:   `clickhouse migration replay rejects REPLICATEDMERGETREE table engine .*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := guard.ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReplayGuardUnknownDialect_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(types.DBInfo{Dialect: "oracle"})

	err := guard.ValidateStatement("CREATE TABLE users (id integer)")

	c.Assert(err, qt.ErrorMatches, `oracle migration replay rejects unsupported dialect .*`)
}
