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
			name:      "ordinary function",
			statement: `CREATE FUNCTION public.answer() RETURNS integer LANGUAGE SQL AS $$ SELECT 42 $$`,
		},
		{
			name:      "session search path",
			statement: `SET search_path TO archive, public`,
		},
		{
			name:      "anonymous data migration",
			statement: `DO $$ BEGIN UPDATE public.users SET active = true; END $$`,
		},
		{
			name:      "copy into managed table",
			statement: `COPY public.users FROM STDIN`,
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
			name:      "database event",
			statement: "CREATE EVENT `ptah_dev`.`prune_users` ON SCHEDULE EVERY 1 DAY DO DELETE FROM users",
		},
		{
			name:      "insert with table modifier",
			statement: "INSERT INTO TABLE `ptah_dev`.`users` VALUES (1)",
		},
		{
			name:      "table named temp",
			statement: "CREATE TABLE `ptah_dev`.`temp` (id bigint)",
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
			wantErr:   `mysql migration replay rejects external file operation .*`,
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
			name:      "external table in current database",
			statement: "CREATE EXTERNAL TABLE [tenant].[events] ([id] bigint) WITH (LOCATION = '/events')",
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
