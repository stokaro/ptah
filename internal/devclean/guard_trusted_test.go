package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// trustReplaySQL resolves rawURL as a command does when the operator trusts
// the SQL it replays, and keeps the answer until the test ends.
func trustReplaySQL(c *qt.C, rawURL string) {
	c.Helper()
	_, release, err := devdocker.Resolve(replaytrust.With(c.Context(), true), rawURL, devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
}

// Named dev servers, one per engine guard, each reached by a URL of its own.
// None is declared disposable, so each realm is the dev database.
var (
	trustedPostgresDev   = catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://dev@trusted-pg:5432/ptah_dev"}
	trustedCockroachDev  = catalog.ServerInfo{Dialect: platform.CockroachDB, Schema: "public", URL: "postgres://dev@trusted-crdb:26257/ptah_dev"}
	trustedMySQLDev      = catalog.ServerInfo{Dialect: platform.MySQL, Schema: "ptah_dev", URL: "mysql://dev@trusted-mysql:3306/ptah_dev"}
	trustedSQLServerDev  = catalog.ServerInfo{Dialect: platform.SQLServer, Schema: "dbo", URL: "sqlserver://dev@trusted-mssql:1433?database=ptah_dev"}
	trustedClickHouseDev = catalog.ServerInfo{Dialect: platform.ClickHouse, Schema: "ptah_dev", URL: "clickhouse://dev@trusted-ch:9000/ptah_dev"}
	trustedSQLiteDev     = catalog.ServerInfo{Dialect: platform.SQLite, URL: "sqlite:///trusted/dev.db"}
)

// trustLifted is, per engine guard, the statements a named dev server refuses
// only because their effects cannot be confined to the dev database: an opaque
// body, a role, a database or a server-level privilege, a write to another
// database, and data sent across the server's edge. The first PostgreSQL rows
// are the shape of the Rebase history that asked for trust: its routines, the
// DO block that creates the role its policies name, and the bare CREATE ROLE
// its generated desired file carries.
var trustLifted = []struct {
	name      string
	info      catalog.ServerInfo
	statement string
}{
	{"PostgreSQL SQL routine", trustedPostgresDev,
		"CREATE OR REPLACE FUNCTION rebase.uid() RETURNS text AS $$ SELECT current_setting('app.uid', true) $$ LANGUAGE sql STABLE PARALLEL SAFE"},
	{"PostgreSQL DO block creating a role", trustedPostgresDev,
		"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rebase_admin') THEN " +
			"CREATE ROLE rebase_admin NOLOGIN NOINHERIT; END IF; END $$"},
	{"PostgreSQL bare role", trustedPostgresDev, "CREATE ROLE rebase_admin NOLOGIN NOINHERIT"},
	{"PostgreSQL CALL", trustedPostgresDev, "CALL refresh_items()"},
	{"PostgreSQL routine change", trustedPostgresDev, "ALTER FUNCTION item_count() SECURITY DEFINER"},
	{"PostgreSQL role change without SET", trustedPostgresDev, "ALTER ROLE app_rw NOLOGIN"},
	{"PostgreSQL dropped role", trustedPostgresDev, "DROP ROLE rebase_admin"},
	{"PostgreSQL database", trustedPostgresDev, "CREATE DATABASE other"},
	{"PostgreSQL role membership", trustedPostgresDev, `GRANT "admin" TO "app_rw"`},
	{"PostgreSQL privilege on the database", trustedPostgresDev, "GRANT CONNECT ON DATABASE ptah_dev TO app_rw"},
	{"PostgreSQL privilege on a schema", trustedPostgresDev, `GRANT USAGE ON SCHEMA "public" TO PUBLIC`},
	{"PostgreSQL comment on a role", trustedPostgresDev, "COMMENT ON ROLE app_rw IS 'reader'"},
	{"PostgreSQL COPY to a program", trustedPostgresDev, "COPY items TO PROGRAM 'cat > /tmp/items'"},
	{"PostgreSQL dblink", trustedPostgresDev, "SELECT dblink_exec('host=elsewhere', 'DROP TABLE t')"},
	{"PostgreSQL foreign schema import", trustedPostgresDev, "IMPORT FOREIGN SCHEMA public FROM SERVER s INTO app"},
	{"CockroachDB role", trustedCockroachDev, "CREATE ROLE r"},
	{"MySQL procedure", trustedMySQLDev, "CREATE PROCEDURE refresh() BEGIN SELECT 1; END"},
	{"MySQL trigger", trustedMySQLDev, "CREATE TRIGGER items_audit AFTER INSERT ON items FOR EACH ROW INSERT INTO audit VALUES (NEW.id)"},
	{"MySQL loadable function", trustedMySQLDev, "CREATE FUNCTION sys_exec RETURNS integer SONAME 'lib_mysqludf_sys.so'"},
	{"MySQL user", trustedMySQLDev, "CREATE USER app@'%'"},
	{"MySQL privilege", trustedMySQLDev, "GRANT SELECT ON ptah_dev.* TO app@'%'"},
	{"MySQL database", trustedMySQLDev, "CREATE DATABASE other"},
	{"MySQL write to another database", trustedMySQLDev, "INSERT INTO other.t VALUES (1)"},
	{"MySQL prepared statement", trustedMySQLDev, "PREPARE s FROM 'SELECT 1'"},
	{"MySQL LOAD DATA", trustedMySQLDev, "LOAD DATA INFILE '/tmp/items' INTO TABLE items"},
	{"MySQL OUTFILE", trustedMySQLDev, "SELECT * FROM items INTO OUTFILE '/tmp/items'"},
	{"MySQL FEDERATED table", trustedMySQLDev, "CREATE TABLE t (id int) ENGINE=FEDERATED CONNECTION='mysql://u@elsewhere/db/t'"},
	{"SQL Server procedure", trustedSQLServerDev, "CREATE PROCEDURE dbo.refresh AS SELECT 1"},
	{"SQL Server EXEC", trustedSQLServerDev, "EXEC dbo.refresh"},
	{"SQL Server remote EXECUTE", trustedSQLServerDev, "EXEC ('SELECT 1') AT remote_server"},
	{"SQL Server login", trustedSQLServerDev, "CREATE LOGIN app WITH PASSWORD = 'secret'"},
	{"SQL Server database", trustedSQLServerDev, "CREATE DATABASE other"},
	{"SQL Server synonym", trustedSQLServerDev, "CREATE SYNONYM dbo.s FOR other.dbo.t"},
	{"SQL Server credential", trustedSQLServerDev, "CREATE CREDENTIAL c WITH IDENTITY = 'x'"},
	{"SQL Server backup", trustedSQLServerDev, "BACKUP DATABASE ptah_dev TO DISK = 'dev.bak'"},
	{"SQL Server BULK INSERT", trustedSQLServerDev, "BULK INSERT dbo.t FROM 'items.csv'"},
	{"SQL Server write to another database", trustedSQLServerDev, "INSERT INTO other.dbo.t VALUES (1)"},
	{"SQL Server SELECT INTO another database", trustedSQLServerDev, "SELECT * INTO other.dbo.t FROM dbo.items"},
	{"SQL Server OPENQUERY destination", trustedSQLServerDev,
		"INSERT OPENQUERY(remote_link, 'SELECT id FROM remote_table') VALUES (1)"},
	{"ClickHouse function", trustedClickHouseDev, "CREATE FUNCTION plus_one AS (x) -> x + 1"},
	{"ClickHouse user", trustedClickHouseDev, "CREATE USER app"},
	{"ClickHouse role", trustedClickHouseDev, "CREATE ROLE r"},
	{"ClickHouse named collection", trustedClickHouseDev, "CREATE NAMED COLLECTION nc AS host = 'elsewhere'"},
	{"ClickHouse grant", trustedClickHouseDev, "GRANT SELECT ON ptah_dev.* TO app"},
	{"ClickHouse row policy", trustedClickHouseDev, "CREATE ROW POLICY p ON ptah_dev.t USING 1 TO app"},
	{"ClickHouse database", trustedClickHouseDev, "CREATE DATABASE other"},
	{"ClickHouse ON CLUSTER", trustedClickHouseDev, "CREATE TABLE ptah_dev.t ON CLUSTER c (id UInt8) ENGINE = Memory"},
	{"ClickHouse write to another database", trustedClickHouseDev, "INSERT INTO other.t VALUES (1)"},
	{"ClickHouse OUTFILE", trustedClickHouseDev, "SELECT 1 INTO OUTFILE 'out.tsv'"},
	{"ClickHouse external dictionary", trustedClickHouseDev,
		"CREATE DICTIONARY ptah_dev.d (id UInt64) PRIMARY KEY id SOURCE(MYSQL(host 'elsewhere')) LAYOUT(FLAT()) LIFETIME(0)"},
	{"ClickHouse backup", trustedClickHouseDev, "BACKUP DATABASE ptah_dev TO Disk('backups', 'dev')"},
	{"ClickHouse table snapshot", trustedClickHouseDev, "ALTER TABLE ptah_dev.events FREEZE"},
	{"ClickHouse database-wide TRUNCATE", trustedClickHouseDev, "TRUNCATE DATABASE ptah_dev"},
	{"ClickHouse external table engine", trustedClickHouseDev,
		"CREATE TABLE ptah_dev.feed (id UInt8) ENGINE = URL('http://elsewhere/feed.csv', CSV)"},
	{"SQLite VACUUM INTO", trustedSQLiteDev, "VACUUM INTO '/tmp/copy.db'"},
}

// TestDevReplayGuard_TrustedSQL_LiftsConfinementRefusals pins what trust
// lifts: every statement above runs on a named dev server whose run trusts the
// SQL. The realm stays the dev database, because trust does not make the
// server the run's own.
func TestDevReplayGuard_TrustedSQL_LiftsConfinementRefusals(t *testing.T) {
	for _, test := range trustLifted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			trustReplaySQL(c, test.info.URL)
			c.Assert(devclean.DevReplayRealm(test.info), qt.Equals, devclean.ReplayRealmDatabase)
			c.Assert(devclean.NewDevReplayGuard(test.info).ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// TestDevReplayGuard_WithoutTrust_RefusesAndNamesTheTrustRemedy is the control
// for the test above: without trust each statement it accepts is refused, and
// the refusal ends with the variable that would lift it.
func TestDevReplayGuard_WithoutTrust_RefusesAndNamesTheTrustRemedy(t *testing.T) {
	for _, test := range trustLifted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.NewDevReplayGuard(test.info).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, `.* migration replay rejects .*`+trustRemedy)
		})
	}
}

// TestDevReplayGuard_WithoutTrust_RefusalWording pins whole refusals, one per
// remedy shape: a PostgreSQL routine, which owning the server lifts too, so
// both remedies follow in that order; an external COPY, which only trust
// lifts; and a SQL Server procedure, an engine no disposable declaration
// serves.
func TestDevReplayGuard_WithoutTrust_RefusalWording(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
		wantErr   string
	}{
		{"owned or trusted", trustedPostgresDev,
			"CREATE FUNCTION rebase.uid() RETURNS text AS $$ SELECT 1 $$ LANGUAGE sql",
			`postgres migration replay rejects CREATE routine definition because its effects cannot be confined ` +
				`to the disposable database realm` + ownedServerRemedy + trustRemedy},
		{"trusted only", trustedPostgresDev, "COPY items TO PROGRAM 'cat'",
			`postgres migration replay rejects external COPY because its effects cannot be confined ` +
				`to the disposable database realm` + trustRemedy},
		{"an engine with no owned realm", trustedSQLServerDev, "CREATE PROCEDURE dbo.refresh AS SELECT 1",
			`sqlserver migration replay rejects CREATE executable stored body because its effects cannot be confined ` +
				`to the disposable database realm` + trustRemedy},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.NewDevReplayGuard(test.info).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestDevReplayGuard_TrustedSQL_KeepsRunProtections pins what trust keeps:
// each statement would mislead the rest of the run or its cleanup, so it is
// refused under trust in the words it is refused without, and its refusal
// names no trust remedy. ErrorMatches anchors the whole message, so a remedy
// appended to any of them fails the row.
func TestDevReplayGuard_TrustedSQL_KeepsRunProtections(t *testing.T) {
	const refused = ` because its effects cannot be confined to the disposable database realm`
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
		wantErr   string
	}{
		{"PostgreSQL search_path", trustedPostgresDev, "SET search_path TO app",
			`postgres migration replay rejects SET search_path` + refused},
		{"PostgreSQL SET ROLE", trustedPostgresDev, "SET ROLE rebase_admin",
			`postgres migration replay rejects SET ROLE` + refused},
		{"PostgreSQL session authorization", trustedPostgresDev, "SET SESSION AUTHORIZATION rebase_admin",
			`postgres migration replay rejects SET SESSION AUTHORIZATION` + refused},
		{"PostgreSQL transaction control", trustedPostgresDev, "BEGIN",
			`postgres migration replay rejects BEGIN session or transaction state` + refused},
		{"PostgreSQL advisory lock", trustedPostgresDev, "SELECT pg_advisory_lock(1)",
			`postgres migration replay rejects session advisory lock` + refused},
		{"PostgreSQL ALTER SYSTEM", trustedPostgresDev, "ALTER SYSTEM SET work_mem = '1GB'",
			`postgres migration replay rejects ALTER SYSTEM` + refused},
		{"PostgreSQL ALTER DATABASE", trustedPostgresDev, "ALTER DATABASE ptah_dev SET work_mem = '1GB'",
			`postgres migration replay rejects ALTER DATABASE` + refused},
		{"PostgreSQL role setting", trustedPostgresDev, "ALTER ROLE app_rw SET search_path = app",
			`postgres migration replay rejects ALTER ROLE` + refused},
		{"PostgreSQL LOAD", trustedPostgresDev, "LOAD 'auto_explain'",
			`postgres migration replay rejects LOAD session or transaction state` + refused},
		{"PostgreSQL control function", trustedPostgresDev, "SELECT pg_terminate_backend(1)",
			`postgres migration replay rejects cluster control function` + refused},
		{"PostgreSQL protected namespace", trustedPostgresDev, "CREATE TABLE pg_catalog.x (id int)",
			`postgres migration replay rejects protected namespace "pg_catalog" mutation` + refused},
		{"PostgreSQL TEMP object", trustedPostgresDev, "CREATE TEMP TABLE x (id int)",
			`postgres migration replay rejects TEMP object` + refused},
		{"PostgreSQL tablespace", trustedPostgresDev, "CREATE TABLESPACE ts LOCATION '/tmp'",
			`postgres migration replay rejects CREATE TABLESPACE` + refused},
		{"PostgreSQL event trigger", trustedPostgresDev, "CREATE EVENT TRIGGER t ON ddl_command_start EXECUTE FUNCTION f()",
			`postgres migration replay rejects CREATE EVENT` + refused},
		{"PostgreSQL language", trustedPostgresDev, "CREATE LANGUAGE plperl",
			`postgres migration replay rejects CREATE LANGUAGE` + refused},
		{"PostgreSQL cast", trustedPostgresDev, "CREATE CAST (int AS text) WITH INOUT",
			`postgres migration replay rejects CREATE CAST` + refused},
		{"PostgreSQL subscription", trustedPostgresDev, "CREATE SUBSCRIPTION s CONNECTION 'host=elsewhere' PUBLICATION p",
			`postgres migration replay rejects CREATE SUBSCRIPTION` + refused},
		{"PostgreSQL foreign server", trustedPostgresDev, "CREATE SERVER s FOREIGN DATA WRAPPER postgres_fdw",
			`postgres migration replay rejects CREATE SERVER` + refused},
		{"PostgreSQL large object", trustedPostgresDev, "SELECT lo_import('/etc/passwd')",
			`postgres migration replay rejects large object operation` + refused},
		{"PostgreSQL replication slot", trustedPostgresDev, "SELECT pg_create_logical_replication_slot('s', 'pgoutput')",
			`postgres migration replay rejects replication state operation` + refused},
		{"PostgreSQL extension update", trustedPostgresDev, "ALTER EXTENSION vector UPDATE",
			`postgres migration replay rejects ALTER EXTENSION` + refused + provisionedServerRemedy},
		{"PostgreSQL extension change", trustedPostgresDev, "ALTER EXTENSION vector SET SCHEMA app",
			`postgres migration replay rejects ALTER EXTENSION` + refused},
		{"PostgreSQL comment on an extension", trustedPostgresDev, "COMMENT ON EXTENSION pgcrypto IS 'hashing'",
			`postgres migration replay rejects COMMENT ON global metadata` + refused + provisionedServerRemedy},
		{"CockroachDB global default privileges", trustedCockroachDev,
			"ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO app_rw",
			`cockroachdb migration replay rejects ALTER DEFAULT PRIVILEGES FOR ALL ROLES without IN SCHEMA` + refused + ownedServerRemedy},
		{"CockroachDB cluster setting", trustedCockroachDev, "SET CLUSTER SETTING sql.defaults.distsql = 'off'",
			`cockroachdb migration replay rejects SET CLUSTER SETTING` + refused},
		{"MySQL executable comment", trustedMySQLDev, "/*!50000 CREATE TABLE t (id int) */",
			`mysql migration replay rejects executable comment` + refused},
		{"MySQL event", trustedMySQLDev, "CREATE EVENT e ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM t",
			`mysql migration replay rejects CREATE executable stored body` + refused},
		{"MySQL event a definer alters", trustedMySQLDev, "ALTER DEFINER=app@localhost.localdomain EVENT e DO DELETE FROM t",
			`mysql migration replay rejects ALTER executable stored body` + refused},
		{"MySQL USE", trustedMySQLDev, "USE other",
			`mysql migration replay rejects USE` + refused},
		{"MySQL global SET", trustedMySQLDev, "SET GLOBAL max_connections = 10",
			`mysql migration replay rejects global or persistent SET` + refused},
		{"MySQL plugin", trustedMySQLDev, "INSTALL PLUGIN p SONAME 'p.so'",
			`mysql migration replay rejects INSTALL` + refused},
		{"MySQL ALTER USER", trustedMySQLDev, "ALTER USER app@'%' IDENTIFIED BY 'x'",
			`mysql migration replay rejects ALTER USER` + refused},
		{"MySQL FLUSH", trustedMySQLDev, "FLUSH PRIVILEGES",
			`mysql migration replay rejects FLUSH` + refused},
		{"MySQL tablespace", trustedMySQLDev, "CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd'",
			`mysql migration replay rejects CREATE TABLESPACE` + refused},
		{"MySQL ALTER DATABASE", trustedMySQLDev, "ALTER DATABASE ptah_dev CHARACTER SET utf8mb4",
			`mysql migration replay rejects ALTER DATABASE` + refused},
		{"MySQL TEMP object", trustedMySQLDev, "CREATE TEMPORARY TABLE x (id int)",
			`mysql migration replay rejects TEMP object` + refused},
		{"MySQL client character set", trustedMySQLDev, "SET NAMES gbk",
			`mysql migration replay rejects client character set change` + refused},
		{"MySQL global SET by prefix", trustedMySQLDev, "SET @@global.max_connections = 10",
			`mysql migration replay rejects global or persistent SET` + refused},
		{"MySQL event behind a dollar sign host", trustedMySQLDev, "CREATE DEFINER=root@$ EVENT e ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM t",
			`mysql migration replay rejects CREATE executable stored body` + refused},
		{"MySQL event a SET STATEMENT runs", trustedMySQLDev, "SET STATEMENT max_statement_time=0 FOR CREATE EVENT e ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM t",
			`mysql migration replay rejects CREATE executable stored body` + refused},
		{"MySQL CREATE of a kind it does not know", trustedMySQLDev, "CREATE LIBRARY lib LANGUAGE JAVASCRIPT AS 'export function f() {}'",
			`mysql migration replay rejects CREATE statement whose object kind cannot be read` + refused},
		{"SQL Server USE", trustedSQLServerDev, "USE other",
			`sqlserver migration replay rejects USE` + refused},
		{"SQL Server permission", trustedSQLServerDev, "GRANT SELECT ON dbo.items TO app",
			`sqlserver migration replay rejects permission mutation` + refused},
		{"SQL Server database principal", trustedSQLServerDev, "CREATE USER app FOR LOGIN app",
			`sqlserver migration replay rejects CREATE DATABASE PRINCIPAL` + refused},
		{"SQL Server server trigger", trustedSQLServerDev, "CREATE TRIGGER t ON ALL SERVER FOR CREATE_DATABASE AS SELECT 1",
			`sqlserver migration replay rejects ON SERVER or ON ALL SERVER` + refused},
		{"SQL Server DBCC", trustedSQLServerDev, "DBCC CHECKDB",
			`sqlserver migration replay rejects DBCC server operation` + refused},
		{"SQL Server assembly", trustedSQLServerDev, "CREATE ASSEMBLY a FROM 0x00",
			`sqlserver migration replay rejects CREATE ASSEMBLY` + refused},
		{"SQL Server temporary object", trustedSQLServerDev, "CREATE TABLE #t (id int)",
			`sqlserver migration replay rejects temporary object` + refused},
		{"SQL Server ALTER LOGIN", trustedSQLServerDev, "ALTER LOGIN app WITH PASSWORD = 'x'",
			`sqlserver migration replay rejects ALTER LOGIN` + refused},
		{"SQL Server scoped credential", trustedSQLServerDev, "CREATE DATABASE SCOPED CREDENTIAL c WITH IDENTITY = 'x'",
			`sqlserver migration replay rejects CREATE DATABASE` + refused},
		{"ClickHouse ATTACH", trustedClickHouseDev, "ATTACH TABLE ptah_dev.t",
			`clickhouse migration replay rejects ATTACH` + refused},
		{"ClickHouse ALTER USER", trustedClickHouseDev, "ALTER USER app IDENTIFIED WITH plaintext_password BY 'x'",
			`clickhouse migration replay rejects ALTER USER` + refused},
		{"ClickHouse SET ROLE", trustedClickHouseDev, "SET ROLE r",
			`clickhouse migration replay rejects role change` + refused},
		{"ClickHouse TEMP object", trustedClickHouseDev, "CREATE TEMPORARY TABLE x (id UInt8) ENGINE = Memory",
			`clickhouse migration replay rejects TEMP object` + refused},
		{"ClickHouse PARALLEL WITH", trustedClickHouseDev,
			"CREATE TABLE ptah_dev.a (id UInt8) ENGINE = Memory PARALLEL WITH CREATE TABLE ptah_dev.b (id UInt8) ENGINE = Memory",
			`clickhouse migration replay rejects PARALLEL WITH` + refused},
		{"ClickHouse RESTORE", trustedClickHouseDev, "RESTORE DATABASE ptah_dev FROM Disk('backups', 'dev')",
			`clickhouse migration replay rejects RESTORE` + refused},
		{"SQLite ATTACH", trustedSQLiteDev, "ATTACH DATABASE '/tmp/other.db' AS other",
			`sqlite migration replay rejects ATTACH` + refused},
		{"SQLite PRAGMA", trustedSQLiteDev, "PRAGMA journal_mode = WAL",
			`sqlite migration replay rejects state-changing or unsupported PRAGMA` + refused},
		{"SQLite load_extension", trustedSQLiteDev, "SELECT load_extension('x')",
			`sqlite migration replay rejects load_extension` + refused},
		{"SQLite TEMP object", trustedSQLiteDev, "CREATE TEMP TABLE x (id int)",
			`sqlite migration replay rejects TEMP object` + refused},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			trustReplaySQL(c, test.info.URL)
			c.Assert(devclean.NewDevReplayGuard(test.info).ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestDevReplayGuard_TrustEndsWithTheRelease pins that trust lasts as long as
// the resolution that recorded it: once its release runs, a guard built for the
// same URL refuses again.
func TestDevReplayGuard_TrustEndsWithTheRelease(t *testing.T) {
	const routine = "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$"
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://dev@trust-ends:5432/ptah_dev"}
	c := qt.New(t)
	_, release, err := devdocker.Resolve(replaytrust.With(t.Context(), true), info.URL, devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(devclean.NewDevReplayGuard(info).ValidateStatement(routine), qt.IsNil)

	release()
	c.Assert(devclean.NewDevReplayGuard(info).ValidateStatement(routine), qt.ErrorMatches,
		`postgres migration replay rejects CREATE routine definition .*`+trustRemedy)
}

// TestDevReplayGuard_TrustIsPerURL pins that trust follows the dev URL it was
// recorded for: a guard for another dev server stays strict.
func TestDevReplayGuard_TrustIsPerURL(t *testing.T) {
	const routine = "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$"
	c := qt.New(t)
	trustReplaySQL(c, "postgres://dev@trust-this-one:5432/ptah_dev")
	other := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://dev@trust-not-this-one:5432/ptah_dev"}
	c.Assert(devclean.NewDevReplayGuard(other).ValidateStatement(routine), qt.ErrorMatches,
		`postgres migration replay rejects CREATE routine definition .*`+trustRemedy)
}

// TestDevReplayGuard_TrustedOnADeclaredServer pins that the two answers
// compose: on a server declared disposable whose SQL the run also trusts, what
// either lifts runs, and what both keep is refused, naming only the remedy
// that is left.
func TestDevReplayGuard_TrustedOnADeclaredServer(t *testing.T) {
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://dev@trusted-and-declared:5432/ptah_dev"}
	c := qt.New(t)
	declareDisposable(c, info.URL)
	trustReplaySQL(c, info.URL)
	guard := devclean.NewDevReplayGuard(info)

	c.Assert(devclean.DevReplayRealm(info), qt.Equals, devclean.ReplayRealmServer)
	c.Assert(guard.ValidateStatement("CREATE ROLE rebase_admin NOLOGIN"), qt.IsNil)
	c.Assert(guard.ValidateStatement("COPY items TO PROGRAM 'cat'"), qt.IsNil)
	c.Assert(guard.ValidateStatement("COMMENT ON EXTENSION pgcrypto IS 'hashing'"), qt.ErrorMatches,
		`postgres migration replay rejects COMMENT ON global metadata because its effects cannot be confined `+
			`to the disposable database realm`+provisionedServerRemedy)
}

// TestBaselineGuard_TrustedSQL pins that a plan rehearsal's baseline is held to
// the same answer as a replay on the same dev server: a routine the target
// holds is restored there when the run trusts the SQL, and a statement trust
// keeps refused is refused in the baseline's words.
func TestBaselineGuard_TrustedSQL(t *testing.T) {
	info := catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@trusted-baseline:5432/ptah_dev"}
	c := qt.New(t)
	trustReplaySQL(c, info.URL)
	guard := devclean.NewBaselineGuard(info)

	c.Assert(guard.ValidateStatement("CREATE FUNCTION item_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM items $$"), qt.IsNil)
	c.Assert(guard.ValidateStatement(`CREATE ROLE "reader" WITH NOLOGIN`), qt.IsNil)
	c.Assert(guard.ValidateStatement("SET search_path TO app"), qt.ErrorMatches,
		`postgres rehearsal baseline refuses SET search_path because its effects cannot be confined to the dev database realm`)
}

// trustedMySQLWholeServer is a MySQL dev URL that names no database, so the
// realm is every user database of the server rather than one of them.
var trustedMySQLWholeServer = catalog.ServerInfo{
	Dialect:     platform.MySQL,
	URL:         "mysql://dev@trusted-mysql-whole:3306/",
	WholeServer: true,
}

// wholeServerTrustLifted are statements that realm refuses because a user, a
// role, a privilege or a stored body outlives its cleanup.
var wholeServerTrustLifted = []struct {
	name      string
	statement string
}{
	{"user", "CREATE USER app@'%'"},
	{"role", "CREATE ROLE app_reader"},
	{"privilege", "GRANT SELECT ON *.* TO app@'%'"},
	{"procedure", "CREATE PROCEDURE app.refresh() BEGIN SELECT 1; END"},
}

// TestDevReplayGuard_TrustedSQL_OnAWholeMySQLServer pins that trust is read the
// same way in the whole-server realm: each statement runs when the run trusts
// the SQL, and the realm stays the server's user databases.
func TestDevReplayGuard_TrustedSQL_OnAWholeMySQLServer(t *testing.T) {
	for _, test := range wholeServerTrustLifted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			trustReplaySQL(c, trustedMySQLWholeServer.URL)
			c.Assert(devclean.DevReplayRealm(trustedMySQLWholeServer), qt.Equals, devclean.ReplayRealmServerDatabases)
			c.Assert(devclean.NewDevReplayGuard(trustedMySQLWholeServer).ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// TestDevReplayGuard_WithoutTrust_OnAWholeMySQLServer is its control: without
// trust each one is refused, and the refusal names the trust remedy.
func TestDevReplayGuard_WithoutTrust_OnAWholeMySQLServer(t *testing.T) {
	for _, test := range wholeServerTrustLifted {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.NewDevReplayGuard(trustedMySQLWholeServer).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, `mysql migration replay rejects .*`+trustRemedy)
		})
	}
}
