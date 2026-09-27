//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// Every writer lists what its reset drops, and the claim refuses a dev database
// holding any of it. Measured on 2026-09-27, each object alone in the dev
// database: the pinned community binary v1.3.0 keeps every MySQL and MariaDB
// object below, and on SQLite drops the view as Ptah does; it opens no SQL
// Server, ClickHouse or Oracle URL. Without the listing, ptah-compat's replay
// and rehearsal dropped each of them at exit 0 (stokaro/ptah#3851). Each test
// reads the object back after the refusal.

// engineObjectDev is a scratch dev database with a connection to it.
type engineObjectDev struct {
	url  string
	conn *dbschema.DatabaseConnection
}

// mySQLFamilyObjectDev is a scratch database on a MySQL-family server, created
// and dropped through the administrative account the URL also carries.
func mySQLFamilyObjectDev(admin, driverAdmin dbtarget.Engine) func(c *qt.C) engineObjectDev {
	return func(c *qt.C) engineObjectDev {
		c.Helper()
		db, err := sql.Open("mysql", dbtarget.DriverDSN(c, driverAdmin))
		c.Assert(err, qt.IsNil)
		name := devDialectScratchName()
		createMySQLDatabase(c, c.Context(), db, name)
		c.Cleanup(func() {
			dropMySQLDatabase(c, context.Background(), db, name)
			c.Check(db.Close(), qt.IsNil)
		})
		devURL := databaseInURL(c, dbtarget.URL(c, admin), name)
		return engineObjectDev{url: devURL, conn: connectDevDialect(c, devURL)}
	}
}

// sqliteObjectDev is a SQLite file of the test's own.
func sqliteObjectDev(c *qt.C) engineObjectDev {
	c.Helper()
	devURL := "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "dev.db"))
	return engineObjectDev{url: devURL, conn: connectDevDialect(c, devURL)}
}

// fromDialectDatabase adapts a dev_not_clean_dialects_e2e_test.go scratch
// database.
func fromDialectDatabase(server func(c *qt.C) devDialectDatabase) func(c *qt.C) engineObjectDev {
	return func(c *qt.C) engineObjectDev {
		c.Helper()
		dev := server(c)
		return engineObjectDev{url: dev.url, conn: dev.conn}
	}
}

var (
	mySQLObjectDev   = mySQLFamilyObjectDev(dbtarget.MySQLAdmin, dbtarget.MySQLAdmin)
	mariaDBObjectDev = mySQLFamilyObjectDev(dbtarget.MariaDBAdmin, dbtarget.MariaDBAdmin)
)

// engineObjects are the objects a reset drops besides tables, one per row,
// each with the statements that create it alone in the dev database, the query
// that counts it, and the object the refusal names.
var engineObjects = []struct {
	name   string
	server func(c *qt.C) engineObjectDev
	ddl    []string
	count  string
	reason string
}{
	{name: "MySQL view", server: mySQLObjectDev, ddl: []string{"CREATE VIEW keep_v AS SELECT 1 AS x"},
		count: "SELECT count(*) FROM information_schema.views WHERE table_schema = DATABASE()", reason: `found view "keep_v" in schema "ptah_dc_\d+"`},
	{name: "MySQL procedure", server: mySQLObjectDev, ddl: []string{"CREATE PROCEDURE keep_p() SELECT 1"},
		count: "SELECT count(*) FROM information_schema.routines WHERE routine_schema = DATABASE()", reason: `found procedure "keep_p" in schema "ptah_dc_\d+"`},
	{name: "MySQL function", server: mySQLObjectDev, ddl: []string{"CREATE FUNCTION keep_f() RETURNS INT DETERMINISTIC RETURN 1"},
		count: "SELECT count(*) FROM information_schema.routines WHERE routine_schema = DATABASE()", reason: `found function "keep_f" in schema "ptah_dc_\d+"`},
	{name: "MySQL event", server: mySQLObjectDev, ddl: []string{"CREATE EVENT keep_ev ON SCHEDULE EVERY 1 DAY DO SELECT 1"},
		count: "SELECT count(*) FROM information_schema.events WHERE event_schema = DATABASE()", reason: `found event "keep_ev" in schema "ptah_dc_\d+"`},
	{name: "MariaDB view", server: mariaDBObjectDev, ddl: []string{"CREATE VIEW keep_v AS SELECT 1 AS x"},
		count: "SELECT count(*) FROM information_schema.views WHERE table_schema = DATABASE()", reason: `found view "keep_v" in schema "ptah_dc_\d+"`},
	{name: "MariaDB sequence", server: mariaDBObjectDev, ddl: []string{"CREATE SEQUENCE keep_s"},
		count: "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'keep_s'", reason: `found sequence "keep_s" in schema "ptah_dc_\d+"`},
	{
		// The table probe reads BASE TABLE, as the binary's check does, so a
		// system-versioned table is named by the reset's listing instead.
		name: "MariaDB system-versioned table", server: mariaDBObjectDev,
		ddl:   []string{"CREATE TABLE keep_sv (id int PRIMARY KEY) WITH SYSTEM VERSIONING"},
		count: "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'keep_sv'", reason: `found table "keep_sv" in schema "ptah_dc_\d+"`,
	},
	{name: "SQLite view", server: sqliteObjectDev, ddl: []string{"CREATE VIEW keep_v AS SELECT 1 AS x"},
		count: "SELECT count(*) FROM sqlite_master WHERE name = 'keep_v'", reason: `found view "keep_v"`},
	{name: "SQL Server procedure", server: fromDialectDatabase(sqlServerDevDatabase), ddl: []string{"CREATE PROCEDURE dbo.keep_p AS SELECT 1"},
		count: "SELECT count(*) FROM sys.procedures WHERE name = 'keep_p'", reason: `found procedure "keep_p" in schema "dbo"`},
	{name: "SQL Server function", server: fromDialectDatabase(sqlServerDevDatabase), ddl: []string{"CREATE FUNCTION dbo.keep_f() RETURNS int AS BEGIN RETURN 1 END"},
		count: "SELECT count(*) FROM sys.objects WHERE name = 'keep_f'", reason: `found function "keep_f" in schema "dbo"`},
	{name: "SQL Server synonym", server: fromDialectDatabase(sqlServerDevDatabase), ddl: []string{"CREATE SYNONYM dbo.keep_syn FOR dbo.nothing"},
		count: "SELECT count(*) FROM sys.synonyms WHERE name = 'keep_syn'", reason: `found synonym "keep_syn" in schema "dbo"`},
	{name: "SQL Server type", server: fromDialectDatabase(sqlServerDevDatabase), ddl: []string{"CREATE TYPE dbo.keep_t FROM int"},
		count: "SELECT count(*) FROM sys.types WHERE name = 'keep_t'", reason: `found type "keep_t" in schema "dbo"`},
	{name: "SQL Server schema", server: fromDialectDatabase(sqlServerDevDatabase), ddl: []string{"CREATE SCHEMA keep_schema"},
		count: "SELECT count(*) FROM sys.schemas WHERE name = 'keep_schema'", reason: `found schema "keep_schema"`},
	{
		name: "SQL Server XML schema collection", server: fromDialectDatabase(sqlServerDevDatabase),
		ddl: []string{`CREATE XML SCHEMA COLLECTION dbo.keep_x AS N'<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">` +
			`<xsd:element name="a" type="xsd:string"/></xsd:schema>'`},
		count: "SELECT count(*) FROM sys.xml_schema_collections WHERE name = 'keep_x'", reason: `found xml schema collection "keep_x" in schema "dbo"`,
	},
	{
		name: "ClickHouse dictionary", server: fromDialectDatabase(clickHouseDevDatabase),
		ddl:   []string{"CREATE DICTIONARY keep_d (id UInt64, v String) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"},
		count: "SELECT toInt64(count()) FROM system.dictionaries WHERE database = currentDatabase() AND name = 'keep_d'", reason: `found dictionary "keep_d" in schema "ptah_dc_\d+"`,
	},
	{name: "Oracle sequence", server: fromDialectDatabase(oracleDevDatabase), ddl: []string{"CREATE SEQUENCE keep_s"},
		count: "SELECT count(*) FROM user_sequences WHERE sequence_name = 'KEEP_S'", reason: `found sequence "KEEP_S" in schema "PTAH_DC_\d+"`},
	{name: "Oracle synonym", server: fromDialectDatabase(oracleDevDatabase), ddl: []string{"CREATE SYNONYM keep_syn FOR dual"},
		count: "SELECT count(*) FROM user_synonyms WHERE synonym_name = 'KEEP_SYN'", reason: `found synonym "KEEP_SYN" in schema "PTAH_DC_\d+"`},
	{name: "Oracle type", server: fromDialectDatabase(oracleDevDatabase), ddl: []string{"CREATE TYPE keep_t AS OBJECT (x NUMBER)"},
		count: "SELECT count(*) FROM user_types WHERE type_name = 'KEEP_T'", reason: `found type "KEEP_T" in schema "PTAH_DC_\d+"`},
}

// TestDevDatabaseClaimRefusesEachObjectTheResetDropsLive claims a dev
// database holding one object the reset drops. The claim refuses it and names
// the object, and the object is still there.
func TestDevDatabaseClaimRefusesEachObjectTheResetDropsLive(t *testing.T) {
	for _, object := range engineObjects {
		t.Run(object.name, func(t *testing.T) {
			c := qt.New(t)
			dev := object.server(c)
			for _, statement := range object.ddl {
				_, err := dev.conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			_, err := devclean.Claim(c.Context(), dev.conn)

			c.Assert(err, qt.ErrorMatches, `connected database is not clean: `+object.reason+
				`; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database`)
			var count int
			c.Assert(dev.conn.QueryRowContext(c.Context(), object.count).Scan(&count), qt.IsNil)
			c.Assert(count, qt.Equals, 1)
		})
	}
}

// engineObjectServer is one engine the dialect test does not cover, with the
// administrative account a target database on it is created through.
type engineObjectServer struct {
	name   string
	server func(c *qt.C) engineObjectDev
	target dbtarget.Engine
}

// engineObjectMySQLFamily are the MySQL-family servers.
var engineObjectMySQLFamily = []engineObjectServer{
	{name: "MySQL", server: mySQLObjectDev, target: dbtarget.MySQLAdmin},
	{name: "MariaDB", server: mariaDBObjectDev, target: dbtarget.MariaDBAdmin},
}

// engineObjectServers adds SQLite, which needs no target account.
var engineObjectServers = append(slices.Clone(engineObjectMySQLFamily),
	engineObjectServer{name: "SQLite", server: sqliteObjectDev},
)

// TestDevDatabaseClaimTakesAnEmptyMySQLFamilyDatabaseLive is the control on
// the engines the dialect test does not cover.
func TestDevDatabaseClaimTakesAnEmptyMySQLFamilyDatabaseLive(t *testing.T) {
	for _, server := range engineObjectServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			dev := server.server(c)

			_, err := devclean.Claim(c.Context(), dev.conn)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestCompatVerbsRefuseAMySQLFamilyViewTheResetDropsE2E replays a migration
// directory and rehearses a plan with a view alone in the dev database, where
// the binary v1.3.0 keeps the view at exit 0. Both refuse, and the view stays.
func TestCompatVerbsRefuseAMySQLFamilyViewTheResetDropsE2E(t *testing.T) {
	verbs := []struct {
		name   string
		args   []string
		prefix string
	}{
		{
			name:   "migrate validate",
			args:   []string{"migrate", "validate", "--dir", "{dir}", "--dev-url", "{dev}"},
			prefix: `replaying the migration directory: sql/migrate: taking database snapshot: `,
		},
		{
			name:   "schema apply",
			args:   []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"},
			prefix: `sql/migrate: taking database snapshot: `,
		},
	}

	for _, server := range engineObjectMySQLFamily {
		for _, verb := range verbs {
			t.Run(server.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				dev := server.server(c)
				_, err := dev.conn.ExecContext(c.Context(), "CREATE VIEW keep_v AS SELECT 1 AS x")
				c.Assert(err, qt.IsNil)
				target := newDevIdentityTarget(c, server.target, "")
				dir := filepath.Join(c.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_a.sql"),
					[]byte("CREATE TABLE added (id int NOT NULL, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
				_, err = migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
				c.Assert(err, qt.IsNil)
				schemaFile, _ := writeDevIdentitySources(c)

				output, err := runCompatVerb(devNotCleanArgs(verb.args, dev.url, target.url, schemaFile, dir, "")...)

				c.Assert(err, qt.ErrorMatches, verb.prefix+
					`sql/migrate: connected database is not clean: found view "keep_v" in schema "ptah_dc_\d+"`,
					qt.Commentf("%s", output))
				var views int
				c.Assert(dev.conn.QueryRowContext(c.Context(),
					"SELECT count(*) FROM information_schema.views WHERE table_schema = DATABASE()").Scan(&views), qt.IsNil)
				c.Assert(views, qt.Equals, 1)
			})
		}
	}
}
