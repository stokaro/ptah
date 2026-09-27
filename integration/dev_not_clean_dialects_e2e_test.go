//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// A dev database is reset before and after a run uses it, and a reset drops
// its tables. The check that refuses a dev database holding a table covers
// every engine Ptah replays or rehearses on. The pinned community binary
// refuses a CockroachDB dev database holding a table when it reaches one
// through a postgres:// URL, and does not open Spanner, SQL Server, ClickHouse
// or Oracle at all. Without the check, the reset drops the table on each
// engine below (stokaro/ptah#3811). Each test reads the table back after the
// refusal.

// devDialectDatabase is a scratch database on one engine: its URL, an open
// connection, the statements that create keep_me with a row in it, the query
// that counts the rows, statements that leave only objects the check does not
// count, and statements that leave objects the reset drops besides tables.
type devDialectDatabase struct {
	url          string
	conn         *dbschema.DatabaseConnection
	keepDDL      []string
	keepRows     string
	uncountedDDL []string
	droppedDDL   []string
	// viewCount counts the view "v" droppedDDL creates.
	viewCount string
}

// exec runs statements on the scratch database.
func (d devDialectDatabase) exec(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := d.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// keptRows counts the rows of keep_me.
func (d devDialectDatabase) keptRows(c *qt.C) int {
	c.Helper()
	var rows int
	c.Assert(d.conn.QueryRowContext(c.Context(), d.keepRows).Scan(&rows), qt.IsNil)
	return rows
}

// devDialectScratchName is a database name unique to one test. Spanner takes
// at most 30 characters.
func devDialectScratchName() string {
	return fmt.Sprintf("ptah_dc_%d", time.Now().UnixNano()%1_000_000_000_000_000)
}

// connectDevDialect opens rawURL, closed when the test ends.
func connectDevDialect(c *qt.C, rawURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), rawURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// createdDevDialectDatabase creates a scratch database through adminURL with
// CREATE DATABASE and returns the URL naming it. dropStatement removes it when
// the test ends; %s is the name.
func createdDevDialectDatabase(c *qt.C, adminURL, dropStatement string, rename func(c *qt.C, adminURL, name string) string) string {
	c.Helper()
	admin := connectDevDialect(c, adminURL)
	name := devDialectScratchName()
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), fmt.Sprintf(dropStatement, name))
		c.Check(dropErr, qt.IsNil)
	})
	return rename(c, adminURL, name)
}

// renameInPath names the database in the URL's path.
func renameInPath(c *qt.C, adminURL, name string) string {
	c.Helper()
	renamed, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	return renamed
}

// cockroachDevDatabase is a scratch database on CockroachDB.
func cockroachDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.CockroachDB),
		"DROP DATABASE IF EXISTS %s CASCADE", renameInPath)
	return devDialectDatabase{
		url:        devURL,
		conn:       connectDevDialect(c, devURL),
		keepDDL:    []string{"CREATE TABLE keep_me (id int PRIMARY KEY)", "INSERT INTO keep_me VALUES (1)"},
		keepRows:   "SELECT count(*) FROM keep_me",
		droppedDDL: []string{"CREATE VIEW v AS SELECT 1 AS id", "CREATE SEQUENCE s"},
		viewCount:  "SELECT count(*) FROM information_schema.views WHERE table_name = 'v'",
	}
}

// cockroachPinnedDevDatabase is cockroachDevDatabase reached through a URL
// that pins search_path=public, so the claim judges that schema alone.
func cockroachPinnedDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	dev := cockroachDevDatabase(c)
	parsed, err := url.Parse(dev.url)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", "public")
	parsed.RawQuery = query.Encode()
	dev.url = parsed.String()
	dev.conn = connectDevDialect(c, dev.url)
	return dev
}

// yugabyteDevDatabase is a scratch database on YugabyteDB.
func yugabyteDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.YugabyteDB),
		"DROP DATABASE IF EXISTS %s", renameInPath)
	return devDialectDatabase{
		url:        devURL,
		conn:       connectDevDialect(c, devURL),
		keepDDL:    []string{"CREATE TABLE keep_me (id int PRIMARY KEY)", "INSERT INTO keep_me VALUES (1)"},
		keepRows:   "SELECT count(*) FROM keep_me",
		droppedDDL: []string{"CREATE VIEW v AS SELECT 1 AS id", "CREATE SEQUENCE s"},
		viewCount:  "SELECT count(*) FROM information_schema.views WHERE table_name = 'v'",
	}
}

// spannerDevDatabase is a scratch database on the Spanner emulator, which
// creates the database the URL names when a client first connects to it.
// keep_me is dropped when the test ends, since the database cannot be.
func spannerDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := renameInPath(c, dbtarget.URL(c, dbtarget.Spanner), devDialectScratchName())
	conn := connectDevDialect(c, devURL)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS keep_me")
		c.Check(err, qt.IsNil)
	})
	return devDialectDatabase{
		url:        devURL,
		conn:       conn,
		keepDDL:    []string{"CREATE TABLE keep_me (id bigint PRIMARY KEY)", "INSERT INTO keep_me VALUES (1)"},
		keepRows:   "SELECT count(*) FROM keep_me",
		droppedDDL: []string{"CREATE VIEW v SQL SECURITY INVOKER AS SELECT 1 AS id"},
		viewCount:  "SELECT count(*) FROM information_schema.views WHERE table_name = 'v'",
	}
}

// sqlServerDevDatabase is a scratch database on SQL Server.
func sqlServerDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
		"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s",
		replaceSQLServerDatabase)
	return devDialectDatabase{
		url:        devURL,
		conn:       connectDevDialect(c, devURL),
		keepDDL:    []string{"CREATE TABLE dbo.keep_me (id int PRIMARY KEY)", "INSERT INTO dbo.keep_me VALUES (1)"},
		keepRows:   "SELECT count(*) FROM dbo.keep_me",
		droppedDDL: []string{"CREATE VIEW dbo.v AS SELECT 1 AS id", "CREATE SEQUENCE dbo.s"},
		viewCount:  "SELECT count(*) FROM sys.views WHERE name = 'v'",
	}
}

// clickHouseDevDatabase is a scratch database on ClickHouse.
func clickHouseDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse),
		"DROP DATABASE IF EXISTS %s", renameInPath)
	return devDialectDatabase{
		url:  devURL,
		conn: connectDevDialect(c, devURL),
		keepDDL: []string{
			"CREATE TABLE keep_me (id Int32) ENGINE = MergeTree ORDER BY id",
			"INSERT INTO keep_me VALUES (1)",
		},
		keepRows: "SELECT toInt64(count()) FROM keep_me",
		// A materialized view with no TO clause keeps its rows in an
		// `.inner` table of its own, which goes with the view.
		droppedDDL: []string{
			"CREATE VIEW v AS SELECT 1 AS id",
			"CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id AS SELECT 1 AS id",
		},
		viewCount: "SELECT toInt64(count()) FROM system.tables WHERE database = currentDatabase() AND name = 'v'",
	}
}

// oracleDevDatabase is a scratch Oracle account, which is a schema of its
// own, created through the administrative account.
func oracleDevDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin := connectDevDialect(c, adminURL)
	account := strings.ToUpper(devDialectScratchName())
	createOracleUser(c.Context(), c, admin, account)
	c.Cleanup(func() { dropOracleUser(context.Background(), c, admin, account) })
	// The objects the reset drops besides tables and views, so a test can
	// leave one of each.
	execOracle(c.Context(), c, admin, "GRANT CREATE SEQUENCE, CREATE SYNONYM, CREATE TYPE TO "+account)
	devURL := oracleURLAs(c, adminURL, account)
	return devDialectDatabase{
		url:      devURL,
		conn:     connectDevDialect(c, devURL),
		keepDDL:  []string{"CREATE TABLE keep_me (id NUMBER PRIMARY KEY)", "INSERT INTO keep_me VALUES (1)"},
		keepRows: "SELECT count(*) FROM keep_me",
		// A dropped table goes to the recycle bin. Oracle Free 23 does not
		// list it in USER_OBJECTS, so this row holds without the probe's
		// BIN$ filter, which mirrors the reset's own object list.
		uncountedDDL: []string{
			"CREATE TABLE gone (id NUMBER)",
			"DROP TABLE gone",
		},
		droppedDDL: []string{"CREATE VIEW v AS SELECT 1 AS id FROM dual"},
		viewCount:  "SELECT count(*) FROM user_views WHERE view_name = 'V'",
	}
}

// devDialectEngine is one engine checked here, with the object its refusal
// names. The PostgreSQL-family URLs pin no search_path, so the whole database
// is judged and the schema is named.
type devDialectEngine struct {
	name   string
	server func(c *qt.C) devDialectDatabase
	reason string
}

// devDialectReplayEngines are the engines a migration directory is replayed
// on.
var devDialectReplayEngines = []devDialectEngine{
	{name: "CockroachDB", server: cockroachDevDatabase, reason: `found table "keep_me" in schema "public"`},
	{name: "YugabyteDB", server: yugabyteDevDatabase, reason: `found table "keep_me" in schema "public"`},
	{name: "SQL Server", server: sqlServerDevDatabase, reason: `found table "keep_me" in schema "dbo"`},
	{name: "ClickHouse", server: clickHouseDevDatabase, reason: `found table "keep_me" in schema "ptah_dc_\d+"`},
}

// devDialectEngines adds a CockroachDB URL that pins a schema, and the engines
// a replay refuses before it claims the dev database. On Spanner the replay's lock refuses it (`spanner replay cannot
// safely serialize destructive dev database use`), and a rehearsal claims it.
// On Oracle no run reaches a reset: the replay's lock and the rehearsal's
// identity check both refuse it first, with `unsupported dev database lock
// dialect "oracle"`. The claim checks it all the same, so a path that reaches
// the reset without them is refused too.
var devDialectEngines = append(slices.Clone(devDialectReplayEngines),
	devDialectEngine{name: "CockroachDB pinned to public", server: cockroachPinnedDevDatabase, reason: `found table "keep_me" in connected schema`},
	devDialectEngine{name: "Spanner", server: spannerDevDatabase, reason: `found table "keep_me" in schema "public"`},
	devDialectEngine{name: "Oracle", server: oracleDevDatabase, reason: `found table "KEEP_ME" in schema "PTAH_DC_\d+"`},
)

// TestDevDatabaseClaimRefusesATableOnEveryEngineLive claims a dev database
// holding keep_me on each engine. The claim refuses it, which is what stops
// the reset that would follow, and the row is still there.
func TestDevDatabaseClaimRefusesATableOnEveryEngineLive(t *testing.T) {
	for _, engine := range devDialectEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := engine.server(c)
			dev.exec(c, dev.keepDDL)

			_, err := devclean.Claim(c.Context(), dev.conn)

			c.Assert(err, qt.ErrorMatches, `connected database is not clean: `+engine.reason+
				`; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database`)
			c.Assert(dev.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestDevDatabaseClaimTakesADatabaseWithNoTableOnEveryEngineLive is the
// control: a scratch database holding nothing but what the check does not
// count is claimed. Every writer lists what its reset drops, so on most
// engines that is nothing at all; on Oracle it is a table in the recycle bin.
func TestDevDatabaseClaimTakesADatabaseWithNoTableOnEveryEngineLive(t *testing.T) {
	for _, engine := range devDialectEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := engine.server(c)
			dev.exec(c, dev.uncountedDDL)

			_, err := devclean.Claim(c.Context(), dev.conn)

			c.Assert(err, qt.IsNil)
		})
	}
}

// devDialectDroppingEngines are the engines checked here with the object each
// refusal names when the database holds a view "v" beside another object the
// reset drops: the first of them in the order the engine's reset lists them.
var devDialectDroppingEngines = []devDialectEngine{
	{name: "CockroachDB", server: cockroachDevDatabase, reason: `found sequence "s" in schema "public"`},
	{name: "CockroachDB pinned to public", server: cockroachPinnedDevDatabase, reason: `found sequence "s" in connected schema`},
	{name: "YugabyteDB", server: yugabyteDevDatabase, reason: `found sequence "s" in schema "public"`},
	{name: "Spanner", server: spannerDevDatabase, reason: `found view "v" in schema "public"`},
	{name: "SQL Server", server: sqlServerDevDatabase, reason: `found sequence "s" in schema "dbo"`},
	{name: "ClickHouse", server: clickHouseDevDatabase, reason: `found materialized view "mv" in schema "ptah_dc_\d+"`},
	{name: "Oracle", server: oracleDevDatabase, reason: `found view "V" in schema "PTAH_DC_\d+"`},
}

// TestDevDatabaseClaimRefusesAnObjectTheResetDropsOnEveryEngineLive claims a
// dev database holding a view and another object the reset drops, on each
// engine. The claim refuses the database and names one of them, and the view
// is still there (stokaro/ptah#3808, stokaro/ptah#3851).
func TestDevDatabaseClaimRefusesAnObjectTheResetDropsOnEveryEngineLive(t *testing.T) {
	for _, engine := range devDialectDroppingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := engine.server(c)
			dev.exec(c, dev.droppedDDL)

			_, err := devclean.Claim(c.Context(), dev.conn)

			c.Assert(err, qt.ErrorMatches, `connected database is not clean: `+engine.reason+
				`; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database`)
			var views int
			c.Assert(dev.conn.QueryRowContext(c.Context(), dev.viewCount).Scan(&views), qt.IsNil)
			c.Assert(views, qt.Equals, 1)
		})
	}
}

// TestCompatMigrateValidateRefusesADevDatabaseHoldingATableE2E replays a
// migration directory with the dev database holding keep_me, on the engines
// ptah-compat replays on besides those dev_not_clean_e2e_test.go covers. The
// replay refuses before its reset, and the row survives.
func TestCompatMigrateValidateRefusesADevDatabaseHoldingATableE2E(t *testing.T) {
	for _, engine := range devDialectReplayEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := engine.server(c)
			dev.exec(c, dev.keepDDL)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_a.sql"),
				[]byte("CREATE TABLE added (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
			_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
			c.Assert(err, qt.IsNil)

			output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", dev.url)

			c.Assert(err, qt.ErrorMatches,
				`replaying the migration directory: sql/migrate: taking database snapshot: sql/migrate: `+
					`connected database is not clean: `+engine.reason,
				qt.Commentf("%s", output))
			c.Assert(dev.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestCompatSchemaInspectLeavesSpannerSysOutE2E inspects a Spanner database at
// realm scope. The realm is the schemas the dev database check judges, and
// spanner_sys is the server's own: listed, it made an empty Spanner dev
// database read as not clean, and `schema inspect` wrote a block for it.
func TestCompatSchemaInspectLeavesSpannerSysOutE2E(t *testing.T) {
	c := qt.New(t)
	dev := spannerDevDatabase(c)

	output, err := runCompatVerb("schema", "inspect", "-u", dev.url)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, `schema "public"`)
	c.Assert(output, qt.Not(qt.Contains), "spanner_sys")
}
