//go:build integration

package providercli_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// The migration-units fixture splits a body by a toy grammar the host cannot
// read itself: `DEFINE name = literal;` lives only in the unit that carries
// it, a run of INSERT, UPDATE and DELETE is one unit that commits with its
// checkpoint, every other statement runs outside a transaction, and COMMIT is
// refused. Each subtest is one row of ADR 0055's table, named for the fixture
// column, or one rule the ADR states beside it.

// unitsFixture is the migration-units program, built once.
type unitsFixture struct{ binary, digest string }

func buildUnitsFixture(c *qt.C) unitsFixture {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "migration-units")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return unitsFixture{binary: binary, digest: hex.EncodeToString(digest[:])}
}

func (u unitsFixture) spec(environment ...string) providerhost.Spec {
	return providerhost.Spec{Executable: u.binary, SHA256: u.digest, ProviderID: "example.org/fixture", Environment: environment}
}

// unitsDatabase is one fixture database and the call log of the provider
// processes that served it.
type unitsDatabase struct{ path, address, log string }

func newUnitsDatabase(c *qt.C) unitsDatabase {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, "units.db")
	return unitsDatabase{path: path, log: filepath.Join(dir, "calls.log"),
		address: (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()}
}

// connect starts a provider with environment and opens a session on the
// database through it.
func (u unitsFixture) connect(c *qt.C, database unitsDatabase, environment ...string) *dbschema.DatabaseConnection {
	c.Helper()
	host, err := providerhost.Start(c.Context(), u.spec(append(environment, "FIXTURE_UNITS_LOG="+database.log)...))
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	return connectTransactionAddress(c, host, database.address)
}

func unitsMigrator(c *qt.C, conn *dbschema.DatabaseConnection, files map[string]string, options ...migrator.FSProviderOption) *migrator.Migrator {
	c.Helper()
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	m, err := migrator.NewFSMigrator(conn, fsys, options...)
	c.Assert(err, qt.IsNil)
	return m
}

// unitsProgress is what a test reads off one revision.
type unitsProgress struct {
	Version int64
	State   string
	Applied int
	Total   int
}

func revisionsOf(c *qt.C, m *migrator.Migrator) []unitsProgress {
	c.Helper()
	revisions, err := m.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	progress := make([]unitsProgress, 0, len(revisions))
	for _, revision := range revisions {
		progress = append(progress, unitsProgress{Version: revision.Version, State: revision.State, Applied: revision.Applied, Total: revision.Total})
	}
	return progress
}

// rowCount counts the rows of a table, read straight from the SQLite file.
func rowCount(c *qt.C, database unitsDatabase, table string) int64 {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(database.path))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int64
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT count(*) FROM "`+table+`"`).Scan(&count), qt.IsNil)
	return count
}

// tableExists reports whether the SQLite file holds a table.
func tableExists(c *qt.C, database unitsDatabase, table string) bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(database.path))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int64
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&count), qt.IsNil)
	return count == 1
}

// calls returns the lines of the call log that begin with one of prefixes.
func calls(c *qt.C, database unitsDatabase, prefixes ...string) []string {
	c.Helper()
	data, err := os.ReadFile(database.log)
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(strings.Split(strings.TrimSpace(string(data)), "\n"), func(line string) bool {
		return !slices.ContainsFunc(prefixes, func(prefix string) bool { return strings.HasPrefix(line, prefix) })
	})
}

// executed is the call log's line for an execution of sql.
func executed(transaction bool, timeout, sql string) string {
	return "execute tx=" + strconv.FormatBool(transaction) + " timeout=" + timeout + " sql=" + strconv.Quote(sql)
}

const unknownOutcome = "statement execution outcome is unknown after process interruption"

// unitsFiles is a directory of one migration.
func unitsFiles(up, down string) map[string]string {
	return map[string]string{"0000000001_units.up.sql": up, "0000000001_units.down.sql": down}
}

// offeredUnits is an interceptor that takes over nothing and records each
// unit it was offered.
type offeredUnits struct{ offered []string }

func (o *offeredUnits) ValidateDirectives(map[string]string) error { return nil }
func (o *offeredUnits) ExecuteStatement(_ context.Context, _ *dbschema.DatabaseConnection, statement string, _ map[string]string) (bool, error) {
	o.offered = append(o.offered, statement)
	return false, nil
}

func TestProviderMigrationUnits(t *testing.T) {
	fixture := buildUnitsFixture(qt.New(t))
	const create = `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`

	t.Run("CLI lifecycle, carried definition", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		f := newProviderCLI(c, fixture.spec("FIXTURE_UNITS_LOG="+database.log))
		f.address = database.address
		dir := c.TempDir()
		up := create + "\nDEFINE name = 'first';\n" + `INSERT INTO "items" VALUES (1, $name);` + "\n" + `INSERT INTO "items" VALUES (2, $name || 'x');` + "\n"
		c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.up.sql"), []byte(up), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.down.sql"), []byte(`DROP TABLE "items";`), 0o600), qt.IsNil)

		out := runProviderMigration(c, f, "up", "--migrations-dir", dir, "--statement-timeout", "30s", "--json")

		var result migrator.RunResult
		c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil)
		c.Assert(result.Applied, qt.DeepEquals, []int64{1})
		c.Assert(rowCount(c, database, "items"), qt.Equals, int64(2))
		c.Assert(calls(c, database, "execute"), qt.DeepEquals, []string{
			executed(false, "30s", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT)`),
			executed(true, "30s", "DEFINE name = 'first';\n"+`INSERT INTO "items" VALUES (1, $name);`+"\n"+`INSERT INTO "items" VALUES (2, $name || 'x')`),
		})
		c.Assert(calls(c, database, "split"), qt.DeepEquals, []string{"split " + strconv.Quote(up)})
		runProviderMigration(c, f, "down", "--migrations-dir", dir, "--target", "0", "--confirm")
		c.Assert(tableExists(c, database, "items"), qt.IsFalse)
	})

	t.Run("counts 4/4 and 2/2, native and Atlas", func(t *testing.T) {
		for _, format := range []migrator.RevisionTableFormat{migrator.RevisionTableFormatPtah, migrator.RevisionTableFormatAtlas} {
			t.Run(string(format), func(t *testing.T) {
				c := qt.New(t)
				database := newUnitsDatabase(c)
				conn := fixture.connect(c, database)
				files := map[string]string{
					"0000000001_items.up.sql": create + "\nDEFINE name = 'first';\n" + `INSERT INTO "items" VALUES (1, $name);` + "\n" +
						`INSERT INTO "items" VALUES (2, $name);` + "\n" + `ALTER TABLE "items" ADD COLUMN "note" TEXT;` + "\n" +
						`UPDATE "items" SET "note" = $name WHERE "id" = 1;` + "\n",
					"0000000001_items.down.sql": `DROP TABLE "items";`,
					"0000000002_tags.up.sql":    `CREATE TABLE "tags" ("id" INTEGER NOT NULL PRIMARY KEY);` + "\n" + `INSERT INTO "tags" VALUES (1);` + "\n",
					"0000000002_tags.down.sql":  `DROP TABLE "tags";`,
				}
				m := unitsMigrator(c, conn, files).WithRevisionTableFormat(format)

				c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
				c.Assert(revisionsOf(c, m), qt.DeepEquals, []unitsProgress{
					{Version: 1, State: "applied", Applied: 4, Total: 4},
					{Version: 2, State: "applied", Applied: 2, Total: 2},
				})
				c.Assert(rowCount(c, database, "items"), qt.Equals, int64(2))
				c.Assert(m.MigrateDownTo(c.Context(), 0), qt.IsNil)
				c.Assert(revisionsOf(c, m), qt.HasLen, 0)
				c.Assert(tableExists(c, database, "items"), qt.IsFalse)
			})
		}
	})

	t.Run("failed 2/3, resume at unit 3", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		head := `CREATE TABLE "a" ("id" INTEGER NOT NULL PRIMARY KEY);` + "\nDEFINE id = 1;\n" + `INSERT INTO "a" VALUES ($id);` + "\n"
		broken := unitsFiles(head+`CREATE TABLE "a" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "a";`)
		fixed := unitsFiles(head+`CREATE TABLE "b" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "b"; DROP TABLE "a";`)

		failed := unitsMigrator(c, conn, broken).MigrateUp(c.Context())

		c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*NOT_APPLIED.*`)
		c.Assert(revisionsOf(c, unitsMigrator(c, conn, broken)), qt.DeepEquals, []unitsProgress{{Version: 1, State: "failed", Applied: 2, Total: 3}})
		resumed := unitsMigrator(c, conn, fixed)
		c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
		c.Assert(revisionsOf(c, resumed), qt.DeepEquals, []unitsProgress{{Version: 1, State: "applied", Applied: 3, Total: 3}})
		c.Assert(rowCount(c, database, "a"), qt.Equals, int64(1))
		c.Assert(tableExists(c, database, "b"), qt.IsTrue)
	})

	t.Run("key conflict leaves neither row, failed 1/2", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		head := `CREATE TABLE "c" ("id" INTEGER NOT NULL PRIMARY KEY);` + "\n" + `INSERT INTO "c" VALUES (1);` + "\n"
		conflicting := unitsFiles(head+`INSERT INTO "c" VALUES (1);`, `DROP TABLE "c";`)
		fixed := unitsFiles(head+`INSERT INTO "c" VALUES (2);`, `DROP TABLE "c";`)

		failed := unitsMigrator(c, conn, conflicting).MigrateUp(c.Context())

		c.Assert(failed, qt.ErrorMatches, `(?s)failed to apply migration 1: .*NOT_APPLIED.*`)
		c.Assert(rowCount(c, database, "c"), qt.Equals, int64(0))
		c.Assert(revisionsOf(c, unitsMigrator(c, conn, conflicting)), qt.DeepEquals, []unitsProgress{{Version: 1, State: "failed", Applied: 1, Total: 2}})
		resumed := unitsMigrator(c, conn, fixed)
		c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
		c.Assert(rowCount(c, database, "c"), qt.Equals, int64(2))
	})

	t.Run("refusal, no revision row", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		m := unitsMigrator(c, conn, unitsFiles(`CREATE TABLE "r" ("id" INTEGER);`+"\n"+`INSERT INTO "r" VALUES (1);`+"\nCOMMIT;\n", `DROP TABLE "r";`))

		err := m.MigrateUp(c.Context())

		c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up on fixture: the provider refuses "COMMIT;": the statement controls a transaction, `+
			`and the host runs each data unit in a transaction of its own`)
		c.Assert(revisionsOf(c, m), qt.HasLen, 0)
		c.Assert(tableExists(c, database, "r"), qt.IsFalse)
		c.Assert(calls(c, database, "execute", "begin"), qt.HasLen, 0)
	})

	t.Run("applied 1, 2, 3 by ReadHistory", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		host, err := providerhost.Start(c.Context(), fixture.spec())
		c.Assert(err, qt.IsNil)
		c.Cleanup(host.Close)
		conn := connectTransactionAddress(c, host, database.address)
		reader := migrator.NewMigrator(connectTransactionAddress(c, host, database.address), migrator.NewRegisteredMigrationProvider())
		var seen []int
		observer := migrator.StatementObserverFunc(func(ctx context.Context, _ migrator.StatementEvent) error {
			revisions, err := reader.GetRevisions(ctx)
			seen = append(seen, revisions[0].Applied)
			return err
		})
		files := unitsFiles(`CREATE TABLE "g" ("id" INTEGER NOT NULL PRIMARY KEY);`+"\n"+`INSERT INTO "g" VALUES (1);`+"\n"+`INSERT INTO "g" VALUES (2);`+"\n"+
			`ALTER TABLE "g" ADD COLUMN "note" TEXT;`+"\n", `DROP TABLE "g";`)

		c.Assert(unitsMigrator(c, conn, files, migrator.WithStatementObserver(observer)).MigrateUp(c.Context()), qt.IsNil)
		c.Assert(seen, qt.DeepEquals, []int{1, 2, 3})
	})

	t.Run("edit inside a committed source", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		body := func(value, last string) map[string]string {
			return unitsFiles(`CREATE TABLE "e" ("id" INTEGER NOT NULL PRIMARY KEY);`+"\nDEFINE id = 1;\n"+`INSERT INTO "e" VALUES (`+value+`);`+"\n"+
				`CREATE TABLE "`+last+`" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "f"; DROP TABLE "e";`)
		}
		c.Assert(unitsMigrator(c, conn, body("$id", "e")).MigrateUp(c.Context()), qt.IsNotNil)

		err := unitsMigrator(c, conn, body("$id * 7", "f")).MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

		c.Assert(err, qt.ErrorMatches, `migration 1 cannot resume automatically: the already committed statement prefix `+
			`changed after 2 of 3 statements committed; inspect the database before choosing a repair point`)
		c.Assert(tableExists(c, database, "f"), qt.IsFalse)
	})

	t.Run("interceptor sees OUTSIDE only", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		interceptor := &offeredUnits{}
		files := unitsFiles(`CREATE TABLE "h" ("id" INTEGER NOT NULL PRIMARY KEY);`+"\n"+`INSERT INTO "h" VALUES (1);`+"\n"+
			`ALTER TABLE "h" ADD COLUMN "note" TEXT;`+"\n", `DROP TABLE "h";`)

		c.Assert(unitsMigrator(c, conn, files, migrator.WithStatementInterceptor(interceptor)).MigrateUp(c.Context()), qt.IsNil)
		c.Assert(interceptor.offered, qt.DeepEquals, []string{
			`CREATE TABLE "h" ("id" INTEGER NOT NULL PRIMARY KEY)`, `ALTER TABLE "h" ADD COLUMN "note" TEXT`,
		})
		c.Assert(rowCount(c, database, "h"), qt.Equals, int64(1))
	})

	t.Run("lost commit reply; row keeps 2/3", func(t *testing.T) {
		files := unitsFiles(`CREATE TABLE "l" ("id" INTEGER NOT NULL PRIMARY KEY);`+"\n"+`INSERT INTO "l" VALUES (1);`+"\n"+
			`CREATE TABLE "m" ("id" INTEGER NOT NULL PRIMARY KEY);`+"\n", `DROP TABLE "m"; DROP TABLE "l";`)
		t.Run("the read-back settles it", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			m := unitsMigrator(c, fixture.connect(c, database, "FIXTURE_UNITS_LOSE_COMMIT=1"), files)

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(revisionsOf(c, m), qt.DeepEquals, []unitsProgress{{Version: 1, State: "applied", Applied: 3, Total: 3}})
			c.Assert(rowCount(c, database, "l"), qt.Equals, int64(1))
		})
		t.Run("the failure keeps what the row records", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			failing := unitsMigrator(c, fixture.connect(c, database, "FIXTURE_UNITS_LOSE_COMMIT=1", "FIXTURE_UNITS_LOSE_READS=1"), files)

			failed := failing.MigrateUp(c.Context())

			c.Assert(failed, qt.ErrorMatches, `(?s).*provider finish transaction failed.*read back whether the query committed: .*provider read history failed.*`)
			resumed := unitsMigrator(c, fixture.connect(c, database), files)
			c.Assert(revisionsOf(c, resumed), qt.DeepEquals, []unitsProgress{{Version: 1, State: "failed", Applied: 2, Total: 3}})
			c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
			c.Assert(revisionsOf(c, resumed), qt.DeepEquals, []unitsProgress{{Version: 1, State: "applied", Applied: 3, Total: 3}})
			c.Assert(rowCount(c, database, "l"), qt.Equals, int64(1))
		})
	})

	t.Run("ResumeFrom 2, no in-flight mark", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		head := `CREATE TABLE "r" ("id" INTEGER NOT NULL PRIMARY KEY);` + "\n"
		broken := unitsFiles(head+`CREATE TABLE "r" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "r";`)
		fixed := unitsFiles(head+`INSERT INTO "r" VALUES (1);`, `DROP TABLE "r";`)
		c.Assert(unitsMigrator(c, fixture.connect(c, database), broken).MigrateUp(c.Context()), qt.IsNotNil)
		c.Assert(os.Remove(database.log), qt.IsNil)
		repaired := unitsMigrator(c, fixture.connect(c, database), fixed)

		err := repaired.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})

		c.Assert(err, qt.IsNil)
		c.Assert(revisionsOf(c, repaired), qt.DeepEquals, []unitsProgress{{Version: 1, State: "applied", Applied: 2, Total: 2}})
		c.Assert(rowCount(c, database, "r"), qt.Equals, int64(1))
		c.Assert(calls(c, database, "begin", "execute", "history tx=true", "finish"), qt.DeepEquals, []string{
			"begin isolation=SERIALIZABLE",
			executed(true, "0s", `INSERT INTO "r" VALUES (1)`),
			`history tx=true applied=2 error=""`,
			"finish action=COMMIT outcome=COMMITTED",
		})
		c.Assert(strings.Join(calls(c, database, "history"), "\n"), qt.Not(qt.Contains), unknownOutcome)
	})

	t.Run("refusal on repair; row stays failed 1/2", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		head := `CREATE TABLE "r" ("id" INTEGER NOT NULL PRIMARY KEY);` + "\n"
		c.Assert(unitsMigrator(c, conn, unitsFiles(head+`CREATE TABLE "r" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "r";`)).MigrateUp(c.Context()), qt.IsNotNil)
		repaired := unitsMigrator(c, conn, unitsFiles(head+"COMMIT;\n"+`CREATE TABLE "s" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "r";`))

		err := repaired.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})

		c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up on fixture: the provider refuses "COMMIT;": .*`)
		c.Assert(tableExists(c, database, "s"), qt.IsFalse)
		c.Assert(revisionsOf(c, repaired), qt.DeepEquals, []unitsProgress{{Version: 1, State: "failed", Applied: 1, Total: 2}})
	})

	t.Run("OUTSIDE placement", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		files := unitsFiles(`CREATE TABLE "p" ("id" INTEGER NOT NULL PRIMARY KEY);`+"\n"+`INSERT INTO "p" VALUES (1);`+"\n"+
			`ALTER TABLE "p" ADD COLUMN "note" TEXT;`+"\n"+`DELETE FROM "p";`+"\n", `DROP TABLE "p";`)

		c.Assert(unitsMigrator(c, fixture.connect(c, database), files).MigrateUp(c.Context()), qt.IsNil)
		c.Assert(calls(c, database, "begin", "execute"), qt.DeepEquals, []string{
			executed(false, "0s", `CREATE TABLE "p" ("id" INTEGER NOT NULL PRIMARY KEY)`),
			"begin isolation=SERIALIZABLE", executed(true, "0s", `INSERT INTO "p" VALUES (1)`),
			executed(false, "0s", `ALTER TABLE "p" ADD COLUMN "note" TEXT`),
			"begin isolation=SERIALIZABLE", executed(true, "0s", `DELETE FROM "p"`),
		})
	})

	t.Run("no Execute, no BeginTransaction", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		conn.SchemaWriter().SetDryRun(true)
		files := unitsFiles(create+"\n"+`INSERT INTO "items" VALUES (1, 'a');`+"\n", `DROP TABLE "items";`)

		c.Assert(unitsMigrator(c, conn, files).MigrateUp(c.Context()), qt.IsNil)
		c.Assert(calls(c, database, "split"), qt.HasLen, 1)
		c.Assert(calls(c, database, "execute", "begin", "history"), qt.HasLen, 0)
		c.Assert(tableExists(c, database, "items"), qt.IsFalse)
	})

	t.Run("a conflict abort runs the unit again under the bound", func(t *testing.T) {
		files := unitsFiles(create+"\n"+`INSERT INTO "items" VALUES (1, 'a');`+"\n", `DROP TABLE "items";`)
		for _, row := range []struct{ name, fault string }{
			{"a commit", "FIXTURE_UNITS_ABORT_COMMITS=1"}, {"an execution", "FIXTURE_UNITS_ABORT_EXECUTIONS=1"},
		} {
			t.Run(row.name, func(t *testing.T) {
				c := qt.New(t)
				database := newUnitsDatabase(c)
				m := unitsMigrator(c, fixture.connect(c, database, row.fault), files)

				c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
				c.Assert(rowCount(c, database, "items"), qt.Equals, int64(1))
				c.Assert(calls(c, database, "begin"), qt.HasLen, 2)
			})
		}
		t.Run("every attempt", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			m := unitsMigrator(c, fixture.connect(c, database, "FIXTURE_UNITS_ABORT_COMMITS=9"), files)

			err := m.MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, `(?s).*the engine aborted the transaction for a conflict and applied nothing.*`)
			c.Assert(calls(c, database, "abort commit"), qt.HasLen, 5)
			c.Assert(rowCount(c, database, "items"), qt.Equals, int64(0))
			c.Assert(revisionsOf(c, m), qt.DeepEquals, []unitsProgress{{Version: 1, State: "failed", Applied: 1, Total: 2}})
		})
	})

	t.Run("a stopped unit is not applied and leaves no mark", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		files := unitsFiles("-- +ptah statement_timeout=200ms\n"+create+"\nSLOW;\n", `DROP TABLE "items";`)

		err := unitsMigrator(c, conn, files).MigrateUp(c.Context())

		c.Assert(err, qt.ErrorMatches, `(?s).*NOT_APPLIED at the statement timeout.*`)
		revisions, readErr := unitsMigrator(c, conn, files).GetRevisions(c.Context())
		c.Assert(readErr, qt.IsNil)
		c.Assert(revisions, qt.HasLen, 1)
		c.Assert([]any{revisions[0].State, revisions[0].Applied, revisions[0].Total}, qt.DeepEquals, []any{"failed", 1, 2})
		c.Assert(revisions[0].Error, qt.Not(qt.Contains), unknownOutcome)
	})

	t.Run("transactional DDL", func(t *testing.T) {
		data := unitsFiles(`INSERT INTO "kept" VALUES (1);`+"\n"+`INSERT INTO "kept" VALUES (2);`+"\n", `DELETE FROM "kept";`)
		mixed := unitsFiles(`CREATE TABLE "x" ("id" INTEGER);`+"\n"+`INSERT INTO "x" VALUES (1);`+"\n", `DROP TABLE "x";`)
		t.Run("a file of data units runs in one transaction", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			conn := fixture.connect(c, database, "FIXTURE_UNITS_DDL=transactional")
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "kept" ("id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
			c.Assert(os.Remove(database.log), qt.IsNil)

			c.Assert(unitsMigrator(c, conn, data).MigrateUp(c.Context()), qt.IsNil)
			c.Assert(calls(c, database, "begin", "execute"), qt.DeepEquals, []string{
				"begin isolation=DEFAULT", executed(true, "0s", `INSERT INTO "kept" VALUES (1);`+"\n"+`INSERT INTO "kept" VALUES (2)`),
			})
			c.Assert(rowCount(c, database, "kept"), qt.Equals, int64(2))
		})
		for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeAll} {
			t.Run("an OUTSIDE unit refuses "+string(mode), func(t *testing.T) {
				c := qt.New(t)
				database := newUnitsDatabase(c)
				m := unitsMigrator(c, fixture.connect(c, database, "FIXTURE_UNITS_DDL=transactional"), mixed).WithTransactionMode(mode)

				err := m.MigrateUp(c.Context())

				c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up on fixture with tx-mode `+string(mode)+`: its unit 1 runs outside any `+
					`transaction, so one transaction cannot hold the body; run it with tx-mode none, which commits each unit on its own`)
				c.Assert(tableExists(c, database, "x"), qt.IsFalse)
			})
		}
		t.Run("a file whose commit the engine aborts records a failure that applied nothing", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			conn := fixture.connect(c, database, "FIXTURE_UNITS_DDL=transactional", "FIXTURE_UNITS_ABORT_COMMITS=9")
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "kept" ("id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
			m := unitsMigrator(c, conn, data).WithTransactionMode(migrator.MigrationTxModeFile)

			err := m.MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, `failed to commit transaction for migration 1: the engine aborted the transaction for a conflict and applied nothing: .*`)
			c.Assert(rowCount(c, database, "kept"), qt.Equals, int64(0))
			c.Assert(revisionsOf(c, m), qt.DeepEquals, []unitsProgress{{Version: 1, State: "failed", Applied: 0, Total: 1}})
		})
		t.Run("none runs an OUTSIDE unit on its own", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			m := unitsMigrator(c, fixture.connect(c, database, "FIXTURE_UNITS_DDL=transactional"), mixed).WithTransactionMode(migrator.MigrationTxModeNone)

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(revisionsOf(c, m), qt.DeepEquals, []unitsProgress{{Version: 1, State: "applied", Applied: 2, Total: 2}})
		})
	})

	t.Run("file on an engine without transactional DDL runs as none, and all is refused", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		conn := fixture.connect(c, database)
		files := unitsFiles(create+"\n"+`INSERT INTO "items" VALUES (1, 'a');`+"\n", `DROP TABLE "items";`)

		refused := unitsMigrator(c, conn, files).WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(c.Context())

		c.Assert(refused, qt.ErrorMatches, `tx-mode all is not supported for dialect "fixture": .*`)
		c.Assert(unitsMigrator(c, conn, files).WithTransactionMode(migrator.MigrationTxModeFile).MigrateUp(c.Context()), qt.IsNil)
		c.Assert(rowCount(c, database, "items"), qt.Equals, int64(1))
	})

	t.Run("a check takes the provider's verdict", func(t *testing.T) {
		refused := unitsFiles("-- +ptah check name=feed assert=\"SELECT COUNT(*) = 0 FROM external_feed\"\n"+create+"\n", `DROP TABLE "items";`)
		const verdict = `(?s).*pre-migration check feed for migration 1 could not run: check assertion must be one read-only SELECT statement: ` +
			`the assertion reads an external table, whose rows are not in the database.*`
		t.Run("a real run", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)

			err := unitsMigrator(c, fixture.connect(c, database), refused).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, verdict)
			c.Assert(tableExists(c, database, "items"), qt.IsFalse)
		})
		t.Run("a dry run", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			conn := fixture.connect(c, database)
			conn.SchemaWriter().SetDryRun(true)

			c.Assert(unitsMigrator(c, conn, refused).MigrateUp(c.Context()), qt.ErrorMatches, verdict)
		})
		t.Run("a read-only assertion runs", func(t *testing.T) {
			c := qt.New(t)
			database := newUnitsDatabase(c)
			files := unitsFiles("-- +ptah check name=empty assert=\"SELECT COUNT(*) = 0 FROM sqlite_schema WHERE name = 'items'\"\n"+create+"\n", `DROP TABLE "items";`)

			c.Assert(unitsMigrator(c, fixture.connect(c, database), files).MigrateUp(c.Context()), qt.IsNil)
			c.Assert(tableExists(c, database, "items"), qt.IsTrue)
		})
	})

	t.Run("ptah-compat migrate apply counts and hashes units", func(t *testing.T) {
		c := qt.New(t)
		database := newUnitsDatabase(c)
		f := newProviderCLI(c, fixture.spec("FIXTURE_UNITS_LOG="+database.log))
		c.Setenv("PTAH_PROVIDER_CONFIG", f.manifestPath)
		dir := filepath.Join(c.TempDir(), "migrations")
		c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
		body := create + "\nDEFINE name = 'first';\n" + `INSERT INTO "items" VALUES (1, $name);` + "\n" + `ALTER TABLE "items" ADD COLUMN "note" TEXT;` + "\n"
		c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_items.sql"), []byte(body), 0o600), qt.IsNil)
		dirURL := "file://" + filepath.ToSlash(dir)
		_, diagnostics, code := runCompat(c, "migrate", "hash", "--dir", dirURL)
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))

		out, diagnostics, code := runCompat(c, "migrate", "apply", "--dir", dirURL, "--url", database.address)

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
		c.Assert(out, qt.Contains, "Migrating to version 20240101000000 (1 migrations in total):\n")
		c.Assert(out, qt.Contains, "\n  -- 1 migration\n")
		c.Assert(rowCount(c, database, "items"), qt.Equals, int64(1))
		reader, err := migrator.NewFSMigrator(fixture.connect(c, database), os.DirFS(dir), migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
		c.Assert(err, qt.IsNil)
		reader = reader.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).WithMigrationsTable("", "atlas_schema_revisions")
		c.Assert(revisionsOf(c, reader), qt.DeepEquals, []unitsProgress{{Version: 20240101000000, State: "applied", Applied: 3, Total: 3}})
	})

	t.Run("a provider that does not split refuses a longer body outside a transaction", func(t *testing.T) {
		c := qt.New(t)
		spec := migrationProviderCommandSpec(c, "native-migrations")
		host, err := providerhost.Start(c.Context(), spec)
		c.Assert(err, qt.IsNil)
		c.Cleanup(host.Close)
		database := newUnitsDatabase(c)
		conn := connectTransactionAddress(c, host, database.address)
		files := unitsFiles(`CREATE TABLE "a" ("id" INTEGER);`+"\n"+`CREATE TABLE "b" ("id" INTEGER);`+"\n"+`INSERT INTO "a" VALUES (1);`+"\n"+
			`INSERT INTO "b" VALUES (1);`+"\n", `DROP TABLE "b"; DROP TABLE "a";`)

		err = unitsMigrator(c, conn, files).WithTransactionMode(migrator.MigrationTxModeNone).MigrateUp(c.Context())

		c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up on fixture: the provider does not split a migration body into the units `+
			`its engine runs \(units.migration-split\), so a body of 4 statements cannot run outside a transaction; upgrade the provider`)
		c.Assert(tableExists(c, database, "a"), qt.IsFalse)
		c.Assert(unitsMigrator(c, conn, files).MigrateUp(c.Context()), qt.IsNil)
		c.Assert(rowCount(c, database, "b"), qt.Equals, int64(1))
	})
}

// runCompat runs ptah-compat in this process.
func runCompat(c *qt.C, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	code := root.RunContext(c.Context(), cmd, args...)
	return out.String(), diagnostics.String(), code
}
