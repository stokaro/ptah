//go:build integration

package providercli_test

import (
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	// The tests read the fixture's database files back through a driver of
	// their own, so the answer does not pass through the provider.
	_ "modernc.org/sqlite"

	"ptah.run/internal/devclean"
	"ptah.run/internal/devlock"
	"ptah.run/internal/providerhost"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// The fixture's SQLite is the engine: the provider cleans its dev databases,
// guards what a rehearsal runs on them, provisions them, creates realms in a
// database the operator names, and stops a statement at its timeout. The
// expected state comes from reading the database files back.

type devCLI struct {
	providerCLI
	dir, devPath string
}

func devdbSpec(c *qt.C, dir string) providerhost.Spec {
	c.Helper()
	spec := migrationProviderCommandSpec(c, "devdb")
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_DIR="+dir)
	return spec
}

func newDevCLI(c *qt.C) devCLI {
	c.Helper()
	dir := c.TempDir()
	f := devCLI{providerCLI: newProviderCLI(c, devdbSpec(c, dir)), dir: dir}
	f.devPath = filepath.Join(filepath.Dir(f.manifestPath), "dev.db")
	return f
}

func fixtureAddress(host, path string) string {
	return (&url.URL{Scheme: "fixture", Host: host, Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
}

func (f devCLI) applyWithDev(c *qt.C, devURL string) (string, int) {
	c.Helper()
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--dev-url", devURL, "--auto-approve", "--json")
	return diagnostics, code
}

// tables lists the tables a fixture database file holds.
func tables(c *qt.C, path string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

func createTable(c *qt.C, path, name string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), `CREATE TABLE "`+name+`" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
}

func entries(c *qt.C, dir string) []string {
	c.Helper()
	found, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(found))
	for _, entry := range found {
		names = append(names, entry.Name())
	}
	return names
}

// The plan is rehearsed on the dev database before it reaches the target: a
// dev database whose provider runs nothing stops the apply, and a clean one is
// handed back empty.
func TestProviderRehearsesThePlanOnItsDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	diagnostics, code := f.applyWithDev(c, fixtureAddress("", f.devPath)+"?fault=reject")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Not(qt.Equals), "")
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)

	diagnostics, code = f.applyWithDev(c, fixtureAddress("", f.devPath))
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"items", "second"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
}

// The provider's listing decides whether the dev database is clean, and one
// that holds a table nobody's run put there is refused before it is reset.
func TestProviderRefusesADevDatabaseThatIsNotClean(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	createTable(c, f.devPath, "leftover")
	diagnostics, code := f.applyWithDev(c, fixtureAddress("", f.devPath))
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, `connected database is not clean: found table "leftover"`)
	c.Assert(tables(c, f.devPath), qt.DeepEquals, []string{"leftover"})
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)
}

// A dev URL that names the target is refused by what the two sessions
// select, since the host reads no provider address.
func TestProviderDevDatabaseMustNotBeTheTarget(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	diagnostics, code := f.applyWithDev(c, f.address)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "--dev-url must not point at the target database")
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)
}

// A cleanup whose reply is lost leaves the dev database not clean, and the
// apply stops before the target.
func TestProviderLostCleanupLeavesTheDevDatabaseNotClean(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	diagnostics, code := f.applyWithDev(c, fixtureAddress("", f.devPath)+"?fault=lost-cleanup")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "the provider did not confirm the dev database cleanup")
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)
}

// Both docker URL forms reach the provider, which provisions a dev database
// and removes it when the run ends.
func TestProviderProvisionsADevDatabaseForEitherDockerForm(t *testing.T) {
	for _, devURL := range []string{"docker+fixture://_/example.org/fixture:2.0", "docker://fixture/2.0"} {
		t.Run(devURL, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)
			diagnostics, code := f.applyWithDev(c, devURL)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"items", "second"})
			c.Assert(entries(c, f.dir), qt.HasLen, 0)
		})
	}
}

// An engine whose provider creates realms gets one for the run inside the
// database the operator names. The rehearsal runs in the realm, which the
// release removes, and the operator's database keeps what it held.
func TestProviderRehearsesInARealmOfTheOperatorsDatabase(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	operator := filepath.Join(f.dir, "operator.db")
	createTable(c, operator, "kept")
	diagnostics, code := f.applyWithDev(c, fixtureAddress("localhost", operator))
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"items", "second"})
	c.Assert(tables(c, operator), qt.DeepEquals, []string{"kept"})
	c.Assert(entries(c, f.dir), qt.DeepEquals, []string{"operator.db"})
}

// The provider judges each statement a run replays on its dev database. A
// provider that offers no guard leaves every statement refused.
func TestProviderGuardsReplayedStatements(t *testing.T) {
	for _, row := range []struct {
		program, statement, wantErr string
	}{
		{"devdb", `PRAGMA journal_mode = OFF`, `fixture migration replay rejects PRAGMA changes the replay session's settings because .*`},
		{"devdb", `ATTACH 'other.db' AS other`, `fixture migration replay rejects ATTACH reaches another database file because .*`},
		// Text the wire cannot carry gets no verdict, and no verdict is a
		// refusal.
		{"devdb", "SELECT '\xff'", `fixture migration replay rejects a statement the provider could not judge because .*`},
		{"provider", `CREATE TABLE "x" ("id" INTEGER)`, `fixture migration replay rejects unsupported dialect because .*`},
	} {
		t.Run(row.program+" "+row.statement, func(t *testing.T) {
			c := qt.New(t)
			host, _ := yamlProviderCommand(c, row.program)
			conn, _ := transactionConnection(c, host, "")
			c.Assert(devclean.NewDevReplayGuardFor(conn).ValidateStatement(row.statement), qt.ErrorMatches, row.wantErr)
		})
	}
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	conn, _ := transactionConnection(c, host, "")
	c.Assert(devclean.NewDevReplayGuardFor(conn).ValidateStatement(`CREATE TABLE "x" ("id" INTEGER)`), qt.IsNil)
}

// The dev database services are the engine's as well as the provider's. An
// engine that declares none of them is neither cleaned nor guarded, even by a
// provider that declares both for another engine.
func TestProviderEngineWithoutDevServicesIsNeitherCleanedNorGuarded(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	spec := devdbSpec(c, dir)
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_ENGINE=bare")
	f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
	devPath := filepath.Join(dir, "dev.db")

	diagnostics, code := f.applyWithDev(c, fixtureAddress("", devPath))

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "the experimental provider contract does not support database cleanup")
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)

	host, _ := yamlProviderEnvironment(c, "devdb", []string{"FIXTURE_DEV_ENGINE=bare"})
	conn, _ := transactionConnection(c, host, "")
	c.Assert(devclean.NewDevReplayGuardFor(conn).ValidateStatement(`CREATE TABLE "x" ("id" INTEGER)`), qt.ErrorMatches,
		`fixture migration replay rejects unsupported dialect because .*`)
}

// A migration's statement timeout reaches the provider with each unit, in a
// migration's own transaction and outside one, and the provider stops a unit
// that runs longer: nothing of it applies. tx-mode all takes no
// per-migration timeout on any engine.
func TestProviderStopsAMigrationStatementAtItsTimeout(t *testing.T) {
	for _, mode := range []string{"file", "none"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			f := newDevCLI(c)
			dir := migrationDirectory(c)
			f.address += "?fault=slow-unit"
			out, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f.providerCLI, "up", "--migrations-dir", dir,
				"--tx-mode", mode, "--statement-timeout", "300ms", "--json"))
			f.assertStopped(c)
			c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
			c.Assert(diagnostics+out, qt.Contains, "NOT_APPLIED at the statement timeout")
			c.Assert(tables(c, f.databasePath), qt.Not(qt.Contains), "records")

			// The control: the same timeout on a unit that ends in time.
			fresh := newDevCLI(c)
			runProviderMigration(c, fresh.providerCLI, "up", "--migrations-dir", dir, "--tx-mode", mode, "--statement-timeout", "30s", "--json")
			c.Assert(tables(c, fresh.databasePath), qt.Contains, "records")
		})
	}
}

// The migrator refuses a lock timeout on a provider that stops statements
// itself, for a caller that reaches it without the command's own check.
func TestProviderMigratorRefusesALockTimeout(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	conn, _ := transactionConnection(c, host, "")
	migration := migrator.CreateMigrationFromSQL(1, "records", `CREATE TABLE "records" ("id" INTEGER);`, `DROP TABLE "records";`)
	run := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithDefaultTimeouts(migrationfile.Timeouts{LockTimeout: time.Second, HasLockTimeout: true})
	c.Assert(run.MigrateUp(c.Context()), qt.ErrorMatches, `(?s).*a lock timeout cannot be honored.*`)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
}

// A lock timeout has nothing on the provider's side to bound, so it is
// refused before anything runs.
func TestProviderRefusesAMigrationLockTimeout(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	dir := migrationDirectory(c)
	_, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f.providerCLI, "up", "--migrations-dir", dir, "--lock-timeout", "1s"))
	f.assertStopped(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "has no lock wait Ptah can bound around a migration")
	c.Assert(tables(c, f.databasePath), qt.Not(qt.Contains), "records")
}

// A replay holds the dev database's realm through the provider's session
// locks, so a second replay of the same database waits for it.
func TestProviderDevRealmLockExcludesASecondReplay(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	first, address := transactionConnection(c, host, "")
	second := connectTransactionAddress(c, host, address)
	held, err := devlock.Acquire(c.Context(), first, 0)
	c.Assert(err, qt.IsNil)
	refused, err := devlock.Acquire(c.Context(), second, -1)
	c.Assert(err, qt.ErrorMatches, `acquire dev database realm lock: .*`)
	c.Assert(refused, qt.IsNil)
	c.Assert(held.Release(), qt.IsNil)
	again, err := devlock.Acquire(c.Context(), second, -1)
	c.Assert(err, qt.IsNil)
	c.Assert(again.Release(), qt.IsNil)
}

// lintMigrations is a hashed migration directory whose first version creates
// "records" and whose second runs drop, each with a rollback.
func lintMigrations(c *qt.C, drop string) string {
	c.Helper()
	dir := c.TempDir()
	for name, statement := range map[string]string{
		"0000000001_records.up.sql":   `CREATE TABLE "records" ("id" INTEGER);`,
		"0000000001_records.down.sql": `DROP TABLE "records";`,
		"0000000002_drop.up.sql":      drop,
		"0000000002_drop.down.sql":    `CREATE TABLE "records" ("id" INTEGER);`,
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(statement), 0o600), qt.IsNil)
	}
	_, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "hash", "--dir", dir})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	return dir
}

// lintWithDev lints dir with a provider that cleans and guards its dev
// databases and owns the lint rules of its engine, replaying it on devPath.
func lintWithDev(c *qt.C, dir, devPath string, leftover ...string) (out, diagnostics string, code int) {
	c.Helper()
	f := newProviderCLI(c, migrationProviderCommandSpec(c, "devdb-lint"))
	for _, table := range leftover {
		createTable(c, devPath, table)
	}
	out, diagnostics, code = executeProviderCLI(c.Context(), []string{
		"migrations", "lint", "--provider-config", f.manifestPath, "--dir", dir, "--dev-url", fixtureAddress("", devPath),
	})
	f.assertStopped(c)
	return out, diagnostics, code
}

// A lint run replays the directory on a dev database its provider cleans and
// guards: the provider's rule reports the drop, and the dev database is empty
// again when the run ends.
func TestProviderLintReplaysMigrationsOnItsDevDatabase(t *testing.T) {
	c := qt.New(t)
	devPath := filepath.Join(c.TempDir(), "dev.db")

	out, diagnostics, code := lintWithDev(c, lintMigrations(c, `DROP TABLE "records";`), devPath)

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "0000000002_drop.up.sql:1 [error] FX101: DROP TABLE removes every row")
	c.Assert(tables(c, devPath), qt.HasLen, 0)
}

// The replay is what judges a statement the dev database refuses, and the
// provider's listing is what judges a dev database clean: one that holds a
// table is refused before the replay, and keeps the table.
func TestProviderLintReplaysMigrationsOnItsDevDatabase_FailurePath(t *testing.T) {
	t.Run("a statement the dev database refuses", func(t *testing.T) {
		c := qt.New(t)
		devPath := filepath.Join(c.TempDir(), "dev.db")

		out, diagnostics, code := lintWithDev(c, lintMigrations(c, `DROP TABLE "gadgets";`), devPath)

		c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
		c.Assert(diagnostics, qt.Contains, "error validating migration SQL on dev database: replay migration 2 on dev database: ")
		c.Assert(tables(c, devPath), qt.HasLen, 0)
	})
	t.Run("a dev database that is not clean", func(t *testing.T) {
		c := qt.New(t)
		devPath := filepath.Join(c.TempDir(), "dev.db")

		out, diagnostics, code := lintWithDev(c, lintMigrations(c, `DROP TABLE "records";`), devPath, "leftover")

		c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
		c.Assert(diagnostics, qt.Contains, `connected database is not clean: found table "leftover"`)
		c.Assert(tables(c, devPath), qt.DeepEquals, []string{"leftover"})
	})
}
