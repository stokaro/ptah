package migrateup_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
)

// writeThreeUpMigrations writes a three-migration ptah-format directory, the
// smallest one where a bound at the middle version leaves work above it.
func writeThreeUpMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":      "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":    "DROP TABLE users;\n",
		"0000000002_orders.up.sql":     "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql":   "DROP TABLE orders;\n",
		"0000000003_invoices.up.sql":   "CREATE TABLE invoices (id INTEGER PRIMARY KEY);\n",
		"0000000003_invoices.down.sql": "DROP TABLE invoices;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// runUpThroughRoot drives the shipped command tree. The PTAH_* binding is
// installed on the root command, so a run assembled from the leaf alone reads
// no environment variable and can say nothing about a rule over one.
func runUpThroughRoot(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"migrations", "up"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// countRevisionTables answers whether a run created Ptah's revision table,
// which a dry run must not.
func countRevisionTables(c *qt.C, dbPath string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func TestMigrateUpToVersion_HappyPath(t *testing.T) {
	t.Run("the run stops at the named version", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "bounded.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "2")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Database is now at version: 2")
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))

		// The migration above the bound stays pending, and an unbounded run
		// takes it.
		out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
	})

	t.Run("a zero-padded version names the same migration", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "padded.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "0000000002")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
	})

	t.Run("a target the database holds exactly applies nothing", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "reached.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "2")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

		out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "2")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
		// The report is what tells the operator why nothing was applied, so an
		// empty selection is where it is needed rather than where it is dropped.
		c.Assert(out, qt.Contains, "=== MIGRATE UP ===")
		c.Assert(out, qt.Contains, "Current version: 2")
		c.Assert(out, qt.Contains, "Pending migrations: 1")
	})

	t.Run("an empty bound leaves a batch limit to decide", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "empty-bound.db")

		out, err := runUp(
			"--db-url", "sqlite://"+dbPath,
			"--migrations-dir", migrationsDir,
			"--to-version", "",
			"--limit", "2",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
	})

	t.Run("a zero limit leaves the bound to decide", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "zero-limit.db")

		out, err := runUp(
			"--db-url", "sqlite://"+dbPath,
			"--migrations-dir", migrationsDir,
			"--limit", "0",
			"--to-version", "2",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
	})
}

// TestMigrateUpToVersionAlreadyReachedReportsTheDryRunHeader is the same empty
// selection under --dry-run, where the lost report took the header saying
// nothing would be written with it.
func TestMigrateUpToVersionAlreadyReachedReportsTheDryRunHeader(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeThreeUpMigrations(c)
	dbPath := filepath.Join(c.TempDir(), "reached-dry.db")
	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "2")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runUp(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--to-version", "2",
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "=== DRY RUN MODE ===")
	c.Assert(out, qt.Contains, "=== MIGRATE UP ===")
	c.Assert(out, qt.Contains, "Would have applied 0 migrations")
}

// TestMigrateUpToVersionDryRunReportsBoundedCount pins the count a preview
// reports against the plan rather than against the pending list: three
// migrations are pending and the bound selects two of them.
func TestMigrateUpToVersionDryRunReportsBoundedCount(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeThreeUpMigrations(c)
	dbPath := filepath.Join(c.TempDir(), "dry.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "2", "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Pending migrations: 3")
	c.Assert(out, qt.Contains, "Would have applied 2 migrations")
	c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
}

func TestMigrateUpToVersion_FailurePath(t *testing.T) {
	t.Run("a version the directory does not carry", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "unknown.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "99")

		c.Assert(err, qt.ErrorMatches,
			`error running migrations: target version 99 was not found in the migration provider`,
			qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(0))
	})

	t.Run("a version the database has already passed", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "passed.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

		out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "2")

		c.Assert(err, qt.ErrorMatches,
			`error running migrations: cannot migrate up to version 2: the database already records version 3`,
			qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
	})

	t.Run("a version beside a batch limit", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "exclusive.db")

		out, err := runUp(
			"--db-url", "sqlite://"+dbPath,
			"--migrations-dir", migrationsDir,
			"--to-version", "2",
			"--limit", "1",
		)

		c.Assert(err, qt.ErrorMatches,
			`if any flags in the group \[limit to-version\] are set none of the others can be; \[limit to-version\] were all set`,
			qt.Commentf("%s", out))
		c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
	})

	t.Run("a version that is not a number", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "words.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "v2")

		c.Assert(err, qt.ErrorMatches, `invalid --to-version "v2": a migration version is a number`, qt.Commentf("%s", out))
		c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
	})

	t.Run("a version of zero", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "zero.db")

		out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", "0")

		c.Assert(err, qt.ErrorMatches,
			`invalid --to-version "0": a migration version is greater than zero`,
			qt.Commentf("%s", out))
		c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
	})
}

// TestMigrateUpWithoutToVersionReportsUpToDate is the control for the bound's
// hold on the no-pending shortcut. An unbounded run over a directory the
// database has fully applied still answers from the status read before the
// migration lock, so the bound did not take that answer away from every run.
func TestMigrateUpWithoutToVersionReportsUpToDate(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeThreeUpMigrations(c)
	dbPath := filepath.Join(c.TempDir(), "uptodate.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Database is already up to date!")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
}

// TestMigrateUpToVersionAndLimitFromTheEnvironment_FailurePath is the pair
// nobody typed. The refusal names the variables that carry it and arrives
// before the connection is opened, where the migrator's own refusal names an
// "amount" no flag spells and arrives under the migration lock.
func TestMigrateUpToVersionAndLimitFromTheEnvironment_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LIMIT", "1")
	t.Setenv("PTAH_TO_VERSION", "2")
	migrationsDir := writeThreeUpMigrations(c)
	dbPath := filepath.Join(c.TempDir(), "environment-pair.db")

	out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

	c.Assert(err, qt.ErrorMatches,
		`if any flags in the group \[limit to-version\] are set none of the others can be; `+
			`\[PTAH_LIMIT PTAH_TO_VERSION\] were all set`,
		qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateUpToVersionFromTheEnvironment_HappyPath is the control for the
// refusal above, in both directions: one variable on its own still bounds a
// run, and a typed flag beside the other variable is a run the operator can
// have.
func TestMigrateUpToVersionFromTheEnvironment_HappyPath(t *testing.T) {
	t.Run("the variable alone bounds the run", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv("PTAH_TO_VERSION", "2")
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "environment-bound.db")

		out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
	})

	t.Run("a typed bound withdraws the batch limit the environment carries", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv("PTAH_LIMIT", "1")
		migrationsDir := writeThreeUpMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "typed-bound.db")

		out, err := runUpThroughRoot(
			"--db-url", "sqlite://"+dbPath,
			"--migrations-dir", migrationsDir,
			"--to-version", "2",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
	})
}
