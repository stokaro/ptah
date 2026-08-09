package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/dbschema"
)

// TestMigrateGenerateWritesTheUndeclaredPartitionDropUnlessAskedToCheck pins
// which verb refuses the DROP TABLE that a desired state naming a partitioned
// parent, but not its partitions, plans for those partitions.
//
// Two separate things are easy to conflate here, and #997's record conflated
// them once already:
//
//   - `migrations generate` writes the statement. Its destructive gate is
//     opt-in: --check-destructive turns it on, and --allow-destructive only
//     means anything once it is on. With neither flag the command exits
//     successfully and the file, its checksum and its commit exist.
//   - The refusal that quotes DS101 belongs to `migrations up`, which reads
//     the written directory and declines to apply it. That gate is pinned by
//     TestMigrateUp_LintConfigWarningStillBlocksPostgresDropTable in
//     cmd/migrateup, which also asserts the table survives the refusal.
//
// The fixture is a real partitioned parent rather than two ordinary tables,
// because the neighboring over-reach this PR must not commit is to start
// suppressing the partition the way it now suppresses the partition's copy of
// a parent index. A partition is a table holding rows of its own; dropping it
// is destructive and has to stay visible as such.
func TestMigrateGenerateWritesTheUndeclaredPartitionDropUnlessAskedToCheck(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()

	dbURL, conn := requireMigrateGeneratePostgresTestConnection(t, ctx)
	defer dbschema.CloseAndWarn(conn)
	releaseLock := acquireMigrateGenerateTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	}()

	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	_, err := conn.ExecContext(ctx, `
		CREATE TABLE events (
			id BIGINT NOT NULL,
			tenant TEXT NOT NULL,
			created_at DATE NOT NULL
		) PARTITION BY RANGE (created_at)
	`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE events_2026 PARTITION OF events
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')
	`)
	c.Assert(err, qt.IsNil)

	entitiesDir := writeMigrateGenerateParentOnlyEntities(c, c.TempDir())

	c.Run("--check-destructive refuses before anything is written", func(c *qt.C) {
		migrationsDir := filepath.Join(c.TempDir(), "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

		err := runMigrateGenerate(c, []string{
			"--root-dir", entitiesDir,
			"--db-url", dbURL,
			"--migrations-dir", migrationsDir,
			"--name", "checked",
			"--check-destructive",
		})

		c.Assert(err, qt.ErrorMatches, "destructive migration statements require AllowDestructive")
		c.Assert(migrateGenerateSQLFiles(c, migrationsDir), qt.HasLen, 0)
	})

	c.Run("the default writes the DROP TABLE and reports success", func(c *qt.C) {
		migrationsDir := filepath.Join(c.TempDir(), "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

		err := runMigrateGenerate(c, []string{
			"--root-dir", entitiesDir,
			"--db-url", dbURL,
			"--migrations-dir", migrationsDir,
			"--name", "unchecked",
		})

		c.Assert(err, qt.IsNil)
		upSQL := migrateGenerateUpSQL(c, migrationsDir)
		c.Assert(upSQL, qt.Contains, `DROP TABLE IF EXISTS "events_2026" CASCADE;`)
		// The declared half of the plan is here too, so the row above reports
		// the destructive gate rather than a plan that came out empty.
		c.Assert(upSQL, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_events_tenant" ON "events" ("tenant");`)
	})

	c.Run("--check-destructive with --allow-destructive writes it too", func(c *qt.C) {
		migrationsDir := filepath.Join(c.TempDir(), "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

		err := runMigrateGenerate(c, []string{
			"--root-dir", entitiesDir,
			"--db-url", dbURL,
			"--migrations-dir", migrationsDir,
			"--name", "allowed",
			"--check-destructive",
			"--allow-destructive",
		})

		c.Assert(err, qt.IsNil)
		c.Assert(
			migrateGenerateUpSQL(c, migrationsDir),
			qt.Contains,
			`DROP TABLE IF EXISTS "events_2026" CASCADE;`,
		)
	})
}

// runMigrateGenerate executes one `migrations generate` invocation and returns
// its error, with stdout and stderr captured so a failure reports what the
// command said.
func runMigrateGenerate(c *qt.C, args []string) error {
	c.Helper()
	var stdout, stderr bytes.Buffer
	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	c.Logf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	return err
}

// migrateGenerateSQLFiles lists the .sql files a generate invocation left in a
// directory, so "nothing was written" is measured against the directory.
func migrateGenerateSQLFiles(c *qt.C, dir string) []string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	return matches
}

// migrateGenerateUpSQL reads the one generated up file in a directory. It
// asserts there is exactly one, so a row that reads the wrong migration says so
// rather than matching against whichever file the glob happened to order first.
func migrateGenerateUpSQL(c *qt.C, dir string) string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	content, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	return string(content)
}

func writeMigrateGenerateParentOnlyEntities(c *qt.C, dir string) string {
	c.Helper()
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o755), qt.IsNil)
	content := `package entities

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="tenant" type="TEXT" not_null="true"
	//ptah:schema:index name="idx_events_tenant" fields="tenant"
	Tenant string

	//ptah:schema:field name="created_at" type="DATE" not_null="true"
	CreatedAt string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0o600), qt.IsNil)
	return entitiesDir
}
