package atlasmigratereport_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/atlasmigratereport"
	"ptah.run/migration/migrator"
)

// applyTextRun runs files against a fresh SQLite database as `migrate apply`
// does and renders the default report of the run. The run's clock is replaced
// by one second, so the report's durations read `1s`. The run's own error is
// returned beside the report, because a failed run is reported too.
func applyTextRun(c *qt.C, files map[string]string, configure func(*atlasmigrate.ApplyOptions)) (string, error) {
	c.Helper()
	ctx := context.Background()
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "text.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	opts := atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	}
	configure(&opts)
	plan, err := atlasmigrate.PrepareApply(ctx, conn, opts)
	c.Assert(err, qt.IsNil)
	result, runErr := plan.Execute(ctx)
	result.StartedAt = time.Unix(100, 0).UTC()
	result.EndedAt = time.Unix(101, 0).UTC()
	var out bytes.Buffer

	err = atlasmigratereport.WriteApplyText(&out, atlasmigratereport.ApplyFormatOptions{
		Conn:   conn,
		FS:     os.DirFS(migrationsDir),
		Result: result,
	})

	c.Assert(err, qt.IsNil)
	return out.String(), runErr
}

func runAsIs(*atlasmigrate.ApplyOptions) {}

// The directory the pinned community binary v1.3.0 was measured on for a
// failure in the middle of a file. Its report is the same apart from the
// durations and the driver's wording, where its driver prints `no such table:
// nosuch`: the statements before the failure, the refused one last, the
// database's answer, and no statement after it.
func TestWriteApplyText_ReportsTheStatementTheDatabaseRefused(t *testing.T) {
	c := qt.New(t)

	report, runErr := applyTextRun(c, map[string]string{
		"1.sql": "CREATE TABLE users (id int);\n",
		"2.sql": "ALTER TABLE users ADD COLUMN name text;\nINSERT INTO nosuch VALUES (1);\nCREATE TABLE after (id int);\n",
		"3.sql": "CREATE TABLE t3 (id int);\n",
	}, runAsIs)

	c.Assert(runErr, qt.IsNotNil)
	c.Assert(report, qt.Equals, "Migrating to version 3 (3 migrations in total):\n"+
		"\n"+
		"  -- migrating version 1\n"+
		"    -> CREATE TABLE users (id int);\n"+
		"  -- ok (1s)\n"+
		"\n"+
		"  -- migrating version 2\n"+
		"    -> ALTER TABLE users ADD COLUMN name text;\n"+
		"    -> INSERT INTO nosuch VALUES (1);\n"+
		"    SQL logic error: no such table: nosuch (1)\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 1 migration ok, 1 with errors\n"+
		"  -- 2 sql statements ok, 1 with errors\n")
}

// A refusal at the first statement lists that statement alone, and counts no
// statement as passed. Measured on the community binary with the same
// directory, which it reports the same way apart from durations and wording.
func TestWriteApplyText_ReportsARefusalAtTheFirstStatement(t *testing.T) {
	c := qt.New(t)

	report, runErr := applyTextRun(c, map[string]string{
		"1.sql": "INSERT INTO nosuch VALUES (1);\nCREATE TABLE users (id int);\n",
		"2.sql": "CREATE TABLE t2 (id int);\n",
	}, runAsIs)

	c.Assert(runErr, qt.IsNotNil)
	c.Assert(report, qt.Equals, "Migrating to version 2 (2 migrations in total):\n"+
		"\n"+
		"  -- migrating version 1\n"+
		"    -> INSERT INTO nosuch VALUES (1);\n"+
		"    SQL logic error: no such table: nosuch (1)\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 1 migration with errors\n"+
		"  -- 1 sql statement with errors\n")
}

// checkedMigrations is a directory whose second file carries a pre-migration
// check that passes.
var checkedMigrations = map[string]string{
	"1.sql": "CREATE TABLE users (id int);\n",
	"2.sql": "-- +ptah check assert=\"SELECT COUNT(*) = 0 FROM users\"\nCREATE TABLE posts (id int);\n",
}

// A dry run lists every statement it would run, as the community binary's
// does, and leaves out the check it deferred: listing it would report as
// passed an assertion nothing evaluated.
func TestWriteApplyText_DryRunListsTheStatementsAndNoDeferredCheck(t *testing.T) {
	c := qt.New(t)

	report, runErr := applyTextRun(c, checkedMigrations, func(opts *atlasmigrate.ApplyOptions) {
		opts.DryRun = true
	})

	c.Assert(runErr, qt.IsNil)
	c.Assert(report, qt.Equals, "Migrating to version 2 (2 migrations in total):\n"+
		"\n"+
		"  -- migrating version 1\n"+
		"    -> CREATE TABLE users (id int);\n"+
		"  -- ok (1s)\n"+
		"\n"+
		"  -- migrating version 2\n"+
		"    -> CREATE TABLE posts (id int);\n"+
		"  -- ok (1s)\n"+
		"\n"+
		"  -------------------------\n"+
		"  -- 1s\n"+
		"  -- 2 migrations\n"+
		"  -- 2 sql statements\n")
}

// A run told to skip checks leaves them out of the report for the same reason.
// The control beside it is the same directory run with its checks, which lists
// the check, so the leaving out is the skip and not a check the report never
// saw.
func TestWriteApplyText_ListsChecksOnlyWhenTheyRan(t *testing.T) {
	tests := []struct {
		name       string
		skipChecks bool
		want       string
	}{
		{
			name: "checks run",
			want: "Migrating to version 2 (2 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -- pre-migration checks of version 2\n" +
				"    -> SELECT COUNT(*) = 0 FROM users\n" +
				"  -- ok\n" +
				"  -- migrating version 2\n" +
				"    -> CREATE TABLE posts (id int);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 2 migrations\n" +
				"  -- 1 check\n" +
				"  -- 2 sql statements\n",
		},
		{
			name:       "checks skipped",
			skipChecks: true,
			want: "Migrating to version 2 (2 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> CREATE TABLE posts (id int);\n" +
				"  -- ok (1s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1s\n" +
				"  -- 2 migrations\n" +
				"  -- 2 sql statements\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, runErr := applyTextRun(c, checkedMigrations, func(opts *atlasmigrate.ApplyOptions) {
				opts.SkipChecks = test.skipChecks
			})

			c.Assert(runErr, qt.IsNil)
			c.Assert(report, qt.Equals, test.want)
		})
	}
}
