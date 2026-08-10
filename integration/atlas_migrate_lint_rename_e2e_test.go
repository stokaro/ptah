//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// exitStatusOf reports the process exit code a run finished with. The exit code
// is one of the five things #1074 asks to align, so it is read as a number
// rather than matched as the text of an error -- a nil error and an error that
// is not an exit status are different outcomes, and only one of them means the
// process exited 0.
func exitStatusOf(c *qt.C, err error) int {
	c.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	c.Assert(err, qt.ErrorAs, &exitErr, qt.Commentf("run failed without an exit status: %v", err))
	return exitErr.ExitCode()
}

// TestAtlasMigrateLintRenameE2E covers issue #1074 part 1 against a live
// PostgreSQL dev database, which is where the divergence was measured: a rename
// retires a logical name, and `ptah-compat migrate lint` has to report that as
// a destructive change to the retired name and exit 1, where it used to print a
// BC101 warning and exit 0.
//
// Each want below is the pinned community binary's output on the same fixture,
// byte for byte apart from the elapsed durations.
//
// The tables live in the schema the dev URL covers. Objects in a schema outside
// it are a separate axis -- the community binary reports nothing there at all --
// so a fixture built that way would measure schema scope and report it as a
// classification result.
//
// The two rename forms are separate cases because they differ in more than
// wording: the column form is DS103 with a NULL precheck and one schema change,
// the table form is DS102 with an emptiness precheck and two. The third case is
// the control: renaming an object the same file created reports nothing on
// either tool, so a change that classified renames destructively regardless of
// provenance would fail there while the first two still passed.
func TestAtlasMigrateLintRenameE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	tests := []struct {
		name     string
		base     string
		rename   string
		exitCode int
		want     string
	}{
		{
			name:     "column rename",
			base:     "CREATE TABLE users (id int);\n",
			rename:   "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:     "table rename",
			base:     "CREATE TABLE users (id int);\n",
			rename:   "ALTER TABLE users RENAME TO accounts;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:     "control: rename of an object created in the same file",
			base:     "CREATE TABLE seed (x int);\n",
			rename:   "CREATE TABLE users (id int);\nALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 0,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version ok\n" +
				"  -- 2 schema changes\n",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			testDBName := fmt.Sprintf("ptah_lint_rename_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			migrationsDir := c.TempDir()
			writeLintE2EFile(c, migrationsDir, "1.sql", test.base)
			writeLintE2EFile(c, migrationsDir, "2.sql", test.rename)

			stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
				"migrate", "lint",
				"--dir", "file://"+migrationsDir,
				"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
				"--latest", "1",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, test.exitCode)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(redactLintE2EDurations(stdout), qt.Equals, test.want)
		})
	}
}
