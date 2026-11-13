//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// replayFailureProject is the project file of the community binary's own
// SQLite fixture cli-migrate-lint-project.txtar: one env prints the name of
// every reported file, the other counts them.
const replayFailureProject = `lint {
    latest = 1
    destructive {
        error = false
    }
}

env "log_name" {
    lint {
        log = "{{ range .Files }}{{ println .Name }}{{ end }}"
    }
}

env "log_count" {
    lint {
        latest = 2
        log = "{{ len .Files | println }}"
    }
}
`

// writeRowidAliasProject writes the migrations of that fixture. Its base
// renames a column to oid, which the SQLite Ptah bundles refuses -- since
// SQLite 3.54 no column may be renamed to a rowid alias -- and the SQLite the
// community binary v1.3.0 bundles, 3.49.1, accepts.
func writeRowidAliasProject(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	writeLintE2EFile(c, migrations, "1.sql",
		"CREATE TABLE users (id int);\n\nCREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN id TO oid;\n")
	writeLintE2EFile(c, migrations, "2.sql", "DROP TABLE users;\n")
	writeLintE2EFile(c, migrations, "3.sql", "DROP TABLE pets;\n")
	writeLintE2EFile(c, root, "atlas.hcl", replayFailureProject)
	return root
}

// runCompatLintIn runs the built binary in dir, with stdout and stderr kept
// apart.
func runCompatLintIn(ctx context.Context, dir, binaryPath string, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = dir
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// TestAtlasMigrateLintReplayFailureE2E drives `ptah-compat migrate lint` on
// SQLite, where the engine Ptah bundles is what refuses the statement. A
// refused statement is a replay error: the run exits 1 and reports the file
// the replay stopped at, in the shape the community binary reports a replay
// error, and never reports the directory as clean or analyzes the files the
// replay never reached.
//
// The community binary reports a replay error as report content: the header,
// `Error: executing statement: ...` and one version with errors on stdout,
// nothing on stderr, and a `.Files` of exactly the failing file. Measured on
// v1.3.0 with a statement its own engine refuses too (a rename of a column the
// table lacks), the output below differs from that binary's only in the
// driver's message, where its driver prints `no such column: "nope"` and
// Ptah's prints `SQL logic error: no such column: "nope" (1)`.
func TestAtlasMigrateLintReplayFailureE2E(t *testing.T) {
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "false")
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "false")
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)

	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)

	t.Run("a base the bundled engine refuses is reported as a replay error", func(t *testing.T) {
		c := qt.New(t)
		root := writeRowidAliasProject(c)

		stdout, stderr, err := runCompatLintIn(ctx, root, binaryPath,
			"migrate", "lint",
			"--dir", "file://migrations",
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
			"--latest", "1",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(redactLintE2EDurations(stdout), qt.Equals,
			"Analyzing changes from version 2 to 3 (1 migration in total):\n"+
				"\n"+
				"  Error: executing statement: SQL logic error: cannot rename to rowid alias: oid (1)\n"+
				"\n"+
				"  -------------------------\n"+
				"  -- DUR\n"+
				"  -- 1 version with errors\n")
	})

	// The project fixture's envs print `.Files`. On the community binary,
	// whose engine accepts the rename, they print 3.sql and 2 and exit 0.
	// Here the replay stops in the base, so the failing file is the only one
	// reported, and the run exits 1.
	projectRuns := []struct {
		name string
		env  string
		want string
	}{
		{name: "the file names list only the failing file", env: "log_name", want: "1.sql\n"},
		{name: "the file count counts only the failing file", env: "log_count", want: "1\n"},
	}
	for _, run := range projectRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeRowidAliasProject(c)

			stdout, stderr, err := runCompatLintIn(ctx, root, binaryPath,
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
				"--env", run.env,
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(stdout, qt.Equals, run.want)
		})
	}

	t.Run("a statement of an analyzed file is quoted in the file entry", func(t *testing.T) {
		c := qt.New(t)
		migrations := c.TempDir()
		writeLintE2EFile(c, migrations, "1.sql", "CREATE TABLE users (id int);\n")
		writeLintE2EFile(c, migrations, "2.sql",
			"CREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN nope TO x;\n")

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "lint",
			"--dir", "file://"+migrations,
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
			"--latest", "1",
			"--format", "{{ json .Files }}",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Equals, `[{"Name":"2.sql","Error":"executing statement: `+
			`ALTER TABLE users RENAME COLUMN nope TO x;: SQL logic error: no such column: \"nope\" (1)"}]`)
	})
}
