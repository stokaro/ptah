//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

var (
	lintE2EOkDurationRe    = regexp.MustCompile(`-- ok \([^)]+\)`)
	lintE2ETotalDurationRe = regexp.MustCompile(`(?m)^(  -------------------------\n  -- ).+$`)
)

// redactLintE2EDurations replaces the non-deterministic elapsed durations in the
// default migrate lint text report with "DUR" so the rest is asserted exactly.
func redactLintE2EDurations(s string) string {
	s = lintE2EOkDurationRe.ReplaceAllString(s, "-- ok (DUR)")
	return lintE2ETotalDurationRe.ReplaceAllString(s, "${1}DUR")
}

func writeLintE2EFile(c *qt.C, dir, name, body string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
}

// runLintE2EBinary runs the built binary with stdout and stderr kept apart, so
// a report assertion cannot be satisfied by text that was actually a diagnostic
// on the error stream.
func runLintE2EBinary(ctx context.Context, binaryPath string, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// TestAtlasMigrateLintMultiTargetDropE2E covers issue #1074 part 2 against a
// live PostgreSQL dev database, which is the only place the behavior is
// reachable end to end: SQLite rejects `DROP TABLE a, b` outright, so the
// replay step fails before any analyzer runs and the unit-level compat tests
// cannot carry this fixture.
//
// The tables live in the schema the dev URL actually covers. Objects in a
// schema outside it are a separate axis entirely -- both tools change behavior
// there for reasons that have nothing to do with how many targets a statement
// names -- so a fixture built that way would measure schema scope and report it
// as a multi-target result.
//
// The single-target case is the control. Both statements are destructive, both
// exit 1, and both report; only the number of tables named moves. Without it,
// a fixture that reported nothing at all would look like a pass for the wrong
// reason.
//
// Three tables rather than two, deliberately: created mid/zeta/alpha and
// dropped zeta/alpha/mid, so source order, creation order and reverse creation
// order each give a different answer, and only one of them is the order below.
func TestAtlasMigrateLintMultiTargetDropE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

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
		name string
		drop string
		want string
	}{
		{
			name: "single target",
			drop: "DROP TABLE zeta;\n",
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"zeta\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"zeta\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name: "three targets",
			drop: "DROP TABLE zeta, alpha, mid;\n",
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"alpha\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"mid\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"zeta\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"alpha\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"mid\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"zeta\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 3 schema changes\n" +
				"  -- 3 diagnostics\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			testDBName := fmt.Sprintf("ptah_lint_mt_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			migrationsDir := c.TempDir()
			writeLintE2EFile(c, migrationsDir, "1.sql",
				"CREATE TABLE mid (id int);\nCREATE TABLE zeta (id int);\nCREATE TABLE alpha (id int);\n")
			writeLintE2EFile(c, migrationsDir, "2.sql", test.drop)

			stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
				"migrate", "lint",
				"--dir", "file://"+migrationsDir,
				"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
				"--latest", "1",
			)

			c.Assert(err, qt.ErrorMatches, "exit status 1")
			c.Assert(stderr, qt.Equals, "")
			c.Assert(redactLintE2EDurations(stdout), qt.Equals, test.want)
		})
	}
}
