//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestAtlasSchemaPlanTxModeDirectiveE2E is `schema plan --directive` doing
// work rather than being recorded (stokaro/ptah#1700).
//
// PostgreSQL is the one target that can tell the difference from the outside:
// it refuses `CREATE INDEX CONCURRENTLY` inside a transaction block, so a plan
// whose statements are wrapped cannot create the index and a plan run without
// a transaction can. That makes the CATALOG the observable, which is what this
// test asserts -- an exit code would pass on an apply that wrapped the
// statement and swallowed the failure.
//
// The rows separate the directive from everything else that could produce the
// same outcome:
//
//   - the directive row is the finding: a plan carrying `-- atlas:txmode none`
//     applies with no --tx-mode on the command line, and the index exists;
//   - the no-directive row is the control that fails if the concurrent index
//     had been creatable under the default transaction mode all along, which
//     would make the first row pass for the wrong reason;
//   - the --tx-mode none row is the control that fails if the plan had become
//     unappliable for some reason unrelated to the transaction mode: the same
//     plan, without the directive, applies when the operator supplies the mode
//     the directive would have.
func TestAtlasSchemaPlanTxModeDirectiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	tests := []struct {
		name           string
		directive      []string
		applyTxMode    []string
		wantApplyError bool
	}{
		{
			name:      "the plan's directive supplies the transaction mode",
			directive: []string{"-d", "atlas:txmode none"},
		},
		{
			name:           "without it the concurrent index cannot be applied",
			wantApplyError: true,
		},
		{
			name:        "the operator can still supply the same mode by flag",
			applyTxMode: []string{"--tx-mode", "none"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := t.TempDir()
			const table = "plan_directive"
			const index = "plan_directive_email_idx"

			// A throwaway database per row, not a table in the shared one:
			// `schema apply --plan` verifies the end state against --to, and a
			// desired file naming one table would drop every other table the
			// shared database holds before this test could read its own
			// answer.
			rowURL := newPlanDirectiveDatabase(c, ctx, dbURL)
			db := openPlanDirectiveDB(c, rowURL)

			desiredPath := filepath.Join(work, "desired.sql")
			c.Assert(os.WriteFile(desiredPath, []byte(
				"CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, email TEXT);\n"+
					"CREATE INDEX "+index+" ON "+table+" (email);\n"), 0o600), qt.IsNil)
			// The seed leaves the index as the only difference, so the plan is
			// exactly one CONCURRENTLY statement.
			runPlanDirectiveSQL(c, ctx, db,
				"CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, email TEXT)")
			// diff.concurrent_index.create is what makes the planner emit
			// CONCURRENTLY at all; without it the plan would be an ordinary
			// CREATE INDEX and no transaction mode would matter.
			c.Assert(os.WriteFile(filepath.Join(work, "atlas.hcl"), []byte(`env "local" {
  diff {
    concurrent_index {
      create = true
    }
  }
}
`), 0o600), qt.IsNil)

			// The native JSON plan, so the run needs no dev database: an
			// Atlas-format plan is verified by replaying it on one, which is a
			// second server this test would have to stand up to answer a
			// question it is not asking. The directive rides in the migration
			// text in either encoding, and
			// TestSchemaPlanDirectiveRoundTripsThroughThePlanReader covers
			// both.
			planPath := filepath.Join(work, "concurrent.plan.json")
			planArgs := append([]string{
				"schema", "plan",
				"--env", "local",
				"--from", rowURL,
				"--to", "file://" + desiredPath,
				"--output", planPath,
			}, test.directive...)
			planCmd := exec.CommandContext(ctx, binaryPath, planArgs...)
			planCmd.Dir = work
			planOut, planErr := planCmd.CombinedOutput()
			c.Assert(planErr, qt.IsNil, qt.Commentf("plan:\n%s", planOut))
			saved, readErr := os.ReadFile(planPath) // #nosec G304 -- test-controlled path
			c.Assert(readErr, qt.IsNil)
			// The premise of every row: the plan really does carry the
			// statement PostgreSQL refuses to wrap.
			c.Assert(string(saved), qt.Contains, "CONCURRENTLY")

			applyArgs := append([]string{
				"schema", "apply",
				"--env", "local",
				"--url", rowURL,
				"--to", "file://" + desiredPath,
				"--plan", "file://" + planPath,
				"--auto-approve",
			}, test.applyTxMode...)
			applyCmd := exec.CommandContext(ctx, binaryPath, applyArgs...)
			applyCmd.Dir = work
			applyOut, applyErr := applyCmd.CombinedOutput()

			c.Assert(applyErr != nil, qt.Equals, test.wantApplyError,
				qt.Commentf("apply:\n%s", applyOut))
			// The catalog, not the exit code: an apply that wrapped the
			// statement and reported success would still leave no index.
			c.Assert(
				planDirectiveE2EIndexExists(c, ctx, db, index),
				qt.Equals, !test.wantApplyError,
				qt.Commentf("apply:\n%s", applyOut))
		})
	}
}

// newPlanDirectiveDatabase creates a throwaway database for one row and
// returns its URL.
func newPlanDirectiveDatabase(c *qt.C, ctx context.Context, adminURL string) string {
	c.Helper()
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_plan_directive_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })
	return replaceDatabaseName(c, adminURL, name)
}

// openPlanDirectiveDB opens the live target for setup and for the catalog
// read, and closes it with the test.
func openPlanDirectiveDB(c *qt.C, dbURL string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })
	return db
}

// runPlanDirectiveSQL executes one setup statement against the live target.
func runPlanDirectiveSQL(c *qt.C, ctx context.Context, db *sql.DB, statement string) {
	c.Helper()
	_, err := db.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// planDirectiveE2EIndexExists asks the catalog whether the index is there.
//
// pg_indexes rather than the apply's own output, because the question is what
// the DATABASE holds: an apply that wrapped the CONCURRENTLY statement, failed
// inside its transaction and reported anything at all would still leave the
// index absent, and that is the difference this test exists to see.
func planDirectiveE2EIndexExists(c *qt.C, ctx context.Context, db *sql.DB, index string) bool {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_indexes WHERE indexname = $1", index).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}
