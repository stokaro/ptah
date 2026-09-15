//go:build integration

package integration_test

// What `ptah schema apply` does with a plan PostgreSQL refuses inside a
// transaction.
//
// The default `--tx-mode file` runs the whole plan in one transaction. Two
// shapes cannot run there: a concurrent index build, and a value added to an
// existing enum type and then used. The migrator refuses both in a
// transactional file before it sends anything, through internal/txrequire, and
// schema apply asks the same rule. With a preflight that searches the SQL for
// `CREATE INDEX CONCURRENTLY` instead, the enum plan reaches the server and
// fails with its SQLSTATE (stokaro/ptah#3283):
//
//	error: apply schema changes: failed to execute SQL statement: SQL execution failed:
//	ERROR: unsafe use of new value "archived" of enum type probe_status (SQLSTATE 55P04)
//
// The transaction rolls back either way, so the catalog cannot tell the refusal
// from the server's failure. The discriminating observable is the diagnostic:
// Ptah's sentence names `--tx-mode none` and carries no SQLSTATE, and a
// SQLSTATE proves the statement was sent.
//
// The happy paths are the controls. `--tx-mode none` is the way through the
// refusal names, so it has to run the same plan to completion. A plan that
// creates an enum type, adds a value and uses it is valid in one transaction,
// so a preflight that refused every `ADD VALUE` would fail that row while
// passing every failure row.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for the setup and catalog reads below

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// txPreflightEnumSetup is the target the enum rows start from: an enum type and
// a table using it.
var txPreflightEnumSetup = []string{
	"CREATE TYPE probe_status AS ENUM ('draft', 'sent')",
	"CREATE TABLE messages (id integer NOT NULL, state probe_status NOT NULL DEFAULT 'draft')",
}

// txPreflightEnumDesired adds a value to that type and a column defaulting to
// it. The plan is `ALTER TYPE ... ADD VALUE 'archived'` followed by an
// `ALTER TABLE ... ADD COLUMN` that uses the value.
const txPreflightEnumDesired = `CREATE TYPE probe_status AS ENUM ('draft', 'sent', 'archived');
CREATE TABLE messages (
  id integer NOT NULL,
  state probe_status NOT NULL DEFAULT 'draft',
  archive_state probe_status NOT NULL DEFAULT 'archived'
);
`

// txPreflightArchivedLabel counts the enum label the enum plan adds.
const txPreflightArchivedLabel = `SELECT count(*) FROM pg_enum e
JOIN pg_type t ON t.oid = e.enumtypid
WHERE t.typname = $1 AND e.enumlabel = 'archived'`

// TestSchemaApplyTransactionPreflightE2E_FailurePath is the defect and its
// sibling.
//
// The concurrent-index row holds under a text search too. It is here because
// the same preflight answers for both shapes, so it pins that the command
// refuses both in this position with this wording.
func TestSchemaApplyTransactionPreflightE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		setup       []string
		desired     string
		config      string
		wantNamed   string
		witness     string
		witnessName string
	}{
		{
			name:        "an enum value added and used in one plan",
			setup:       txPreflightEnumSetup,
			desired:     txPreflightEnumDesired,
			config:      "{}\n",
			wantNamed:   "it uses 'archived'",
			witness:     txPreflightArchivedLabel,
			witnessName: "probe_status",
		},
		{
			name:        "a concurrent index build in one plan",
			setup:       []string{"CREATE TABLE widgets (id integer NOT NULL, a integer)"},
			desired:     "CREATE TABLE widgets (id integer NOT NULL, a integer);\nCREATE INDEX idx_widgets_a ON widgets (a);\n",
			config:      "diff:\n  concurrent_index: true\n",
			wantNamed:   "CREATE or DROP INDEX CONCURRENTLY",
			witness:     "SELECT count(*) FROM pg_indexes WHERE indexname = $1",
			witnessName: "idx_widgets_a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newTxPreflightTarget(c, ctx, test.setup)
			work := writeTxPreflightWorkdir(c, test.desired, test.config)

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
				"schema", "apply",
				"--db-url", target.url,
				"--schema-file", filepath.Join(work, "desired.sql"),
				"--auto-approve")

			comment := txPreflightCommentf(applied)
			c.Assert(applied.ExitCode, qt.Equals, 2, comment)
			c.Assert(applied.Stderr, qt.Contains, "the planned changes cannot run inside a transaction", comment)
			c.Assert(applied.Stderr, qt.Contains, test.wantNamed, comment)
			c.Assert(applied.Stderr, qt.Contains, "rerun with --tx-mode none", comment)
			// The server was never asked. Its refusals carry a SQLSTATE.
			c.Assert(applied.Stderr, qt.Not(qt.Contains), "SQLSTATE", comment)
			c.Assert(txPreflightCount(c, ctx, target, test.witness, test.witnessName), qt.Equals, 0, comment)
		})
	}
}

// TestSchemaApplyTransactionPreflightE2E_HappyPath holds the refusal to the
// engine's rule.
func TestSchemaApplyTransactionPreflightE2E_HappyPath(t *testing.T) {
	t.Run("tx-mode none runs the enum plan the refusal names", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		target := newTxPreflightTarget(c, ctx, txPreflightEnumSetup)
		work := writeTxPreflightWorkdir(c, txPreflightEnumDesired, "{}\n")

		applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
			"schema", "apply",
			"--db-url", target.url,
			"--schema-file", filepath.Join(work, "desired.sql"),
			"--tx-mode", "none",
			"--auto-approve")

		comment := txPreflightCommentf(applied)
		c.Assert(applied.ExitCode, qt.Equals, 0, comment)
		c.Assert(applied.Stdout, qt.Contains, "Schema apply completed successfully.", comment)
		c.Assert(txPreflightCount(c, ctx, target, txPreflightArchivedLabel, "probe_status"), qt.Equals, 1, comment)
	})

	// A computed plan never adds a value to a type it creates: CREATE TYPE
	// carries every value. A saved plan is SQL an operator can edit, so the row
	// saves a real plan against the empty target, keeping its source
	// fingerprint, and replaces its statements.
	t.Run("a value added to a type created in the same plan runs in one transaction", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		target := newTxPreflightTarget(c, ctx, nil)
		work := writeTxPreflightWorkdir(c,
			"CREATE TYPE fresh_status AS ENUM ('draft');\n"+
				"CREATE TABLE fresh_messages (id integer NOT NULL, state fresh_status NOT NULL DEFAULT 'draft');\n",
			"{}\n")
		planPath := filepath.Join(work, "fresh.plan.json")

		planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
			"schema", "plan",
			"--db-url", target.url,
			"--schema-file", filepath.Join(work, "desired.sql"),
			"--output", planPath)
		c.Assert(planned.ExitCode, qt.Equals, 0, txPreflightCommentf(planned))

		replaceTxPreflightPlanStatements(c, planPath,
			"CREATE TYPE fresh_status AS ENUM ('draft')",
			"ALTER TYPE fresh_status ADD VALUE 'archived'",
			"CREATE TABLE fresh_messages (id integer NOT NULL, state fresh_status NOT NULL DEFAULT 'archived')")

		applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
			"schema", "apply",
			"--db-url", target.url,
			"--plan", planPath,
			"--auto-approve")

		comment := txPreflightCommentf(applied)
		c.Assert(applied.ExitCode, qt.Equals, 0, comment)
		c.Assert(applied.Stdout, qt.Contains, "ALTER TYPE fresh_status ADD VALUE 'archived'", comment)
		c.Assert(applied.Stdout, qt.Contains, "Schema apply completed successfully.", comment)
		c.Assert(txPreflightCount(c, ctx, target, txPreflightArchivedLabel, "fresh_status"), qt.Equals, 1, comment)
	})
}

// txPreflightTarget is one throwaway PostgreSQL database.
type txPreflightTarget struct {
	db  *sql.DB
	url string
}

// newTxPreflightTarget creates a database of its own for one row, runs the
// setup in it, and drops it afterwards.
//
// A database of its own rather than a schema: `schema apply` reconciles the
// whole target, so a shared database would put every other test's objects into
// the plan under measurement.
func newTxPreflightTarget(c *qt.C, ctx context.Context, setup []string) txPreflightTarget {
	c.Helper()

	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	database := fmt.Sprintf("ptah_txpre3283_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, database) })

	targetURL := replaceDatabaseName(c, adminURL, database)
	rowDB, err := sql.Open("pgx", targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rowDB.Close() })

	for _, statement := range setup {
		_, execErr := rowDB.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	return txPreflightTarget{db: rowDB, url: targetURL}
}

// writeTxPreflightWorkdir writes the desired schema and a ptah.yaml into a
// directory of the row's own, so no project config beside the test decides
// anything. A row with no setting passes `{}`: an empty ptah.yaml is refused
// with `failed to parse ptah config ptah.yaml: EOF`.
func writeTxPreflightWorkdir(c *qt.C, desired, config string) string {
	c.Helper()

	work := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(work, "desired.sql"), []byte(desired), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(work, "ptah.yaml"), []byte(config), 0o600), qt.IsNil)
	return work
}

// replaceTxPreflightPlanStatements rewrites a saved plan's statement list and
// keeps everything else, the source fingerprint included.
func replaceTxPreflightPlanStatements(c *qt.C, planPath string, statements ...string) {
	c.Helper()

	contents, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	var document map[string]any
	c.Assert(json.Unmarshal(contents, &document), qt.IsNil)

	replaced := make([]map[string]string, 0, len(statements))
	for _, statement := range statements {
		replaced = append(replaced, map[string]string{
			"sql":      statement,
			"severity": "safe",
			"reason":   "does not remove data or tighten constraints",
		})
	}
	document["statements"] = replaced

	rewritten, err := json.MarshalIndent(document, "", "  ")
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, rewritten, 0o600), qt.IsNil)
}

// txPreflightCount asks the catalog rather than Ptah, so one misreading cannot
// satisfy both the apply and the check of what it did.
func txPreflightCount(c *qt.C, ctx context.Context, target txPreflightTarget, query, name string) int {
	c.Helper()

	var count int
	c.Assert(target.db.QueryRowContext(ctx, query, name).Scan(&count), qt.IsNil)
	return count
}

// txPreflightCommentf attaches both streams to a failure: the plan lands on
// stdout and the diagnostic on stderr.
func txPreflightCommentf(result clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", result.ExitCode, result.Stdout, result.Stderr)
}
