//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestSchemaTestDatabaseDesiredSourcePostgresE2E covers `ptah schema test` with
// a live PostgreSQL database as the desired-state source.
//
// PostgreSQL is required, not incidental. SQLite introspects no roles and no
// grants, so it can never produce the cluster-scoped state the command has to
// drop, and the note reporting that drop is unreachable without a server. This
// is the only place the note is observed end to end.
//
// The asserted table exists in the source database and in no other input, so a
// pass can only come from having introspected it.
func TestSchemaTestDatabaseDesiredSourcePostgresE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []struct {
		name   string
		args   []string
		verify func(c *qt.C, stdout, stderr string)
	}{
		{
			name: "text report",
			args: []string{"--report", "text"},
			verify: func(c *qt.C, stdout, stderr string) {
				c.Assert(stdout, qt.Contains, `PASS  case "db-sourced table exists"`)
				c.Assert(stdout, qt.Contains, "1 cases, 1 passed, 0 failed")
				assertDesiredStateNoteOnStderrOnly(c.TB, stdout, stderr)
			},
		},
		{
			// The whole point of the row: a machine-readable report must be
			// parseable on its own. A note written to the report stream leaves
			// a passing run unparseable while still exiting 0.
			name: "json report parses on stdout alone",
			args: []string{"--report", "json"},
			verify: func(c *qt.C, stdout, stderr string) {
				var report struct {
					Total  int `json:"total"`
					Passed int `json:"passed"`
					Failed int `json:"failed"`
				}
				c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil,
					qt.Commentf("stdout is not valid JSON:\n%s", stdout))
				c.Assert(report.Total, qt.Equals, 1)
				c.Assert(report.Passed, qt.Equals, 1)
				c.Assert(report.Failed, qt.Equals, 0)
				assertDesiredStateNoteOnStderrOnly(c.TB, stdout, stderr)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", adminURL)
			c.Assert(err, qt.IsNil)
			// Registered before the per-database cleanups so that LIFO order
			// closes the admin connection last; a deferred Close would run
			// first and leave the drops with no connection.
			c.Cleanup(func() { _ = adminDB.Close() })

			// Every row gets its own source and throwaway database. The runner
			// does not reset a caller-owned throwaway between invocations, so a
			// shared one would let a later row pass on an earlier row's state.
			sourceURL := freshSchemaTestE2EDatabase(c.TB, ctx, adminDB, adminURL, "src")
			devURL := freshSchemaTestE2EDatabase(c.TB, ctx, adminDB, adminURL, "dev")
			seedSchemaTestSourceDatabase(c.TB, ctx, sourceURL)

			args := append([]string{
				"schema", "test",
				"--dir", writeSchemaTestDatabaseSourceCases(c.TB),
				"--root-dir", sourceURL,
				"--db-url", devURL,
			}, tt.args...)
			stdout, stderr, runErr := runPtahSplitStreams(ctx, args)

			c.Assert(runErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			tt.verify(c, stdout, stderr)
		})
	}
}

// assertDesiredStateNoteOnStderrOnly pins where the introspected roles/grants
// note is written. A fresh PostgreSQL database always carries the connecting
// role and its own GRANT USAGE ON SCHEMA public TO PUBLIC, so the note is
// always emitted here -- the only question is which stream it lands on.
func assertDesiredStateNoteOnStderrOnly(tb testing.TB, stdout, stderr string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(stderr, qt.Contains, "introspected from the desired-state database")
	c.Assert(stdout, qt.Not(qt.Contains), "introspected from the desired-state database")
}

// freshSchemaTestE2EDatabase creates a uniquely named database and registers
// its removal.
func freshSchemaTestE2EDatabase(tb testing.TB, ctx context.Context, adminDB *sql.DB, adminURL, role string) string {
	c := qt.New(tb)
	c.Helper()
	name := fmt.Sprintf("ptah_schema_test_src_e2e_%s_%d", role, time.Now().UnixNano())
	createE2EDatabase(c.TB, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c.TB, context.Background(), adminDB, name) })
	return replaceDatabaseName(c.TB, adminURL, name)
}

// seedSchemaTestSourceDatabase creates the table the cases assert on. It exists
// in the source database and nowhere else, which is what makes the fixture
// discriminate: with the table declared in every input, the run would pass
// whether or not the source was read.
func seedSchemaTestSourceDatabase(tb testing.TB, ctx context.Context, sourceURL string) {
	c := qt.New(tb)
	c.Helper()
	db, err := sql.Open("pgx", sourceURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	_, err = db.ExecContext(ctx,
		"CREATE TABLE orders_from_db (id SERIAL PRIMARY KEY, note TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}

func writeSchemaTestDatabaseSourceCases(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	return writeLiveTestCases(c.TB, `cases:
  - name: db-sourced table exists
    steps:
      - name: introspected table is present
        assert:
          query: SELECT count(*) FROM orders_from_db
          row_count: 1
`)
}

// runPtahSplitStreams runs the native command with stdout and stderr kept
// apart. The merged-stream helper used elsewhere in this package cannot see the
// difference this test exists to check.
func runPtahSplitStreams(ctx context.Context, args []string) (stdout, stderr string, err error) {
	cmd := root.NewRootCommand()
	var outBuf, errBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(args)
	err = cmd.ExecuteContext(ctx)
	return outBuf.String(), errBuf.String(), err
}
