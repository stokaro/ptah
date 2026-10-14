//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// spannerIndexTable is the table every index in these tests is built on.
const spannerIndexTable = "CREATE TABLE t (id bigint PRIMARY KEY, a varchar, b bigint);\n"

// TestSpannerIndexComparisonE2E_HappyPath applies an index to Spanner, changes
// one attribute of its declaration, and applies again.
//
// Measured on master against the emulator behind PGAdapter v0.56.1, each
// change below reported "Schema is synced" and left the old index in place:
// the comparison read an index's name and nothing else of its definition.
// The plan has to rebuild the index, and the second apply of the same schema
// has to find nothing left to do.
func TestSpannerIndexComparisonE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		from     string
		to       string
		wantPlan string
	}{
		{
			name:     "uniqueness",
			from:     "CREATE INDEX t_i ON t (a);",
			to:       "CREATE UNIQUE INDEX t_i ON t (a);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE UNIQUE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"a\");\n",
		},
		{
			name:     "the order of the keys",
			from:     "CREATE INDEX t_i ON t (a, b);",
			to:       "CREATE INDEX t_i ON t (b, a);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"b\", \"a\");\n",
		},
		{
			name:     "a key's direction",
			from:     "CREATE INDEX t_i ON t (a);",
			to:       "CREATE INDEX t_i ON t (a DESC);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"a\" DESC);\n",
		},
		{
			name:     "a key's NULLS ordering",
			from:     "CREATE INDEX t_i ON t (b);",
			to:       "CREATE INDEX t_i ON t (b ASC NULLS FIRST);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"b\" NULLS FIRST);\n",
		},
		{
			// Spanner reports this key as DESC NULLS LAST, which read as
			// DESC alone did not: the key read back as ascending.
			name:     "a descending key's NULLS ordering",
			from:     "CREATE INDEX t_i ON t (b DESC);",
			to:       "CREATE INDEX t_i ON t (b DESC NULLS LAST);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"b\" DESC NULLS LAST);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL := spannerIndexDatabase(c)
			from := writeSpannerIndexSchema(c, "from.sql", spannerIndexTable+test.from+"\n")
			to := writeSpannerIndexSchema(c, "to.sql", spannerIndexTable+test.to+"\n")

			created, createdErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

			changed, changedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed, changedErr))
			c.Assert(changed, qt.Contains, test.wantPlan)

			reapplied, reappliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied, reappliedErr))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// spannerIndexDatabase is a scratch database on the Spanner emulator, which
// creates the database the URL names when a client first connects to it. The
// table is dropped when the test ends, since the database cannot be: its index
// first, because Spanner refuses to drop a table an index is on.
func spannerIndexDatabase(c *qt.C) string {
	c.Helper()
	dbURL := renameInPath(c, dbtarget.URL(c, dbtarget.Spanner), devDialectScratchName())
	conn := connectDevDialect(c, dbURL)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP INDEX IF EXISTS t_i")
		c.Check(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS t")
		c.Check(err, qt.IsNil)
	})
	return dbURL
}

func writeSpannerIndexSchema(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}
