//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// yugabyteIndexTable is the table every index in these tests is built on.
const yugabyteIndexTable = "CREATE TABLE t (id INT8 PRIMARY KEY, a TEXT, b INT8, j JSONB);\n"

// TestYugabyteDBIndexComparisonE2E_HappyPath applies an index to YugabyteDB,
// changes one attribute of its declaration, and applies again.
//
// Measured on master against 2026.1.2, each change below reported "Schema is
// synced" and left the old index in place: the comparison read an index's
// name and nothing else of its definition. The plan has to rebuild the index,
// and the second apply of the same schema has to find nothing left to do.
func TestYugabyteDBIndexComparisonE2E_HappyPath(t *testing.T) {
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
			// The leading key is ASC rather than HASH only because the plan
			// says ASC; without it YugabyteDB ignores the ordering.
			name:     "a key's NULLS ordering",
			from:     "CREATE INDEX t_i ON t (b);",
			to:       "CREATE INDEX t_i ON t (b ASC NULLS FIRST);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"b\" ASC NULLS FIRST);\n",
		},
		{
			name:     "a key's expression",
			from:     "CREATE INDEX t_i ON t (lower(a));",
			to:       "CREATE INDEX t_i ON t (upper(a));",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" ((upper(a)));\n",
		},
		{
			name:     "a key's operator class",
			from:     "CREATE INDEX t_i ON t (a);",
			to:       "CREATE INDEX t_i ON t (a text_pattern_ops);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"a\" text_pattern_ops);\n",
		},
		{
			// No column type takes both lsm and ybgin on 2026.1.2: lsm refuses
			// JSONB, arrays and tsvector. The method is held by the unit test
			// of the rules; this row changes a GIN key's operator class.
			name:     "a GIN key's operator class",
			from:     "CREATE INDEX t_i ON t USING GIN (j);",
			to:       "CREATE INDEX t_i ON t USING GIN (j jsonb_path_ops);",
			wantPlan: "DROP INDEX IF EXISTS \"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" USING GIN (\"j\" jsonb_path_ops);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 10*time.Minute)
			defer cancel()
			dbURL := newYugabyteIndexDatabase(c, ctx)
			from := writeYugabyteIndexSchema(c, "from.sql", yugabyteIndexTable+test.from+"\n")
			to := writeYugabyteIndexSchema(c, "to.sql", yugabyteIndexTable+test.to+"\n")

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

// newYugabyteIndexDatabase creates an empty database on the YugabyteDB server
// and drops it when the test ends.
func newYugabyteIndexDatabase(c *qt.C, ctx context.Context) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.YugabyteDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_ix_yb_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	return replaceDatabaseName(c, adminURL, name)
}

func writeYugabyteIndexSchema(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}
