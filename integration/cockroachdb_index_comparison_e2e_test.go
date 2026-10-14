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

// cockroachIndexTable is the table every index in these tests is built on.
const cockroachIndexTable = "CREATE TABLE t (id INT8 PRIMARY KEY, a STRING, b INT8, j JSONB);\n"

// TestCockroachDBIndexComparisonE2E_HappyPath applies an index to CockroachDB,
// changes one attribute of its declaration, and applies again.
//
// Measured on master against v26.3.2, each change below reported "Schema is
// synced" and left the old index in place: the comparison read an index's
// name and nothing else of its definition. The plan has to rebuild the index,
// and the second apply of the same schema has to find nothing left to do.
func TestCockroachDBIndexComparisonE2E_HappyPath(t *testing.T) {
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
			wantPlan: "DROP INDEX IF EXISTS \"t\"@\"t_i\";\nCREATE UNIQUE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"a\");\n",
		},
		{
			name:     "a key column",
			from:     "CREATE INDEX t_i ON t (a);",
			to:       "CREATE INDEX t_i ON t (b);",
			wantPlan: "DROP INDEX IF EXISTS \"t\"@\"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"b\");\n",
		},
		{
			name:     "the order of the keys",
			from:     "CREATE INDEX t_i ON t (a, b);",
			to:       "CREATE INDEX t_i ON t (b, a);",
			wantPlan: "DROP INDEX IF EXISTS \"t\"@\"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"b\", \"a\");\n",
		},
		{
			name:     "a key's direction",
			from:     "CREATE INDEX t_i ON t (a);",
			to:       "CREATE INDEX t_i ON t (a DESC);",
			wantPlan: "DROP INDEX IF EXISTS \"t\"@\"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" (\"a\" DESC);\n",
		},
		{
			name:     "a key's expression",
			from:     "CREATE INDEX t_i ON t (lower(a));",
			to:       "CREATE INDEX t_i ON t (upper(a));",
			wantPlan: "DROP INDEX IF EXISTS \"t\"@\"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" ((upper(a)));\n",
		},
		{
			name:     "the access method",
			from:     "CREATE INDEX t_i ON t (j);",
			to:       "CREATE INDEX t_i ON t USING GIN (j);",
			wantPlan: "DROP INDEX IF EXISTS \"t\"@\"t_i\";\nCREATE INDEX IF NOT EXISTS \"t_i\" ON \"t\" USING GIN (\"j\");\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL := newCockroachIndexDatabase(c, ctx, "ptah_ix_cmp")
			from := writeCockroachIndexSchema(c, "from.sql", cockroachIndexTable+test.from+"\n")
			to := writeCockroachIndexSchema(c, "to.sql", cockroachIndexTable+test.to+"\n")

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

// TestCockroachDBIndexReadE2E_HappyPath describes a CockroachDB database's
// indexes and applies the description to an empty database.
//
// CockroachDB orders NULLs first in an ascending key and has no NULLS clause
// in an index key, and the read took that order for a declared NULLS FIRST:
// measured on master against v26.3.2, the description wrote
// `CREATE INDEX ... ("a" NULLS FIRST)` for every index, and applying it failed
// with "unimplemented: this syntax".
func TestCockroachDBIndexReadE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	sourceURL := newCockroachIndexDatabase(c, ctx, "ptah_ix_src")
	targetURL := newCockroachIndexDatabase(c, ctx, "ptah_ix_dst")
	source := writeCockroachIndexSchema(c, "source.sql", cockroachIndexTable+
		"CREATE INDEX t_a ON t (a);\nCREATE INDEX t_ab ON t (a, b DESC);\n"+
		"CREATE INDEX t_lower ON t (lower(a));\nCREATE INDEX t_j ON t USING GIN (j);\n")
	created, createdErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", source, "--db-url", sourceURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

	read, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", sourceURL})
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
	c.Assert(read, qt.Not(qt.Contains), "NULLS")
	described := writeCockroachIndexSchema(c, "described.sql", read)

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", described, "--db-url", targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(applied, qt.Contains, `CREATE INDEX IF NOT EXISTS "t_ab" ON "t" ("a", "b" DESC);`)

	compared, comparedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", source, "--db-url", targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, comparedErr))
	c.Assert(compared, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// newCockroachIndexDatabase creates an empty database on the CockroachDB
// server and drops it when the test ends.
func newCockroachIndexDatabase(c *qt.C, ctx context.Context, prefix string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	return replaceDatabaseName(c, adminURL, name)
}

func writeCockroachIndexSchema(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// TestCockroachDBUniqueIndexReadE2E_HappyPath describes a CockroachDB
// database whose unique indexes have descending keys, and applies the
// description to an empty database.
//
// CockroachDB lists a unique index in pg_constraint as a UNIQUE constraint.
// Measured on master against v26.3.2, the description wrote
// `CONSTRAINT "t_a" UNIQUE ("a")` for `CREATE UNIQUE INDEX t_a ON t (a DESC)`,
// so the database built from it held an ascending key. A unique key without a
// direction is the control: it is still described as a constraint.
func TestCockroachDBUniqueIndexReadE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	sourceURL := newCockroachIndexDatabase(c, ctx, "ptah_uq_src")
	targetURL := newCockroachIndexDatabase(c, ctx, "ptah_uq_dst")
	source := writeCockroachIndexSchema(c, "source.sql", cockroachIndexTable+
		"CREATE UNIQUE INDEX t_a ON t (a DESC);\nCREATE UNIQUE INDEX t_ab ON t (a, b DESC);\n"+
		"CREATE UNIQUE INDEX t_b ON t (b);\n")
	created, createdErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", source, "--db-url", sourceURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

	read, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", sourceURL})
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
	c.Assert(read, qt.Contains, `CREATE UNIQUE INDEX IF NOT EXISTS "t_a" ON "t" ("a" DESC);`)
	c.Assert(read, qt.Contains, `CREATE UNIQUE INDEX IF NOT EXISTS "t_ab" ON "t" ("a", "b" DESC);`)
	c.Assert(read, qt.Contains, `CONSTRAINT "t_b" UNIQUE ("b")`)
	described := writeCockroachIndexSchema(c, "described.sql", read)

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", described, "--db-url", targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))

	compared, comparedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", source, "--db-url", targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, comparedErr))
	c.Assert(compared, qt.Equals, "Schema is synced, no changes to be made.\n")
}
