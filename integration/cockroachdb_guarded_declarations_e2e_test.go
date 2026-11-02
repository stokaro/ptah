//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestCockroachDBGuardedDeclarationsDiffEmptyE2E builds a CockroachDB database
// from each file and diffs it with the file: nothing changes.
//
// Each file guards a CREATE TABLE or a CREATE SEQUENCE with a name a relation
// of another kind already holds. CockroachDB keeps tables, views, materialized
// views and sequences in one namespace per schema, so v26.3.2 skips the
// guarded statement, the CREATE TABLE with `relation "t" already exists,
// skipping` and the CREATE SEQUENCE without a notice. A reader that looks for
// the name among one kind only plans the table or the sequence on every run
// (stokaro/ptah#5042).
//
// A file that guards a table after a sequence of its name is read the same
// way, and the unit tests hold it; it is not a row here because CockroachDB
// lists a sequence's own key among the table constraints, and the comparison
// plans to drop that key for any database holding a sequence.
func TestCockroachDBGuardedDeclarationsDiffEmptyE2E(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "a table guarded after a view of its name", sql: "CREATE VIEW t AS SELECT 1 AS a;\nCREATE TABLE IF NOT EXISTS t (a int);"},
		{name: "a table guarded after a materialized view of its name", sql: "CREATE MATERIALIZED VIEW t AS SELECT 1 AS a;\nCREATE TABLE IF NOT EXISTS t (a int);"},
		{name: "a sequence guarded after a table of its name", sql: "CREATE TABLE s (a int);\nCREATE SEQUENCE IF NOT EXISTS s;"},
		{name: "a sequence guarded after a view of its name", sql: "CREATE VIEW s AS SELECT 1 AS a;\nCREATE SEQUENCE IF NOT EXISTS s;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL := newCockroachIndexDatabase(c, ctx, "ptah_guarded")
			db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			execCockroachStatements(c, ctx, db, test.sql)
			schema := writeCockroachIndexSchema(c, "schema.sql", test.sql+"\n")

			stdout, stderr, err := runPtahSplitStreams(ctx, []string{"schema", "diff", "--from", dbURL, "--to", "file://" + filepath.ToSlash(schema)})

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}
