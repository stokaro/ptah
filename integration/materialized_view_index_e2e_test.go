//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// PostgreSQL indexes a materialized view as it indexes a table, and the SQL
// schema reader reads every statement that names such an index. The schema
// below declares two materialized views, mv1 and mv2, each beside a table of
// its name in s2, so an index the reader resolved by its relation's name alone
// lands on the table. mv1 is declared without a schema and mv2 with the
// default one.

// viewIndexSchema declares the relations, the three indexes of the views, and
// then statements.
func viewIndexSchema(statements string) string {
	return "CREATE SCHEMA s2;\n" +
		"CREATE TABLE o (a int);\n" +
		"CREATE TABLE s2.mv1 (a int);\nCREATE TABLE s2.mv2 (a int);\n" +
		"CREATE MATERIALIZED VIEW mv1 AS SELECT a FROM o;\n" +
		"CREATE MATERIALIZED VIEW public.mv2 AS SELECT a FROM o;\n" +
		"CREATE INDEX mv1_a ON mv1 (a);\nCREATE INDEX mv1_gone ON mv1 (a);\nCREATE INDEX mv2_a ON mv2 (a);\n" +
		statements
}

// heldViewIndexes reads back from pg_indexes every index of a relation called
// mv1 or mv2 in public and s2, the views and the tables, with its comment, as
// one line: `public.mv1.mv1_a=comment ...`.
func heldViewIndexes(c *qt.C, db *sql.DB) string {
	c.Helper()
	var held string
	err := db.QueryRowContext(c.Context(), `
		SELECT string_agg(schemaname || '.' || tablename || '.' || indexname || '=' ||
			COALESCE(obj_description(format('%I.%I', schemaname, indexname)::regclass, 'pg_class'), ''),
			' ' ORDER BY schemaname, tablename, indexname)
		FROM pg_indexes
		WHERE schemaname IN ('public', 's2') AND tablename IN ('mv1', 'mv2')`).Scan(&held)
	c.Assert(err, qt.IsNil)
	return held
}

// TestSchemaApplyMaterializedViewIndexStatementsE2E applies the indexes, then
// a document that drops one, renames the other two, one of them with IF
// EXISTS, and comments both by their new names. After each apply it reads the
// indexes back from the catalog and diffs the database against the document
// again.
func TestSchemaApplyMaterializedViewIndexStatementsE2E(t *testing.T) {
	c := qt.New(t)
	url := postgresScratchDevURL(c, "mvops")
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	steps := []struct {
		document string
		want     string
	}{
		{
			document: viewIndexSchema(""),
			want:     "public.mv1.mv1_a= public.mv1.mv1_gone= public.mv2.mv2_a=",
		},
		{
			document: viewIndexSchema("DROP INDEX mv1_gone;\n" +
				"ALTER INDEX mv1_a RENAME TO mv1_b;\nALTER INDEX IF EXISTS mv2_a RENAME TO mv2_b;\n" +
				"COMMENT ON INDEX mv1_b IS 'renamed';\nCOMMENT ON INDEX mv2_b IS 'renamed too';\n"),
			want: "public.mv1.mv1_b=renamed public.mv2.mv2_b=renamed too",
		},
	}

	for _, step := range steps {
		path := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(path, []byte(step.document), 0o600), qt.IsNil)

		out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", path, "--auto-approve")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(heldViewIndexes(c, db), qt.Equals, step.want, qt.Commentf("%s", step.document))
		out, err = runPtahNativeWithError("schema", "diff", "--from", url, "--to", path)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
	}
}
