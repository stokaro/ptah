//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// An index is named in its table's schema, and on CockroachDB within its
// table. Each test below declares two indexes called t_a, applies comments on
// them, and reads back from the catalog which index holds which comment. A
// comment found by the index's name alone lands on the first index declared
// and leaves the other without one.

// heldIndexComments reads back the comment of every index called t_a, under
// its table's qualified name, as one line: `public.t=pia s2.t=ia`.
func heldIndexComments(c *qt.C, dev devDialectDatabase) string {
	c.Helper()
	var held string
	err := dev.conn.QueryRowContext(c.Context(), `
		SELECT string_agg(n.nspname || '.' || r.relname || '=' || COALESCE(obj_description(c.oid, 'pg_class'), ''),
			' ' ORDER BY n.nspname, r.relname)
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_class r ON r.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = r.relnamespace
		WHERE c.relname = 't_a'`).Scan(&held)
	c.Assert(err, qt.IsNil)
	return held
}

// applyIndexComments applies document to dev, and diffs the database against
// it again.
func applyIndexComments(c *qt.C, dev devDialectDatabase, document string) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
}

// indexCommentStep is one apply: the document and the comments the indexes
// hold after it.
type indexCommentStep struct {
	document string
	want     string
}

// twoSchemaIndexComments declares s2.t before public.t, each with an index
// t_a, and then the comments.
func twoSchemaIndexComments(comments string) string {
	return "CREATE SCHEMA s2;\n" +
		"CREATE TABLE s2.t (a int);\nCREATE INDEX t_a ON s2.t (a);\n" +
		"CREATE TABLE public.t (a int);\nCREATE INDEX t_a ON public.t (a);\n" + comments
}

// twoTableIndexComments declares public.t and public.u, each with an index
// t_a, and then the comments.
func twoTableIndexComments(comments string) string {
	return "CREATE TABLE t (a int);\nCREATE TABLE u (a int);\n" +
		"CREATE INDEX t_a ON t (a);\nCREATE INDEX t_a ON u (a);\n" + comments
}

// twoSchemaIndexCommentSteps comments both indexes, then changes one comment and
// removes the other, then comments only the index whose comment was removed, by
// its bare name.
var twoSchemaIndexCommentSteps = []indexCommentStep{
	{
		document: twoSchemaIndexComments("COMMENT ON INDEX s2.t_a IS 'ia';\nCOMMENT ON INDEX public.t_a IS 'pia';\n"),
		want:     "public.t=pia s2.t=ia",
	},
	{
		document: twoSchemaIndexComments("COMMENT ON INDEX s2.t_a IS 'ia2';\n"),
		want:     "public.t= s2.t=ia2",
	},
	{
		document: twoSchemaIndexComments("COMMENT ON INDEX t_a IS 'pia2';\n"),
		want:     "public.t=pia2 s2.t=",
	},
}

// applyIndexCommentSteps applies each step to dev and reads the comments back.
func applyIndexCommentSteps(c *qt.C, dev devDialectDatabase, steps []indexCommentStep) {
	c.Helper()
	for _, step := range steps {
		applyIndexComments(c, dev, step.document)
		c.Assert(heldIndexComments(c, dev), qt.Equals, step.want, qt.Commentf("%s", step.document))
	}
}

// TestSchemaApplyCommentsSameNamedIndexesInTwoSchemasE2E applies
// [twoSchemaIndexCommentSteps]. Each comment is set on the index of the schema
// the statement names.
func TestSchemaApplyCommentsSameNamedIndexesInTwoSchemasE2E(t *testing.T) {
	t.Run("PostgreSQL", func(t *testing.T) {
		c := qt.New(t)
		applyIndexCommentSteps(c, postgresCommentsDatabase(c), twoSchemaIndexCommentSteps)
	})
	t.Run("CockroachDB", func(t *testing.T) {
		c := qt.New(t)
		applyIndexCommentSteps(c, cockroachDevDatabase(c), twoSchemaIndexCommentSteps)
	})
}

// TestSchemaApplyCommentsOneTablesIndexOnCockroachDBE2E comments two indexes
// of one name in one schema, which CockroachDB allows because it names an
// index within its table, each by its table: `t@t_a`. Then one comment
// changes and the other is removed.
func TestSchemaApplyCommentsOneTablesIndexOnCockroachDBE2E(t *testing.T) {
	c := qt.New(t)
	steps := []indexCommentStep{
		{
			document: twoTableIndexComments("COMMENT ON INDEX t@t_a IS 'tia';\nCOMMENT ON INDEX public.u@t_a IS 'uia';\n"),
			want:     "public.t=tia public.u=uia",
		},
		{
			document: twoTableIndexComments(`COMMENT ON INDEX "public"."u"@"t_a" IS 'uia2';` + "\n"),
			want:     "public.t= public.u=uia2",
		},
	}

	applyIndexCommentSteps(c, cockroachDevDatabase(c), steps)
}
