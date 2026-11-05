package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
)

// changeCommentPlan is two statements under the comments that name their
// changes, as `migrate diff` hands them to the report.
func changeCommentPlan() []atlasreport.SchemaChange {
	return []atlasreport.SchemaChange{
		{Cmd: "CREATE TABLE \"owners\" (\n  \"id\" bigint NOT NULL\n);", Comment: `create "owners" table`},
		{Cmd: `CREATE INDEX "owners_id_idx" ON "owners" ("id")`, Comment: `create index "owners_id_idx" to table: "owners"`},
	}
}

// TestWriteMigrateDiff_ChangeComments_HappyPath pins how a change's comment is
// written. Measured on the pinned community binary v1.3.0 against PostgreSQL
// 15.19, the `sql` helper of a native migration file writes the comment's
// first letter in upper case, `-- Create "owners" table`, while a
// golang-migrate file, written through that binary's own template, keeps it
// as the change names it. A template can also read the comment apart from the
// statement, as `.Comment`.
func TestWriteMigrateDiff_ChangeComments_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		foreign bool
		want    string
	}{
		{
			name:   "the sql helper of a native file",
			format: `{{ sql . "  " }}`,
			want: "-- Create \"owners\" table\nCREATE TABLE \"owners\" (\n  \"id\" bigint NOT NULL\n);\n" +
				"-- Create index \"owners_id_idx\" to table: \"owners\"\nCREATE INDEX \"owners_id_idx\" ON \"owners\" (\"id\");\n",
		},
		{
			name:    "a file of a layout another tool reads",
			format:  `{{ sql . "\t" }}`,
			foreign: true,
			want: "-- create \"owners\" table\nCREATE TABLE \"owners\" (\n\t\"id\" bigint NOT NULL\n);\n" +
				"-- create index \"owners_id_idx\" to table: \"owners\"\nCREATE INDEX \"owners_id_idx\" ON \"owners\" (\"id\");\n",
		},
		{
			name:   "the template fields",
			format: `{{ range .Changes }}[{{ .Comment }}]{{ end }}`,
			want:   `[create "owners" table][create index "owners_id_idx" to table: "owners"]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{
				Dialect:           "postgres",
				Changes:           changeCommentPlan(),
				CommentsAsWritten: tt.foreign,
			})
			var out bytes.Buffer

			err := atlasreport.WriteMigrateDiff(&out, tt.format, report)

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, tt.want)
		})
	}
}

// TestNameStatement_HappyPath pins where the comment that names a change goes:
// directly above the SQL it names, below any note the plan wrote at the head
// of the statement, such as a change a diff policy left out.
func TestNameStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		comment   string
		statement string
		want      string
	}{
		{
			name:      "a statement",
			comment:   `add column "c" to table: "t"`,
			statement: `ALTER TABLE "t" ADD COLUMN "c" TEXT`,
			want:      "-- Add column \"c\" to table: \"t\"\nALTER TABLE \"t\" ADD COLUMN \"c\" TEXT",
		},
		{
			name:      "a statement under notes",
			comment:   `add column "c" to table: "t"`,
			statement: "-- SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\nALTER TABLE \"t\" ADD COLUMN \"c\" TEXT",
			want: "-- SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n" +
				"-- Add column \"c\" to table: \"t\"\nALTER TABLE \"t\" ADD COLUMN \"c\" TEXT",
		},
		{
			name:      "a change nothing names",
			statement: "-- SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\nALTER TABLE \"t\" ADD COLUMN \"c\" TEXT",
			want:      "-- SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\nALTER TABLE \"t\" ADD COLUMN \"c\" TEXT",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasreport.NameStatement(tt.comment, tt.statement), qt.Equals, tt.want)
		})
	}
}

// TestWriteSchemaDiff_ChangeCommentBelowANote_HappyPath pins the `sql` helper
// on a statement that opens with a note: the note stays first, and the name is
// written directly above the statement it names.
func TestWriteSchemaDiff_ChangeCommentBelowANote_HappyPath(t *testing.T) {
	c := qt.New(t)
	report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{
		Dialect: "postgres",
		Changes: []atlasreport.SchemaChange{{
			Cmd:     "-- SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\nALTER TABLE \"t\" ADD COLUMN \"c\" text",
			Comment: `add column "c" to table: "t"`,
		}},
	})
	var out bytes.Buffer

	err := atlasreport.WriteMigrateDiff(&out, `{{ sql . "  " }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "-- SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n"+
		"-- Add column \"c\" to table: \"t\"\nALTER TABLE \"t\" ADD COLUMN \"c\" text;\n")
}
