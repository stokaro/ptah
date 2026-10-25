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
