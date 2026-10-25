package atlasmigrate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/atlasreport"
)

// changeCommentNodes is a plan of a new table and its index, then a planner's
// note in front of a dropped table.
func changeCommentNodes() []ast.Node {
	return []ast.Node{
		ast.NewCreateTable("owners").AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()),
		ast.NewIndex("owners_id_idx", "owners", "id"),
		ast.NewComment("NOTE: archive is dropped by request"),
		ast.NewDropTable("archive"),
	}
}

// TestBuildMigrationFileContents_WritesEachChangeUnderItsComment_HappyPath
// pins the comment above each statement of a migration file, in the words the
// pinned community binary v1.3.0 writes: capitalized by the `sql` helper of a
// native file, as the change names it in a file of a layout another tool
// reads. The comment heads the statement a node renders. A planner's own note
// is a report, not a change: it is a statement of its own with no change
// comment, so it does not land under the change of the statement after it,
// which may be another change altogether.
func TestBuildMigrationFileContents_WritesEachChangeUnderItsComment_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format atlasmigrate.FileFormat
		want   string
	}{
		{
			name:   "a native file",
			format: atlasmigrate.FileFormat{Template: atlasreport.NormalizeMigrateDiffFormat("")},
			want: "-- Create \"owners\" table\n" +
				"CREATE TABLE \"owners\" (\n  \"id\" BIGINT PRIMARY KEY NOT NULL\n);\n" +
				"-- Create index \"owners_id_idx\" to table: \"owners\"\n" +
				"CREATE INDEX \"owners_id_idx\" ON \"owners\" (\"id\");\n" +
				"-- NOTE: archive is dropped by request\n" +
				"-- Drop \"archive\" table\n" +
				"DROP TABLE \"archive\";\n",
		},
		{
			name:   "a file another tool reads",
			format: atlasmigrate.FileFormat{Template: atlasreport.NormalizeMigrateDiffFormat(""), Foreign: true},
			want: "-- create \"owners\" table\n" +
				"CREATE TABLE \"owners\" (\n  \"id\" BIGINT PRIMARY KEY NOT NULL\n);\n" +
				"-- create index \"owners_id_idx\" to table: \"owners\"\n" +
				"CREATE INDEX \"owners_id_idx\" ON \"owners\" (\"id\");\n" +
				"-- NOTE: archive is dropped by request\n" +
				"-- drop \"archive\" table\n" +
				"DROP TABLE \"archive\";\n",
		},
		{
			// The note ends its line, so the statement after it is not part
			// of its line comment.
			name:   "a template that writes every statement on one line",
			format: atlasmigrate.FileFormat{Template: atlasreport.NormalizeMigrateDiffFormat(`{{ range .Changes }}{{ .Cmd }}; {{ end }}`)},
			want: "CREATE TABLE \"owners\" (\n  \"id\" BIGINT PRIMARY KEY NOT NULL\n); " +
				"CREATE INDEX \"owners_id_idx\" ON \"owners\" (\"id\"); " +
				"-- NOTE: archive is dropped by request\n; " +
				"DROP TABLE \"archive\"; ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			contents, err := atlasmigrate.BuildMigrationFileContents(
				t.Context(), selectedRuntime(c),
				platform.Postgres, capability.ForDialect(platform.Postgres), tt.format, changeCommentNodes())

			c.Assert(err, qt.IsNil)
			c.Assert(contents, qt.HasLen, 1)
			c.Assert(contents[0].SQL, qt.Equals, tt.want)
		})
	}
}
