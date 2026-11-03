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

// lineCommentBodyNodes is a materialized view whose body ends in a line
// comment, as a schema file writes it with the comment before the semicolon,
// and a second view after it.
func lineCommentBodyNodes() []ast.Node {
	return []ast.Node{
		ast.NewCreateMaterializedView("order_lc").SetBody("SELECT count(*) AS n FROM orders WITH NO DATA -- empty too"),
		ast.NewCreateMaterializedView("order_snapshot").SetBody("SELECT max(id) AS last_id FROM orders WITH DATA"),
	}
}

// TestBuildMigrationFileContents_EndsALineCommentBeforeTheSemicolon_HappyPath
// writes a view whose body ends in a line comment. The semicolon goes on the
// line after the comment, where it ends the statement. On the comment's line it
// would be part of the comment, and the next CREATE would run into this one.
func TestBuildMigrationFileContents_EndsALineCommentBeforeTheSemicolon_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format atlasmigrate.FileFormat
		want   string
	}{
		{
			name:   "a native file",
			format: atlasmigrate.FileFormat{Template: atlasreport.NormalizeMigrateDiffFormat("")},
			want: "-- Create \"order_lc\" materialized view\n" +
				"CREATE MATERIALIZED VIEW \"order_lc\" AS\n" +
				"SELECT count(*) AS n FROM orders WITH NO DATA -- empty too\n;\n" +
				"-- Create \"order_snapshot\" materialized view\n" +
				"CREATE MATERIALIZED VIEW \"order_snapshot\" AS\n" +
				"SELECT max(id) AS last_id FROM orders WITH DATA;\n",
		},
		{
			name:   "a template that writes every statement on one line",
			format: atlasmigrate.FileFormat{Template: atlasreport.NormalizeMigrateDiffFormat(`{{ range .Changes }}{{ .Cmd }}; {{ end }}`)},
			want: "CREATE MATERIALIZED VIEW \"order_lc\" AS\n" +
				"SELECT count(*) AS n FROM orders WITH NO DATA -- empty too\n; " +
				"CREATE MATERIALIZED VIEW \"order_snapshot\" AS\n" +
				"SELECT max(id) AS last_id FROM orders WITH DATA; ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			contents, err := atlasmigrate.BuildMigrationFileContents(
				t.Context(), selectedRuntime(c),
				platform.Postgres, capability.ForDialect(platform.Postgres), tt.format, lineCommentBodyNodes())

			c.Assert(err, qt.IsNil)
			c.Assert(contents, qt.HasLen, 1)
			c.Assert(contents[0].SQL, qt.Equals, tt.want)
		})
	}
}
