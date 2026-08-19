package migrator

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

func TestParseFileDirectives(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string]string
	}{
		{
			name: "single directive",
			sql:  "-- +ptah online_ddl_tool=ghost\nALTER TABLE users ADD COLUMN bio TEXT;\n",
			want: map[string]string{"online_ddl_tool": "ghost"},
		},
		{
			name: "leading whitespace and multiple pairs on one line",
			sql:  "   --  +ptah online_ddl_tool=pt-osc foo=bar\nSELECT 1;\n",
			want: map[string]string{"online_ddl_tool": "pt-osc", "foo": "bar"},
		},
		{
			name: "multiple directive lines merge with later lines winning",
			sql:  "-- +ptah a=1\n-- +ptah b=2\n-- +ptah a=3\n",
			want: map[string]string{"a": "3", "b": "2"},
		},
		{
			name: "regular comments are not directives",
			sql:  "-- ordinary comment with ptah in it\n-- ptah key=value (no plus)\nSELECT 1; -- +ptah trailing=nope is fine because the line does not start with the comment\n",
			want: make(map[string]string),
		},
		{
			name: "directive-looking text inside a string literal is not a directive",
			sql:  "INSERT INTO notes (body) VALUES ('runbook:\n-- +ptah online_ddl_tool=ghost\ndone');\nALTER TABLE users ADD COLUMN a INT;\n",
			want: make(map[string]string),
		},
		{
			name: "directive-looking text inside a block comment is not a directive",
			sql:  "/*\n-- +ptah online_ddl_tool=ghost\n*/\nALTER TABLE users ADD COLUMN a INT;\n",
			want: make(map[string]string),
		},
		{
			name: "real directive alongside a decoy inside a string still parses",
			sql:  "-- +ptah online_ddl_tool=pt-osc\nINSERT INTO notes (body) VALUES ('-- +ptah online_ddl_tool=ghost');\n",
			want: map[string]string{"online_ddl_tool": "pt-osc"},
		},
		{
			name: "tokens without an equals sign are ignored",
			sql:  "-- +ptah standalone online_ddl_tool=ghost =orphan\n",
			want: map[string]string{"online_ddl_tool": "ghost"},
		},
		{
			name: "bare no transaction shorthand",
			sql:  "-- +ptah no_transaction online_ddl_tool=ghost\n",
			want: map[string]string{"no_transaction": "true", "online_ddl_tool": "ghost"},
		},
		{
			name: "directive prefix must be a whole word",
			sql:  "-- +ptahx key=value\n",
			want: make(map[string]string),
		},
		{
			name: "empty input",
			sql:  "",
			want: make(map[string]string),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ParseFileDirectives(tt.sql), qt.DeepEquals, tt.want)
		})
	}
}

// TestParseFileDirectivesTreatsHashCommentsAsHeaderComments keeps the header
// boundary on the same dialect rules as execution.
//
// Which dialects those are is not restated here, and that is the point: the
// answer comes from the lexer options the file will be read with, so this table
// is a reading of that one authority rather than a second list beside it. SQL
// Server is the live negative control -- it is the one target whose options
// disable hash comments, and its row is what keeps every other row from being
// vacuously true.
func TestParseFileDirectivesTreatsHashCommentsAsHeaderComments(t *testing.T) {
	sql := "# ordinary hash comment\n-- +ptah no_transaction\nSELECT 1;\n"

	tests := []struct {
		name    string
		dialect string
		want    map[string]string
	}{
		{
			// Load time has no connection yet, so it must not read a header
			// SHORTER than the dialect that will execute the file would.
			name:    "unresolved dialect",
			dialect: "",
			want:    map[string]string{"no_transaction": "true"},
		},
		{name: "mysql", dialect: platform.MySQL, want: map[string]string{"no_transaction": "true"}},
		{name: "mariadb", dialect: platform.MariaDB, want: map[string]string{"no_transaction": "true"}},
		{name: "clickhouse", dialect: platform.ClickHouse, want: map[string]string{"no_transaction": "true"}},
		{name: "postgres", dialect: platform.Postgres, want: map[string]string{"no_transaction": "true"}},
		{name: "sqlite", dialect: platform.SQLite, want: map[string]string{"no_transaction": "true"}},
		{name: "sqlserver", dialect: platform.SQLServer, want: make(map[string]string)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Check(parseFileDirectivesForDialect(sql, test.dialect), qt.DeepEquals, test.want)
		})
	}
}
