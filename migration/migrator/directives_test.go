package migrator

import (
	"testing"

	qt "github.com/frankban/quicktest"
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
			want: map[string]string{},
		},
		{
			name: "directive-looking text inside a string literal is not a directive",
			sql:  "INSERT INTO notes (body) VALUES ('runbook:\n-- +ptah online_ddl_tool=ghost\ndone');\nALTER TABLE users ADD COLUMN a INT;\n",
			want: map[string]string{},
		},
		{
			name: "directive-looking text inside a block comment is not a directive",
			sql:  "/*\n-- +ptah online_ddl_tool=ghost\n*/\nALTER TABLE users ADD COLUMN a INT;\n",
			want: map[string]string{},
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
			name: "directive prefix must be a whole word",
			sql:  "-- +ptahx key=value\n",
			want: map[string]string{},
		},
		{
			name: "empty input",
			sql:  "",
			want: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ParseFileDirectives(tt.sql), qt.DeepEquals, tt.want)
		})
	}
}
