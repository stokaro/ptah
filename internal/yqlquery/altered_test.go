package yqlquery_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/yqlquery"
)

func TestAlteredTable_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "bare name", text: "ALTER TABLE items ADD INDEX i GLOBAL ON (v)", want: "items"},
		{name: "quoted path", text: "ALTER TABLE `dir/items` ADD INDEX i GLOBAL ON (v);", want: "dir/items"},
		{name: "absolute path", text: "ALTER TABLE `/local/dir/items` ADD COLUMN c Utf8", want: "/local/dir/items"},
		{name: "translation setting", text: "--!syntax_v1\nALTER TABLE `items` ADD INDEX i GLOBAL ON (v)", want: "items"},
		{
			// A named expression carried into the query is dropped by YDB
			// when unused, and changes no path.
			name: "an unused named expression above it",
			text: "$x = 1;\nALTER TABLE `items` ADD INDEX i GLOBAL ON (v)",
			want: "items",
		},
		{name: "escaped backtick", text: "ALTER TABLE `tick\\`name` ADD COLUMN c Utf8", want: "tick`name"},
		{name: "doubled backtick", text: "ALTER TABLE `tick``name` ADD COLUMN c Utf8", want: "tick`name"},
		{name: "escaped backslash", text: "ALTER TABLE `back\\\\slash` ADD COLUMN c Utf8", want: "back\\slash"},
		{name: "keywords in any case", text: "alter table items add index i global on (v)", want: "items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table, ok := yqlquery.AlteredTable(test.text)

			c.Assert(ok, qt.IsTrue)
			c.Assert(table, qt.Equals, test.want)
		})
	}
}

// Each row is a query whose table the text alone does not settle.
func TestAlteredTable_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "not an ALTER TABLE", text: "CREATE TABLE `items` (id Int64 NOT NULL, PRIMARY KEY (id))"},
		{name: "a data query", text: "UPSERT INTO `items` (id) VALUES (1)"},
		{name: "two queries", text: "ALTER TABLE a ADD COLUMN c Utf8;\nALTER TABLE b ADD COLUMN c Utf8"},
		{name: "a named expression as the table", text: "$t = 'dir/t';\nALTER TABLE $t ADD COLUMN c Utf8"},
		{name: "a path prefix", text: "PRAGMA TablePathPrefix('/local/dir');\nALTER TABLE t ADD COLUMN c Utf8"},
		{name: "a C escape in the name", text: "ALTER TABLE `new\\nline` ADD COLUMN c Utf8"},
		{name: "a quoted keyword is not ALTER", text: "`ALTER` TABLE t ADD COLUMN c Utf8"},
		{name: "nothing after TABLE", text: "ALTER TABLE"},
		{name: "empty", text: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table, ok := yqlquery.AlteredTable(test.text)

			c.Assert(ok, qt.IsFalse)
			c.Assert(table, qt.Equals, "")
		})
	}
}
