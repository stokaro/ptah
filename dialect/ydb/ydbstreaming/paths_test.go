package ydbstreaming_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbstreaming"
)

// TestNamedPaths reads the paths a streaming query body names as a source or
// a target, and leaves out names that are not paths of the database.
func TestNamedPaths(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "a copy between topics", text: "INSERT INTO `app/output` SELECT * FROM `app/input`;", want: []string{"app/output", "app/input"}},
		{name: "bare names and a join", text: "insert into out select * from input join lookup on input.k = lookup.k;", want: []string{"out", "input", "lookup"}},
		{name: "each path once", text: "INSERT INTO `t` SELECT * FROM `t`; INSERT INTO `t` SELECT * FROM `u`;", want: []string{"t", "u"}},
		{name: "escapes in a backticked name", text: "SELECT * FROM `a``b/c\\`d`;", want: []string{"a`b/c`d"}},
		{name: "a name in a comment or a string", text: "/* FROM `x` */ SELECT 'FROM y' FROM `z`; -- INTO `w`", want: []string{"z"}},
		{name: "a data source's topic", text: "INSERT INTO `out` SELECT * FROM `source`.`topic` WITH (FORMAT = json_each_row);", want: []string{"out"}},
		{name: "a named expression and a subquery", text: "$input = SELECT 1; SELECT * FROM $input; SELECT * FROM (SELECT * FROM `t`);", want: []string{"t"}},
		{name: "a table function", text: "SELECT * FROM RANGE(`a`, `b`);"},
		{name: "a path prefix", text: "PRAGMA TablePathPrefix = \"/local/app\"; SELECT * FROM `input`;"},
		{name: "no source", text: "SELECT 1;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbstreaming.NamedPaths(test.text), qt.DeepEquals, test.want)
		})
	}
}
