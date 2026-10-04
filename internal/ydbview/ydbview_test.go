package ydbview_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbview"
)

// Each pair is a view query as it was written in a CREATE VIEW and the text
// DescribeView returned for it, measured on YDB 25.1.4.7 and 26.2.1.14 alike.
// The declaration and the server's text read into the same form, which is
// what lets a view applied once compare equal to its declaration.
func TestQueryText_ReadsTheDeclarationAndTheStoredTextAlike(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{
			name:     "spacing, comments and a quoted name",
			declared: "select   t.id,\n  `name` -- a comment\n  /* block */ FROM `t` as t where amount>=10 AND name = \"a  b\" ORDER BY id DESC LIMIT 5",
			stored:   "select t . id , `name` FROM `t` as t where amount >= 10 AND name = \"a  b\" ORDER BY id DESC LIMIT 5",
		},
		{
			name:     "a trailing semicolon and calls",
			declared: "SELECT COUNT(*) AS c, MAX(amount) FROM t GROUP BY name HAVING COUNT(*) > 1;",
			stored:   "SELECT COUNT ( * ) AS c , MAX ( amount ) FROM t GROUP BY name HAVING COUNT ( * ) > 1",
		},
		{
			name: "literals with suffixes, escapes and a multi-line string",
			declared: `SELECT id, 'x\'y' AS s, "q\"z"u AS u, CAST(amount AS Int64) AS a64, Unicode::ToUpper(name) AS up, ` +
				`-1 AS neg, 10u AS ten, 1.5e3 AS f, @@raw  @@ AS r, amount <> 2 AS ne, amount != 3 AS ne2, ` +
				`name || "s" AS cat FROM t`,
			stored: `SELECT id , 'x\'y' AS s , "q\"z"u AS u , CAST ( amount AS Int64 ) AS a64 , ` +
				`Unicode :: ToUpper ( name ) AS up , - 1 AS neg , 10u AS ten , 1.5e3 AS f , @@raw  @@ AS r , ` +
				`amount <> 2 AS ne , amount != 3 AS ne2 , name || "s" AS cat FROM t`,
		},
		{
			name:     "a leading comment and line breaks inside literals",
			declared: "\n-- leading comment\n\tSELECT \"a\nb\" AS s,\t@@x\n  y@@ AS m, id FROM t -- trailing comment\n;",
			stored:   "SELECT \"a\nb\" AS s , @@x\n  y@@ AS m , id FROM t",
		},
		{
			name:     "a path in a directory",
			declared: "SELECT id, label FROM `app/sub/items`",
			stored:   "SELECT id , label FROM `app/sub/items`",
		},
		{
			name:     "parenthesized selects",
			declared: "(SELECT 1 AS a) UNION ALL (SELECT 2 AS a)",
			stored:   "( SELECT 1 AS a ) UNION ALL ( SELECT 2 AS a )",
		},
		{
			name:     "a pragma the view was created under",
			declared: "PRAGMA OrderedColumns; -- c\nSELECT 1 AS a",
			stored:   "PRAGMA OrderedColumns; -- c\n\nSELECT 1 AS a",
		},
		{
			name:     "two semicolons",
			declared: "SELECT 1 AS a;;",
			stored:   "SELECT 1 AS a",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbview.QueryText(test.declared), qt.Equals, ydbview.QueryText(test.stored))
		})
	}
}

// The form keeps what changes a view: YDB compares names case-sensitively, so
// `id` and `ID` are two columns, and a literal's contents are its value.
func TestQueryText_KeepsWhatTellsTwoViewsApart(t *testing.T) {
	tests := []struct {
		name  string
		one   string
		other string
	}{
		{name: "a column's case", one: "SELECT id FROM t", other: "SELECT ID FROM t"},
		{name: "a keyword's case", one: "SELECT id FROM t", other: "select id from t"},
		{name: "spaces inside a literal", one: `SELECT "a b" AS s`, other: `SELECT "a  b" AS s`},
		{name: "an alias without AS", one: "SELECT x y FROM t", other: "SELECT xy FROM t"},
		{name: "a literal's suffix", one: `SELECT "a"u AS s`, other: `SELECT "a" AS s`},
		{name: "a quoted name", one: "SELECT `a b` FROM t", other: "SELECT `a  b` FROM t"},
		{name: "the pragma a view was created under",
			one: "SELECT id FROM `sub/items`", other: "PRAGMA TablePathPrefix('/local/app');\nSELECT id FROM `sub/items`"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbview.QueryText(test.one), qt.Not(qt.Equals), ydbview.QueryText(test.other))
		})
	}
}

// The text is the tokens joined by one space, with no semicolon at the end:
// the server's own spelling of a query whose operators are single characters.
func TestQueryText_IsTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbview.QueryText("SELECT  id,\n\tname   FROM `t`  -- c\n;"), qt.Equals, "SELECT id , name FROM `t`")
	c.Assert(ydbview.QueryText("  ;  "), qt.Equals, "")
}
