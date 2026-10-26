package viewcolumns_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/viewcolumns"
)

// TestWithAliases_HappyPath pins the rewrite for the select lists a view body
// actually carries.
//
// A comma inside a call, a string, a quoted identifier or a subquery is not a
// select-item separator, and a FROM inside one of those does not end the list.
// Splitting on either would alias the author's columns under the wrong names,
// which nothing downstream could report.
func TestWithAliases_HappyPath(t *testing.T) {
	rows := []struct {
		name  string
		body  string
		names []string
		want  string
	}{
		{
			name:  "two plain columns",
			body:  "SELECT id, label FROM t",
			names: []string{"ident", "title"},
			want:  "SELECT id AS ident, label AS title FROM t",
		},
		{
			name:  "a call whose arguments carry commas",
			body:  "SELECT coalesce(a, b, c), d FROM t",
			names: []string{"first", "second"},
			want:  "SELECT coalesce(a, b, c) AS first, d AS second FROM t",
		},
		{
			name:  "a string holding a comma and the word from",
			body:  "SELECT 'a, from b' , x FROM t",
			names: []string{"lit", "x"},
			want:  "SELECT 'a, from b' AS lit, x AS x FROM t",
		},
		{
			name:  "a subquery holding its own FROM",
			body:  "SELECT (SELECT max(v) FROM u), y FROM t",
			names: []string{"top", "y"},
			want:  "SELECT (SELECT max(v) FROM u) AS top, y AS y FROM t",
		},
		{
			name:  "no relation at all",
			body:  "SELECT 1, 2",
			names: []string{"one", "two"},
			want:  "SELECT 1 AS one, 2 AS two",
		},
		{
			name:  "SELECT DISTINCT",
			body:  "SELECT DISTINCT a, b FROM t",
			names: []string{"x", "y"},
			want:  "SELECT DISTINCT a AS x, b AS y FROM t",
		},
		{
			name:  "an item that already carries its alias",
			body:  "SELECT id AS ident, label FROM t",
			names: []string{"ident", "title"},
			want:  "SELECT id AS ident, label AS title FROM t",
		},
		{
			name:  "declared names replace explicit aliases",
			body:  "SELECT name AS shown, id FROM t",
			names: []string{"label", "key"},
			want:  "SELECT name AS label, id AS key FROM t",
		},
		{
			name:  "an AS inside a cast does not name the select item",
			body:  "SELECT CAST(id AS text) AS old_name, label FROM t",
			names: []string{"key", "name"},
			want:  "SELECT CAST(id AS text) AS key, label AS name FROM t",
		},
		{
			name:  "quoted aliases with spaces and escaped quotes",
			body:  `SELECT name AS "old label", id AS "old""key" FROM t`,
			names: []string{`"new label"`, `"new key"`},
			want:  `SELECT name AS "new label", id AS "new key" FROM t`,
		},
		{
			name:  "OpenMeter casts and literals already have aliases",
			body:  `SELECT NULL::timestamptz AS "base_intent_deleted_at", 'credit_purchase' AS "type" FROM charge_credit_purchases`,
			names: []string{`"type"`, `"base_intent_deleted_at"`},
			want:  `SELECT NULL::timestamptz AS "type", 'credit_purchase' AS "base_intent_deleted_at" FROM charge_credit_purchases`,
		},
		{
			name:  "a quoted identifier holding a comma",
			body:  `SELECT "a,b", c FROM t`,
			names: []string{"weird", "c"},
			want:  `SELECT "a,b" AS weird, c AS c`,
		},
		{
			name:  "no columns declared leaves the body alone",
			body:  "SELECT id, label FROM t",
			names: nil,
			want:  "SELECT id, label FROM t",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := viewcolumns.WithAliases(row.body, row.names)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Contains, row.want)
		})
	}
}

// TestWithAliases_FailurePath pins that a body the splitter cannot read is
// refused rather than rewritten.
//
// A wrong split gives the author's columns the wrong names, and the view the
// server ends up holding is the one nobody declared. A refusal costs an error
// message; the alternative costs a wrong view nothing reports.
func TestWithAliases_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		body  string
		names []string
	}{
		{name: "fewer items than columns", body: "SELECT a FROM t", names: []string{"x", "y"}},
		{name: "more items than columns", body: "SELECT a, b, c FROM t", names: []string{"x", "y"}},
		{name: "a star nobody can count", body: "SELECT * FROM t", names: []string{"x", "y"}},
		{name: "a lone star given one name", body: "SELECT * FROM t", names: []string{"x"}},
		{name: "a qualified star given one name", body: "SELECT t.* FROM t", names: []string{"x"}},
		{name: "not a select at all", body: "TABLE t", names: []string{"x"}},
		{name: "an empty name", body: "SELECT a FROM t", names: []string{"  "}},
		{name: "unreadable existing alias", body: "SELECT a AS old + extra FROM t", names: []string{"x"}},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := viewcolumns.WithAliases(row.body, row.names)

			c.Assert(err, qt.ErrorIs, viewcolumns.ErrUnreadableSelectList)
			c.Assert(got, qt.Equals, "")
		})
	}
}
