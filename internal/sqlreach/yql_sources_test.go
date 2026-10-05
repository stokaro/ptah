package sqlreach_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlreach"
)

func TestReadYQLSources_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		prefix  string
		sources []string
		selects []string
	}{
		{name: "no source", text: "SELECT 1",
			selects: []string{"SELECT 1"}},
		{name: "a bare name", text: "SELECT COUNT(*) FROM orders", sources: []string{"orders"},
			selects: []string{"SELECT COUNT(*) FROM orders"}},
		{name: "a path", text: "SELECT COUNT(*) FROM `ext/events` WHERE id > 0", sources: []string{"ext/events"},
			selects: []string{"SELECT COUNT(*) FROM `ext/events` WHERE id > 0"}},
		{name: "an absolute path", text: "SELECT * FROM `/local/ext/events`", sources: []string{"/local/ext/events"},
			selects: []string{"SELECT * FROM `/local/ext/events`"}},
		{name: "a join", text: "SELECT * FROM `a` AS a LEFT JOIN `b` AS b ON a.id = b.id", sources: []string{"a", "b"},
			selects: []string{"SELECT * FROM `a` AS a LEFT JOIN `b` AS b ON a.id = b.id"}},
		{name: "a join after ANY", text: "SELECT * FROM ANY `a` AS a JOIN ANY `b` AS b ON a.id = b.id",
			sources: []string{"a", "b"},
			selects: []string{"SELECT * FROM ANY `a` AS a JOIN ANY `b` AS b ON a.id = b.id"}},
		{name: "a list of sources", text: "SELECT * FROM `a`, `b` WHERE a.id = b.id", sources: []string{"a", "b"},
			selects: []string{"SELECT * FROM `a`, `b` WHERE a.id = b.id"}},
		{name: "a source after a join condition and a comma",
			text: "SELECT * FROM `a` AS a JOIN `b` AS b ON a.id = b.id, `c` AS c", sources: []string{"a", "b", "c"},
			selects: []string{"SELECT * FROM `a` AS a JOIN `b` AS b ON a.id = b.id, `c` AS c"}},
		{name: "a subquery", text: "SELECT * FROM (SELECT id FROM `inner`) AS s", sources: []string{"inner"},
			selects: []string{"SELECT * FROM (SELECT id FROM `inner`) AS s"}},
		{name: "a subquery in a condition", text: "SELECT 1 FROM `a` WHERE id IN (SELECT id FROM `b`)",
			sources: []string{"a", "b"},
			selects: []string{"SELECT 1 FROM `a` WHERE id IN (SELECT id FROM `b`)"}},
		{name: "a name written twice", text: "SELECT * FROM `a` UNION ALL SELECT * FROM `a`", sources: []string{"a"},
			selects: []string{"SELECT * FROM `a` UNION ALL SELECT * FROM `a`"}},
		{name: "a secondary index", text: "SELECT * FROM `a` VIEW by_name WHERE name = 'x'", sources: []string{"a"},
			selects: []string{"SELECT * FROM `a` VIEW by_name WHERE name = 'x'"}},
		{name: "a comma in a call after the source list ends",
			text: "SELECT * FROM `a` WHERE Coalesce(x, y) > 0", sources: []string{"a"},
			selects: []string{"SELECT * FROM `a` WHERE Coalesce(x, y) > 0"}},
		{name: "the form a view is stored in, after a pragma",
			text: "PRAGMA TablePathPrefix ( '/local/app' ) ; SELECT * FROM `sub/items`", prefix: "/local/app",
			sources: []string{"sub/items"},
			selects: []string{"SELECT * FROM `sub/items`"}},
		{name: "a pragma assigned", text: `PRAGMA TablePathPrefix = "/local/app"; SELECT * FROM items`,
			prefix: "/local/app", sources: []string{"items"},
			selects: []string{"SELECT * FROM items"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := sqlreach.ReadYQLSources(tc.text)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Prefix, qt.Equals, tc.prefix)
			c.Assert(got.Sources, qt.DeepEquals, tc.sources)
			c.Assert(got.Selects, qt.DeepEquals, tc.selects)
		})
	}
}

func TestReadYQLSources_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "a named expression", text: "SELECT * FROM $t", want: `.*: \$t at a source position is not the name of an object`},
		{name: "a table function", text: "SELECT * FROM AS_TABLE($rows)", want: `.*: AS_TABLE is called at a source position`},
		{name: "a range", text: "SELECT * FROM RANGE(`dir`)", want: `.*: RANGE is called at a source position`},
		{name: "a parenthesized name", text: "SELECT * FROM (`t`)", want: `.*: a parenthesized source is not a subquery`},
		{name: "an external source", text: "SELECT * FROM `s3`.`bucket/path` WITH (FORMAT = \"raw\")",
			want: ".*: `s3` names a cluster or an external source"},
		{name: "a string as a source", text: "SELECT * FROM 'events'", want: `.*: 'events' at a source position is not the name of an object`},
		{name: "a backtick inside a name", text: "SELECT * FROM `a``b`", want: ".*: `a``b` is quoted in a way the proof does not read"},
		{name: "an escape inside a name", text: "SELECT * FROM `a\\`b`", want: ".* is quoted in a way the proof does not read"},
		{name: "another pragma", text: "PRAGMA Library('l'); SELECT * FROM `t`", want: `.*: it carries a pragma other than TablePathPrefix`},
		{name: "a prefix with an escape", text: "PRAGMA TablePathPrefix = '/local/\\x61'; SELECT * FROM `t`",
			want: `.*: it carries a pragma other than TablePathPrefix`},
		{name: "a write", text: "SELECT 1; DELETE FROM `t`", want: `the objects a YDB query reads cannot be named from its text: a YDB statement is proved read-only only when it is one SELECT`},
		{name: "a source hidden behind a number", text: "SELECT 1uFROM `t`", want: `.*is written against the number 1u`},
		{name: "nothing after FROM", text: "SELECT * FROM", want: `.*: a source position holds nothing`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := sqlreach.ReadYQLSources(tc.text)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(got, qt.DeepEquals, sqlreach.YQLReads{})
		})
	}
}
