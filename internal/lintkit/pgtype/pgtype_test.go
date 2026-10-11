package pgtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintkit/pgtype"
	"ptah.run/internal/lintscan"
)

// A change from one catalog type to another is decided the way PostgreSQL
// 18.6 was measured to apply it.
func TestCompare(t *testing.T) {
	for _, test := range []struct {
		from, to string
		want     pgtype.Outcome
	}{
		{"integer", "integer", pgtype.Unchanged},
		{"character varying(10)", "character varying(20)", pgtype.InPlace},
		{"character varying(20)", "character varying(10)", pgtype.Rewrite},
		{"numeric(10,2)", "numeric(12,2)", pgtype.InPlace},
		{"numeric(10,2)", "numeric(10,3)", pgtype.Rewrite},
		{"integer", "bigint", pgtype.Rewrite},
		{"timestamp without time zone", "timestamp with time zone", pgtype.RewriteUnlessUTC},
		{"integer[]", "bigint[]", pgtype.Rewrite},
		{"mood", "feeling", pgtype.Unknown},
	} {
		t.Run(test.from+" to "+test.to, func(t *testing.T) {
			c := qt.New(t)
			from, ok := pgtype.ParseSpelling(test.from)
			c.Assert(ok, qt.IsTrue)
			to, ok := pgtype.ParseSpelling(test.to)
			c.Assert(ok, qt.IsTrue)
			c.Assert(pgtype.Compare(from, to).Outcome, qt.Equals, test.want)
		})
	}
}

// Every ALTER COLUMN ... TYPE clause is found with the words of its new type,
// whichever spelling it takes.
func TestChangeSites(t *testing.T) {
	c := qt.New(t)
	statements := lintscan.Split("ALTER TABLE t ALTER COLUMN a TYPE bigint, ALTER b SET DATA TYPE varchar(20);\nSELECT 1;\n",
		lintscan.ForDialect("postgres"))

	sites := pgtype.ChangeSites(statements)

	c.Assert(sites, qt.HasLen, 2)
	words := statements[0].Words
	first, ok := pgtype.ParseAt(words, sites[0].TypeStart, sites[0].TypeEnd)
	c.Assert(ok, qt.IsTrue)
	second, ok := pgtype.ParseAt(words, sites[1].TypeStart, sites[1].TypeEnd)
	c.Assert(ok, qt.IsTrue)
	c.Assert([]any{sites[0].Column, sites[0].Table.Normalized, first.Spell(), sites[1].Column, second.Spell()},
		qt.DeepEquals, []any{"a", "T", "bigint", "b", "character varying(20)"})
}
