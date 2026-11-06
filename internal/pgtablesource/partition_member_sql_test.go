package pgtablesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgtablesource"
)

func TestParsePartitionMemberSQL_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, text string
		want       pgtable.PartitionMember
	}{
		{name: "range", text: "PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
			want: pgtable.PartitionMember{Parent: "events", Bound: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"}},
		{name: "list", text: "PARTITION OF regions FOR VALUES IN ('de', 'fr', NULL)",
			want: pgtable.PartitionMember{Parent: "regions", Bound: "FOR VALUES IN ('de', 'fr', NULL)"}},
		{name: "hash", text: "partition of hashed for values with (modulus 4, remainder 1)",
			want: pgtable.PartitionMember{Parent: "hashed", Bound: "for values with (modulus 4, remainder 1)"}},
		{name: "default", text: "PARTITION OF events DEFAULT", want: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}},
		{name: "a parent in another schema", text: "PARTITION OF archive.events DEFAULT",
			want: pgtable.PartitionMember{ParentSchema: "archive", Parent: "events", Bound: "DEFAULT"}},
		{name: "an unquoted parent folds", text: "PARTITION OF Archive.EVENTS DEFAULT",
			want: pgtable.PartitionMember{ParentSchema: "archive", Parent: "events", Bound: "DEFAULT"}},
		{name: "a quoted parent keeps its case and its dot", text: `PARTITION OF "App"."My.Events" DEFAULT`,
			want: pgtable.PartitionMember{ParentSchema: "App", Parent: "My.Events", Bound: "DEFAULT"}},
		{name: "comments and line breaks", text: "PARTITION OF /* parent */ events -- next\n  DEFAULT",
			want: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgtablesource.ParsePartitionMemberSQL(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(value, qt.DeepEquals, &pgtable.DesiredPartitionMember{PartitionMember: test.want})
		})
	}
}

// TestParsePartitionMemberSQL_FailurePath names what each form is. A refusal of a
// column list or a storage clause has to say what was left unread rather than report
// an unexpected token.
func TestParsePartitionMemberSQL_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, text, want string
	}{
		{name: "a column list", text: "PARTITION OF events (id WITH OPTIONS NOT NULL) DEFAULT", want: `.*PARTITION OF with a column or constraint list is not read: .*`},
		{name: "a constraint list", text: "PARTITION OF events (CHECK (id > 0)) DEFAULT", want: `.*PARTITION OF with a column or constraint list is not read: .*`},
		{name: "storage parameters", text: "PARTITION OF events DEFAULT WITH (fillfactor=70)", want: `.*WITH \(fillfactor=70\) after the bound of PARTITION OF is not read: .*`},
		{name: "an access method", text: "PARTITION OF events DEFAULT USING heap", want: `.*USING heap after the bound of PARTITION OF is not read: .*`},
		{name: "no parent", text: "PARTITION OF", want: `.*a partition's parent is a table name, optionally qualified by its schema`},
		{name: "a database-qualified parent", text: "PARTITION OF a.b.c DEFAULT", want: `.*a partition's parent is a table name, optionally qualified by its schema`},
		{name: "no bound", text: "PARTITION OF events", want: `.*expected FOR VALUES or DEFAULT after the parent table`},
		{name: "another keyword", text: "PARTITION BY RANGE (a)", want: `.*expected PARTITION OF`},
		{name: "a second statement", text: "PARTITION OF events DEFAULT; DROP TABLE events", want: `.*unexpected token ;`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgtablesource.ParsePartitionMemberSQL(test.text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(value, qt.IsNil)
		})
	}
}
