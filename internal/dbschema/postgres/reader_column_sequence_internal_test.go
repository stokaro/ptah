package postgres

// White-box testing required: sequenceInSchema and keepColumnSequencesOnly
// decide which sequence a read column owns, between two catalog reads, and
// nothing exported shows the answer apart from the whole read a fake server
// would have to answer statement by statement.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// TestSequenceInSchema_HappyPath reads back what pg_get_serial_sequence
// answered on PostgreSQL 18.6 for a column of the schema it was asked about:
// it quotes a part as quote_ident does, doubling a quote inside it.
func TestSequenceInSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		reported string
		schema   string
		want     string
	}{
		{name: "plain names", reported: "public.items_id_seq", schema: "public", want: "items_id_seq"},
		{name: "quoted names", reported: `"App"."Items_Id_seq"`, schema: "App", want: "Items_Id_seq"},
		{name: "a quote inside a name", reported: `public."say""hi_id_seq"`, schema: "public", want: `say"hi_id_seq`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sequenceInSchema(test.reported, test.schema), qt.Equals, test.want)
		})
	}
}

// TestSequenceInSchema_FailurePath answers no sequence where the column owns
// none, and where the sequence it owns was moved to another schema.
func TestSequenceInSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		reported string
		schema   string
	}{
		{name: "no sequence", reported: "", schema: "public"},
		{name: "another schema", reported: "other.items_id_seq", schema: "public"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sequenceInSchema(test.reported, test.schema), qt.Equals, "")
		})
	}
}

// TestKeepColumnSequencesOnly clears the owner of a sequence the read
// describes on its own, and keeps the one that is part of its column.
func TestKeepColumnSequencesOnly(t *testing.T) {
	c := qt.New(t)
	tables := []catalog.Table{{Schema: "app", Name: "items", Columns: []catalog.Column{
		{Name: "id", OwnedSequence: "items_id_seq"},
		{Name: "n", OwnedSequence: "lifecycle_seq"},
		{Name: "title"},
	}}}

	keepColumnSequencesOnly(tables, []catalog.Sequence{{Schema: "app", Name: "lifecycle_seq", OwnedBy: "items.n"}})

	c.Assert(tables[0].Columns[0].OwnedSequence, qt.Equals, "items_id_seq")
	c.Assert(tables[0].Columns[1].OwnedSequence, qt.Equals, "")
	c.Assert(tables[0].Columns[2].OwnedSequence, qt.Equals, "")
}

// TestKeepColumnSequencesOnly_ADescribedSequenceIsAnOrdinaryDefault reads the flag
// the comparison skips a default by. A column that draws from a sequence described
// on its own holds an ordinary default. CockroachDB reports such a sequence through
// pg_get_serial_sequence as well, measured on v26.3.2, so the read of the sequences
// is what tells the two apart.
func TestKeepColumnSequencesOnly_ADescribedSequenceIsAnOrdinaryDefault(t *testing.T) {
	c := qt.New(t)
	tables := []catalog.Table{{Schema: "app", Name: "items", Columns: []catalog.Column{
		{Name: "id", OwnedSequence: "items_id_seq", IsAutoIncrement: true},
		{Name: "n", OwnedSequence: "lifecycle_seq", IsAutoIncrement: true},
	}}}

	keepColumnSequencesOnly(tables, []catalog.Sequence{{Schema: "app", Name: "lifecycle_seq"}})

	c.Assert(tables[0].Columns[0].IsAutoIncrement, qt.IsTrue)
	c.Assert(tables[0].Columns[1].IsAutoIncrement, qt.IsFalse)
}

// TestCockroachSequenceDefault_SpellsTheDefaultAsPostgreSQLDoes pins the spelling a
// declared default compares against, and that nothing else changes.
func TestCockroachSequenceDefault_SpellsTheDefaultAsPostgreSQLDoes(t *testing.T) {
	tests := []struct {
		name, dialect, read, want string
	}{
		{name: "the connection's own schema", dialect: platform.CockroachDB, read: "nextval('public.s'::REGCLASS)", want: "nextval('s'::regclass)"},
		{name: "another schema", dialect: platform.CockroachDB, read: "nextval('other.s'::REGCLASS)", want: "nextval('other.s'::regclass)"},
		{name: "no schema", dialect: platform.CockroachDB, read: "nextval('s'::REGCLASS)", want: "nextval('s'::regclass)"},
		{name: "a name that needs quoting", dialect: platform.CockroachDB, read: `nextval('public."S x"'::REGCLASS)`, want: `nextval('public."S x"'::REGCLASS)`},
		{name: "another default", dialect: platform.CockroachDB, read: "unique_rowid()", want: "unique_rowid()"},
		{name: "PostgreSQL", dialect: platform.Postgres, read: "nextval('public.s'::regclass)", want: "nextval('public.s'::regclass)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := &Reader{dialect: test.dialect, schema: "public"}
			c.Assert(*reader.cockroachSequenceDefault(new(test.read)), qt.Equals, test.want)
		})
	}
}

// TestCockroachSequenceDefault_NoDefault keeps a column with no default without one.
func TestCockroachSequenceDefault_NoDefault(t *testing.T) {
	c := qt.New(t)
	reader := &Reader{dialect: platform.CockroachDB, schema: "public"}
	c.Assert(reader.cockroachSequenceDefault(nil), qt.IsNil)
}
