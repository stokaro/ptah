package postgres

// White-box testing required: sequenceInSchema and keepColumnSequencesOnly
// decide which sequence a read column owns, between two catalog reads, and
// nothing exported shows the answer apart from the whole read a fake server
// would have to answer statement by statement.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
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
