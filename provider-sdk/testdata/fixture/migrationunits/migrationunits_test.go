package migrationunits_test

import (
	"testing"

	"example.com/ptah-fixture-provider/migrationunits"
	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/testing/protocmp"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// A definition is carried into every later unit's text and is no unit of its
// own; a run of data statements is one unit with a checkpoint, and every other
// statement runs outside a transaction.
func TestSplit_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []*protocol.MigrationUnit
	}{
		{
			name: "a definition across a schema statement",
			body: "-- +ptah no_transaction\nCREATE TABLE \"t\" (\"v\" TEXT);\nDEFINE v = 'a;b';\nINSERT INTO \"t\" VALUES ($v);\nINSERT INTO \"t\" VALUES ($v);\nALTER TABLE \"t\" ADD COLUMN \"n\" TEXT;\n",
			want: []*protocol.MigrationUnit{
				{Sql: "CREATE TABLE \"t\" (\"v\" TEXT)", Source: "CREATE TABLE \"t\" (\"v\" TEXT);", Placement: protocol.MigrationUnit_OUTSIDE},
				{
					Sql:       "DEFINE v = 'a;b';\nINSERT INTO \"t\" VALUES ($v);\nINSERT INTO \"t\" VALUES ($v)",
					Source:    "INSERT INTO \"t\" VALUES ($v);\nINSERT INTO \"t\" VALUES ($v);",
					Placement: protocol.MigrationUnit_WITH_CHECKPOINT,
				},
				{Sql: "DEFINE v = 'a;b';\nALTER TABLE \"t\" ADD COLUMN \"n\" TEXT", Source: "ALTER TABLE \"t\" ADD COLUMN \"n\" TEXT;", Placement: protocol.MigrationUnit_OUTSIDE},
			},
		},
		{name: "no statement", body: "-- nothing\n;\n", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			units, refusal := migrationunits.Split(test.body)
			c.Assert(refusal, qt.IsNil)
			c.Assert(units, qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// COMMIT is refused by the byte range of its statement, terminator included.
func TestSplit_FailurePath(t *testing.T) {
	c := qt.New(t)
	body := "INSERT INTO \"t\" VALUES (1);\n  COMMIT ;\n"

	units, refusal := migrationunits.Split(body)

	c.Assert(units, qt.IsNil)
	c.Assert(body[refusal.GetStart():refusal.GetEnd()], qt.Equals, "COMMIT ;")
	c.Assert(refusal.GetReason(), qt.Equals, migrationunits.TransactionControlReason)
}

// An assertion is read-only when it is one SELECT over no external table.
func TestVerdict(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
		want      *protocol.AssertionVerdict
	}{
		{name: "a count", assertion: "SELECT COUNT(*) = 1 FROM \"t\"", want: &protocol.AssertionVerdict{ReadOnly: true}},
		{name: "a write", assertion: "DELETE FROM \"t\"", want: &protocol.AssertionVerdict{Reason: "the assertion is not one SELECT statement"}},
		{
			name: "an external table", assertion: "SELECT COUNT(*) FROM external_feed",
			want: &protocol.AssertionVerdict{Reason: "the assertion reads an external table, whose rows are not in the database"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationunits.Verdict(test.assertion), qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}
