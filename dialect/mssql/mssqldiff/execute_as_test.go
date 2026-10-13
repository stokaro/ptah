package mssqldiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// A change names at least one principal, and two that differ; it encodes and
// decodes with a missing side as null, and it always replaces the routine.
func TestExecuteAs_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  *mssqldiff.ExecuteAs
		encoded string
	}{
		{name: "a principal to another", encoded: `{"before":{"principal":"auditor"},"after":{"principal":"reviewer"}}`,
			change: &mssqldiff.ExecuteAs{Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}}},
		{name: "case-sensitive principal", encoded: `{"before":{"principal":"auditor"},"after":{"principal":"Auditor"}}`,
			change: &mssqldiff.ExecuteAs{Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, After: &mssqlschema.DesiredExecuteAs{Principal: "Auditor"}}},
		{name: "a principal to none", encoded: `{"before":{"principal":"auditor"},"after":null}`,
			change: &mssqldiff.ExecuteAs{Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}}},
		{name: "none to a principal", encoded: `{"before":null,"after":{"principal":"reviewer"}}`,
			change: &mssqldiff.ExecuteAs{After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := mssqldiff.ExecuteAsCodec()

			encoded, err := codec.Encode(test.change)
			c.Assert(err, qt.IsNil)
			decoded, err := codec.Decode(encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.encoded)
			c.Assert(decoded, qt.DeepEquals, test.change)
			c.Assert(schemaext.ReplacesOwner(test.change), qt.IsTrue)
		})
	}
}

// A change without a principal, with two that name one principal, or with
// one that names none is refused.
func TestExecuteAs_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  *mssqldiff.ExecuteAs
		wantErr string
	}{
		{name: "no operand", change: &mssqldiff.ExecuteAs{}, wantErr: `.*requires a before or after operand.*`},
		{name: "one principal in two spellings", wantErr: `.*requires two different principals.*`,
			change: &mssqldiff.ExecuteAs{Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, After: &mssqlschema.DesiredExecuteAs{Principal: "auditor "}}},
		{name: "a blank principal", wantErr: `.*must name a database user.*`,
			change: &mssqldiff.ExecuteAs{After: &mssqlschema.DesiredExecuteAs{Principal: " "}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := mssqldiff.ValidateExecuteAs(test.change)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// The reversal swaps the operands across their representations.
func TestExecuteAsReversal(t *testing.T) {
	c := qt.New(t)

	inverse, err := mssqldiff.ExecuteAsReversal(&mssqldiff.ExecuteAs{
		Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}})

	c.Assert(err, qt.IsNil)
	c.Assert(inverse, qt.DeepEquals, &mssqldiff.ExecuteAs{
		Before: &mssqlschema.ObservedExecuteAs{Principal: "reviewer"}, After: &mssqlschema.DesiredExecuteAs{Principal: "auditor"}})
}
