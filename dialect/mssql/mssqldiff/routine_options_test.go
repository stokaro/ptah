package mssqldiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// A change carries at least one set of options, and two that differ; it
// encodes and decodes with a missing side as null, and it always replaces the
// routine.
func TestRoutineOptions_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  *mssqldiff.RoutineOptions
		encoded string
	}{
		{name: "a principal to another", encoded: `{"before":{"principal":"auditor"},"after":{"principal":"reviewer"}}`,
			change: &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}, After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}},
		{name: "case-sensitive principal", encoded: `{"before":{"principal":"auditor"},"after":{"principal":"Auditor"}}`,
			change: &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}, After: &mssqlschema.DesiredRoutineOptions{Principal: "Auditor"}}},
		{name: "a principal to none", encoded: `{"before":{"principal":"auditor"},"after":null}`,
			change: &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}}},
		{name: "none to a principal", encoded: `{"before":null,"after":{"principal":"reviewer"}}`,
			change: &mssqldiff.RoutineOptions{After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}},
		{name: "schema binding added beside a principal", encoded: `{"before":{"principal":"auditor"},"after":{"principal":"auditor","schema_binding":true}}`,
			change: &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"},
				After: &mssqlschema.DesiredRoutineOptions{Principal: "auditor", SchemaBinding: true}}},
		{name: "recompile removed", encoded: `{"before":{"recompile":true},"after":null}`,
			change: &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Recompile: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := mssqldiff.RoutineOptionsCodec()

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

// A change without an operand, with two that state the same options, or with
// a principal that names no user is refused.
func TestRoutineOptions_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  *mssqldiff.RoutineOptions
		wantErr string
	}{
		{name: "no operand", change: &mssqldiff.RoutineOptions{}, wantErr: `.*requires a before or after operand.*`},
		{name: "one principal in two spellings", wantErr: `.*requires two different sets of options.*`,
			change: &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}, After: &mssqlschema.DesiredRoutineOptions{Principal: "auditor "}}},
		{name: "a blank principal", wantErr: `.*must name a database user.*`,
			change: &mssqldiff.RoutineOptions{After: &mssqlschema.DesiredRoutineOptions{Principal: " "}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := mssqldiff.ValidateRoutineOptions(test.change)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// The reversal swaps the operands across their representations.
func TestRoutineOptionsReversal(t *testing.T) {
	c := qt.New(t)

	inverse, err := mssqldiff.RoutineOptionsReversal(&mssqldiff.RoutineOptions{
		Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor", Recompile: true},
		After:  &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}})

	c.Assert(err, qt.IsNil)
	c.Assert(inverse, qt.DeepEquals, &mssqldiff.RoutineOptions{
		Before: &mssqlschema.ObservedRoutineOptions{Principal: "reviewer"},
		After:  &mssqlschema.DesiredRoutineOptions{Principal: "auditor", Recompile: true}})
}
