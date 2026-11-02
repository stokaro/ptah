package routineargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routineargs"
)

// TestSignature_AQuotedBuiltInTypeIsTheTypeTheCatalogReports pins a built-in
// type a declaration names by its quoted typname: the server resolves it, and
// pg_get_function_identity_arguments reports it, as PostgreSQL 18.6 spells
// the type. A quoted name of no built-in type stays as written, and so do the
// grammar aliases, which no quoted name is.
func TestSignature_AQuotedBuiltInTypeIsTheTypeTheCatalogReports(t *testing.T) {
	tests := []struct {
		declared string
		want     string
	}{
		{declared: `a "int4"`, want: "a integer"},
		{declared: `a "varchar"`, want: "a character varying"},
		{declared: `a "bpchar"`, want: "a character"},
		{declared: `a "timestamptz"`, want: "a timestamp with time zone"},
		{declared: `a "char"`, want: `a "char"`},
		{declared: `a "text"`, want: "a text"},
		{declared: `a "_int4"`, want: "a integer[]"},
		{declared: `a "int4"[]`, want: "a integer[]"},
		{declared: `OUT a "int8"`, want: "OUT a bigint"},
		{declared: `a "int"`, want: `a "int"`},
		{declared: `a "Mood"`, want: `a "mood"`},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.Signature(test.declared), qt.Equals, test.want)
		})
	}
}

// TestInputTypes_AQuotedBuiltInTypeIsTheTypeTheCatalogReports is the same
// rule for the input types a GRANT names an overload by.
func TestInputTypes_AQuotedBuiltInTypeIsTheTypeTheCatalogReports(t *testing.T) {
	c := qt.New(t)

	c.Assert(routineargs.InputTypes(`a "int4", b "_varchar"`), qt.Equals, routineargs.InputTypes("a integer, b character varying[]"))
}
