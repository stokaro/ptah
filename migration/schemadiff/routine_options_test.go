package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// principalRoutines returns a declared SQL Server function and the one the
// server holds, each running as principal, empty for none. The declaration's
// source makes no claim about the clause, as a Go annotation cannot;
// [describingPrincipals] gives it the claim a SQL document makes.
func principalRoutines(declared, held, body string) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{Functions: []schemamodel.Function{{
		Name: "dbo.f", Returns: "int", Language: "sql", Security: "DEFINER", Volatility: "VOLATILE", Body: body,
	}}}
	if declared != "" {
		desired.Functions[0].Facets = must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: declared}))
	}
	current := &catalog.Database{Functions: []catalog.Function{{
		Schema: "dbo", Name: "f", Returns: "int", Language: "sql", Security: "DEFINER", Volatility: "VOLATILE", Body: "BEGIN RETURN 1; END",
	}}}
	if held != "" {
		current.Functions[0].Facets = must.Must(mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: held}))
	}
	current.FeatureCoverage = must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return desired, current
}

// describingPrincipals claims that desired's source states the clause, so a
// routine it gives no principal runs as none.
func describingPrincipals(desired *schemamodel.Database) *schemamodel.Database {
	desired.FeatureCoverage = must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return desired
}

// TestCompareWithDialect_ASQLServerRoutinesPrincipalIsCompared pins that the
// principal a SQL Server routine runs as is compared.
//
// The security mode calls a routine that runs as a named user and one that
// runs as its owner both DEFINER, so the principal is compared on its own
// (stokaro/ptah#4490). The change joins the routine's entry, whose
// replacement writes the declared principal.
func TestCompareWithDialect_ASQLServerRoutinesPrincipalIsCompared(t *testing.T) {
	for _, test := range []struct {
		name           string
		declared, held string
		want           []*mssqldiff.RoutineOptions
	}{
		{name: "one principal", declared: "auditor", held: "auditor"},
		{name: "another user", declared: "reviewer", held: "auditor",
			want: []*mssqldiff.RoutineOptions{{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}, After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}}},
		{name: "the owner instead of a user", declared: "", held: "auditor",
			want: []*mssqldiff.RoutineOptions{{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}}}},
		{name: "a user instead of the owner", declared: "reviewer", held: "",
			want: []*mssqldiff.RoutineOptions{{After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := principalRoutines(test.declared, test.held, "BEGIN RETURN 1; END")

			diff, err := schemadiff.CompareWithDialect(t.Context(), describingPrincipals(desired), current, "sqlserver", must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			var got []*mssqldiff.RoutineOptions
			for _, routine := range diff.FunctionsModified {
				c.Assert(routine.Changes, qt.HasLen, 0)
				c.Assert(routine.Desired.Facets.Equal(desired.Functions[0].Facets), qt.IsTrue)
				for _, change := range routine.FeatureChanges {
					got = append(got, change.Value.(*mssqldiff.RoutineOptions))
				}
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A routine the server runs as a user, declared by a source that has no way
// to state a principal, is not replaced: the replacement would run it as its
// owner, so a change to anything else is refused.
func TestCompareWithDialect_ARoutineIsNotReplacedWithoutThePrincipalItRunsAs(t *testing.T) {
	c := qt.New(t)
	desired, current := principalRoutines("", "auditor", "BEGIN RETURN 2; END")

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "sqlserver", must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*function dbo.f is replaced, and it holds "ptah.run/mssql/routine-options", which the declaration's source has no way to state.*`)
	c.Assert(diff, qt.IsNil)
}

// The control for the refusal above: with nothing else changed, the routine
// keeps the principal it runs as, and the comparison is complete.
func TestCompareReportingUndecidedAdditions_ARoutineKeepsAPrincipalItsSourceCannotState(t *testing.T) {
	c := qt.New(t)
	desired, current := principalRoutines("", "auditor", "BEGIN RETURN 1; END")
	opts := config.DefaultCompareOptions()
	opts.Dialect = "sqlserver"

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, current, opts, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
	c.Assert(diagnostics.Empty(), qt.IsTrue)
}

// A routine whose parameters change is paired by name, as a target without
// overloads pairs it, and its replacement is refused the same way: the
// declared signature names no routine the server holds, and the routine of
// that name runs as a user.
func TestCompareWithDialect_ARoutineWithNewParametersIsNotReplacedWithoutItsPrincipal(t *testing.T) {
	c := qt.New(t)
	desired, current := principalRoutines("", "auditor", "BEGIN RETURN 1; END")
	desired.Functions[0].Parameters = "@a int"

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "sqlserver", must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*function dbo.f is replaced, and it holds "ptah.run/mssql/routine-options".*`)
	c.Assert(diff, qt.IsNil)
}
