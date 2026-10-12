package mssqlcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlcompare"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// procedureSubject is the routine every request here compares.
var procedureSubject = objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Procedure("dbo.audit_it", "")

// executeAsSide is one side of a comparison: the principal it holds, empty
// for none, and whether its source claims to describe the clause.
type executeAsSide struct {
	principal string
	describes bool
}

// executeAsState builds a side's captured state.
func executeAsState(c *qt.C, representation schemaext.Representation, side executeAsSide) schemaext.FacetState {
	c.Helper()
	state := schemaext.FacetState{}
	if side.describes {
		state.Coverage = must.Must(mssqlschema.ExecuteAsCoverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil))
	}
	facets := schemaext.Facets{}
	switch {
	case side.principal == "":
	case representation == schemaext.Desired:
		facets = must.Must(mssqlschema.WithExecuteAs(facets, side.principal))
	default:
		facets = must.Must(mssqlschema.WithObservedExecuteAs(facets, side.principal))
	}
	if !facets.IsZero() {
		state.Records = []schemaext.FacetRecord{{Subject: procedureSubject, Values: facets}}
	}
	return state
}

// compareExecuteAs compares one routine both sides hold.
func compareExecuteAs(c *qt.C, desired, current executeAsSide) schemaext.FacetComparisonResult {
	c.Helper()
	result, err := mssqlcompare.ExecuteAsService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
		Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.ExecuteAsKind},
		Desired: executeAsState(c, schemaext.Desired, desired), Current: executeAsState(c, schemaext.Observed, current),
		Owners: []schemaext.ParentState{{Subject: procedureSubject, Desired: true, Current: true}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	return result
}

// The comparison plans a change where the two sides name different
// principals, including where one names none and its source says so.
func TestExecuteAsService_ChangesWhatDiffers(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current executeAsSide
		want             []*mssqldiff.ExecuteAs
	}{
		{name: "one principal", desired: executeAsSide{"auditor", true}, current: executeAsSide{"auditor", true}},
		{name: "neither names one", desired: executeAsSide{"", true}, current: executeAsSide{"", true}},
		{name: "another principal", desired: executeAsSide{"reviewer", true}, current: executeAsSide{"auditor", true},
			want: []*mssqldiff.ExecuteAs{{Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}}}},
		{name: "the owner instead of a principal", desired: executeAsSide{"", true}, current: executeAsSide{"auditor", true},
			want: []*mssqldiff.ExecuteAs{{Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}}}},
		{name: "a principal instead of the owner", desired: executeAsSide{"reviewer", true}, current: executeAsSide{"", true},
			want: []*mssqldiff.ExecuteAs{{After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compareExecuteAs(c, test.desired, test.current)

			var got []*mssqldiff.ExecuteAs
			for _, change := range result.Changes {
				got = append(got, change.Change.Value.(*mssqldiff.ExecuteAs))
			}
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// A declaration whose source cannot state the clause, such as a Go
// annotation, leaves the principal to the routine: no change removes it, and
// nothing is undecided, so a routine nobody changes does not stop a plan.
func TestExecuteAsService_KeepsThePrincipalASourceCannotState(t *testing.T) {
	for _, test := range []struct {
		name    string
		current executeAsSide
	}{
		{name: "a named principal", current: executeAsSide{"auditor", true}},
		{name: "none", current: executeAsSide{"", true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compareExecuteAs(c, executeAsSide{"", false}, test.current)

			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// A declared principal against a read that does not say which one the routine
// runs as is undecided: the comparison can neither plan a change nor call the
// two equal.
func TestExecuteAsService_LeavesUndecidedAPrincipalTheReadDoesNotDescribe(t *testing.T) {
	c := qt.New(t)

	result := compareExecuteAs(c, executeAsSide{"auditor", true}, executeAsSide{"", false})

	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "the current source does not say which principal the routine runs as")
}

// A routine that one side alone holds is created or dropped with its
// settings, so the facet comparison plans nothing for it.
func TestExecuteAsService_LeavesARoutineOneSideHoldsToTheRoutine(t *testing.T) {
	c := qt.New(t)

	result, err := mssqlcompare.ExecuteAsService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
		Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.ExecuteAsKind},
		Desired: executeAsState(c, schemaext.Desired, executeAsSide{"auditor", true}),
		Owners:  []schemaext.ParentState{{Subject: procedureSubject, Desired: true}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// The service compares the facet on SQL Server, on a routine, and nothing
// else.
func TestExecuteAsService_FailurePath(t *testing.T) {
	table := objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Table("dbo.t")
	for _, test := range []struct {
		name    string
		request schemaext.FacetComparisonRequest
		wantErr string
	}{
		{name: "another target", wantErr: `.*SQL Server EXECUTE AS comparison on "postgres".*`,
			request: schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{mssqlschema.ExecuteAsKind}}},
		{name: "another kind", wantErr: `.*unsupported SQL Server EXECUTE AS comparison kinds.*`,
			request: schemaext.FacetComparisonRequest{Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.SecurityPolicyKind}}},
		{name: "a table owner", wantErr: `.*an EXECUTE AS principal attaches to a routine, not table.*`,
			request: schemaext.FacetComparisonRequest{Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.ExecuteAsKind},
				Owners: []schemaext.ParentState{{Subject: table, Desired: true, Current: true}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mssqlcompare.ExecuteAsService{}.CompareFacets(c.Context(), test.request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

// Case-sensitive catalogs distinguish users that differ only by case.
func TestExecuteAsService_UsesCatalogPrincipalEquivalence(t *testing.T) {
	for _, test := range []struct {
		name, currentKey string
		changes          int
	}{
		{"case sensitive", "Auditor", 1}, {"case insensitive", "auditor", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForSQLServerCatalog("test").WithResolvedNames([]identifier.ResolvedName{{Name: "auditor", Key: "auditor"}, {Name: "Auditor", Key: test.currentKey}})
			result, err := mssqlcompare.ExecuteAsService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
				Target: "sqlserver", Identifiers: semantics, Kinds: []schemaext.Kind{mssqlschema.ExecuteAsKind},
				Desired: executeAsState(c, schemaext.Desired, executeAsSide{"auditor", true}), Current: executeAsState(c, schemaext.Observed, executeAsSide{"Auditor", true}),
				Owners: []schemaext.ParentState{{Subject: procedureSubject, Desired: true, Current: true}},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}
