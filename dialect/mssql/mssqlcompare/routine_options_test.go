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

// optionsSide is one side of a comparison: the options it holds, none for the
// zero value, and the claim its source makes about them: describes for a
// complete claim, limit for a knowledge limit on the routine with that reason.
type optionsSide struct {
	options   mssqlschema.RoutineOptions
	describes bool
	limit     string
}

// optionsState builds a side's captured state.
func optionsState(c *qt.C, representation schemaext.Representation, side optionsSide) schemaext.FacetState {
	c.Helper()
	state := schemaext.FacetState{}
	var subjects []schemaext.SubjectCoverage
	if side.limit != "" {
		subjects = []schemaext.SubjectCoverage{{Kind: mssqlschema.RoutineOptionsKind, Subject: procedureSubject,
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: side.limit}}}
	}
	if side.describes || side.limit != "" {
		state.Coverage = must.Must(mssqlschema.RoutineOptionsCoverage(representation, schemaext.Knowledge{State: schemaext.Complete}, subjects))
	}
	if !side.options.IsZero() {
		state.Records = []schemaext.FacetRecord{{Subject: procedureSubject, Values: optionsFacets(representation, side.options)}}
	}
	return state
}

// optionsFacets holds options in the representation given.
func optionsFacets(representation schemaext.Representation, options mssqlschema.RoutineOptions) schemaext.Facets {
	if representation == schemaext.Desired {
		return must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, options))
	}
	return must.Must(mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, options))
}

// compareOptions compares one routine both sides hold.
func compareOptions(c *qt.C, desired, current optionsSide) schemaext.FacetComparisonResult {
	c.Helper()
	result, err := mssqlcompare.RoutineOptionsService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
		Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.RoutineOptionsKind},
		Desired: optionsState(c, schemaext.Desired, desired), Current: optionsState(c, schemaext.Observed, current),
		Owners: []schemaext.ParentState{{Subject: procedureSubject, Desired: true, Current: true}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	return result
}

// The comparison plans a change where the two sides state different options,
// including where one states none and its source says so.
func TestRoutineOptionsService_ChangesWhatDiffers(t *testing.T) {
	auditor := mssqlschema.RoutineOptions{Principal: "auditor"}
	for _, test := range []struct {
		name             string
		desired, current mssqlschema.RoutineOptions
		want             []*mssqldiff.RoutineOptions
	}{
		{name: "one principal", desired: auditor, current: mssqlschema.RoutineOptions{Principal: "auditor"}},
		{name: "neither states one"},
		{name: "every option", desired: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true},
			current: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true}},
		{name: "another principal", desired: mssqlschema.RoutineOptions{Principal: "reviewer"}, current: auditor,
			want: []*mssqldiff.RoutineOptions{{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}, After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}}},
		{name: "the owner instead of a principal", current: auditor,
			want: []*mssqldiff.RoutineOptions{{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"}}}},
		{name: "a principal instead of the owner", desired: mssqlschema.RoutineOptions{Principal: "reviewer"},
			want: []*mssqldiff.RoutineOptions{{After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}}},
		{name: "schema binding added", desired: mssqlschema.RoutineOptions{SchemaBinding: true},
			want: []*mssqldiff.RoutineOptions{{After: &mssqlschema.DesiredRoutineOptions{SchemaBinding: true}}}},
		{name: "recompile removed", current: mssqlschema.RoutineOptions{Recompile: true},
			want: []*mssqldiff.RoutineOptions{{Before: &mssqlschema.ObservedRoutineOptions{Recompile: true}}}},
		{name: "the null input rule beside a principal", desired: mssqlschema.RoutineOptions{Principal: "auditor", ReturnsNullOnNullInput: true}, current: auditor,
			want: []*mssqldiff.RoutineOptions{{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor"},
				After: &mssqlschema.DesiredRoutineOptions{Principal: "auditor", ReturnsNullOnNullInput: true}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compareOptions(c, optionsSide{options: test.desired, describes: true}, optionsSide{options: test.current, describes: true})

			var got []*mssqldiff.RoutineOptions
			for _, change := range result.Changes {
				got = append(got, change.Change.Value.(*mssqldiff.RoutineOptions))
			}
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// A declaration whose source cannot state the options, such as a Go
// annotation, leaves them to the routine: no change removes them, and nothing
// is undecided, so a routine nobody changes does not stop a plan.
func TestRoutineOptionsService_KeepsTheOptionsASourceCannotState(t *testing.T) {
	for _, test := range []struct {
		name    string
		current mssqlschema.RoutineOptions
	}{
		{name: "a named principal", current: mssqlschema.RoutineOptions{Principal: "auditor"}},
		{name: "schema binding", current: mssqlschema.RoutineOptions{SchemaBinding: true}},
		{name: "none"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compareOptions(c, optionsSide{}, optionsSide{options: test.current, describes: true})

			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// What cannot be decided is reported with its reason: declared options
// against a read that does not say which ones the routine holds, and a
// knowledge limit either side records for the routine, in that side's words.
func TestRoutineOptionsService_LeavesUndecidedWhatASideCannotSay(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current optionsSide
		wantReason       string
	}{
		{name: "a read that does not describe the options",
			desired: optionsSide{options: mssqlschema.RoutineOptions{Principal: "auditor"}, describes: true},
			current: optionsSide{}, wantReason: "the current source does not say which options the routine holds"},
		{name: "a limit on the current side",
			desired: optionsSide{describes: true},
			current: optionsSide{limit: "the routine is created WITH ENCRYPTION"}, wantReason: "the routine is created WITH ENCRYPTION"},
		{name: "a limit on the desired side",
			desired: optionsSide{limit: "the document could not read the header"},
			current: optionsSide{describes: true}, wantReason: "the document could not read the header"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compareOptions(c, test.desired, test.current)

			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// A routine that one side alone holds is created or dropped with its
// settings, so the facet comparison plans nothing for it.
func TestRoutineOptionsService_LeavesARoutineOneSideHoldsToTheRoutine(t *testing.T) {
	c := qt.New(t)

	result, err := mssqlcompare.RoutineOptionsService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
		Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.RoutineOptionsKind},
		Desired: optionsState(c, schemaext.Desired, optionsSide{options: mssqlschema.RoutineOptions{Principal: "auditor"}, describes: true}),
		Owners:  []schemaext.ParentState{{Subject: procedureSubject, Desired: true}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// The service compares the facet on SQL Server, on a routine, and nothing
// else.
func TestRoutineOptionsService_FailurePath(t *testing.T) {
	table := objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Table("dbo.t")
	for _, test := range []struct {
		name    string
		request schemaext.FacetComparisonRequest
		wantErr string
	}{
		{name: "another target", wantErr: `.*SQL Server routine options comparison on "postgres".*`,
			request: schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{mssqlschema.RoutineOptionsKind}}},
		{name: "another kind", wantErr: `.*unsupported SQL Server routine options comparison kinds.*`,
			request: schemaext.FacetComparisonRequest{Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.SecurityPolicyKind}}},
		{name: "a table owner", wantErr: `.*routine options attach to a routine, not table.*`,
			request: schemaext.FacetComparisonRequest{Target: "sqlserver", Kinds: []schemaext.Kind{mssqlschema.RoutineOptionsKind},
				Owners: []schemaext.ParentState{{Subject: table, Desired: true, Current: true}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mssqlcompare.RoutineOptionsService{}.CompareFacets(c.Context(), test.request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

func TestRoutineOptionsService_UsesCatalogPrincipalEquivalence(t *testing.T) {
	for _, test := range []struct {
		name, currentKey string
		changes          int
	}{
		{"case sensitive", "Auditor", 1}, {"case insensitive", "auditor", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForSQLServerCatalog("test").WithResolvedNames([]identifier.ResolvedName{{Name: "auditor", Key: "auditor"}, {Name: "Auditor", Key: test.currentKey}})
			result, err := mssqlcompare.RoutineOptionsService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
				Target: "sqlserver", Identifiers: semantics, Kinds: []schemaext.Kind{mssqlschema.RoutineOptionsKind},
				Desired: optionsState(c, schemaext.Desired, optionsSide{options: mssqlschema.RoutineOptions{Principal: "auditor"}, describes: true}), Current: optionsState(c, schemaext.Observed, optionsSide{options: mssqlschema.RoutineOptions{Principal: "Auditor"}, describes: true}),
				Owners: []schemaext.ParentState{{Subject: procedureSubject, Desired: true, Current: true}},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}
