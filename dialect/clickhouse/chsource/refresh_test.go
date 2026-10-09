package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
)

// A declared clause is read into the owner's setting in the spelling the
// server stores, bound to the clickhouse target so no other target treats it
// as part of its view.
func TestRefreshFacetsReadTheClauseTheWayTheServerStoresIt(t *testing.T) {
	for _, test := range []struct {
		name   string
		clause string
		want   chschema.Schedule
	}{
		{"lower case", "every 1 hour", chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}},
		{"an interval the server rewrites", "EVERY 60 MINUTE", chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}},
		{"after", "after 90 second", chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "1 MINUTE 30 SECOND"}},
		{
			"every clause", "every 1 day offset 2 hour randomize for 30 minute depends on other append",
			chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE", DependsOn: []string{"other"}, Append: true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			facets, err := chsource.RefreshFacets(test.clause)
			c.Assert(err, qt.IsNil)
			declared, found, err := schemaext.FacetAs[*chschema.DesiredRefresh](facets, chschema.RefreshKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(declared.Schedule, qt.DeepEquals, test.want)
			c.Assert(facets.TargetScope(chschema.RefreshKind), qt.DeepEquals, []string{"clickhouse"})
		})
	}
}

// An empty clause declares no schedule. Whether that asks for a plain view is
// the source's coverage, which is complete for every view it declares.
func TestRefreshFacetsOfNoClauseDeclareNothing(t *testing.T) {
	c := qt.New(t)
	for _, clause := range []string{"", "  "} {
		facets, err := chsource.RefreshFacets(clause)
		c.Assert(err, qt.IsNil)
		c.Assert(facets.IsZero(), qt.IsTrue)
	}
	coverage := must.Must(chsource.RefreshCoverage())
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "", "totals")
	c.Assert(coverage.Lookup(chschema.RefreshKind, view).State, qt.Equals, schemaext.Complete)
}

// A clause the server would refuse is refused where it is declared, with the
// reason, instead of being sent.
func TestRefreshFacets_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		clause string
		want   string
	}{
		{"not a schedule", "hourly", `.*not a ClickHouse refresh clause.*`},
		{"offset on after", "after 1 hour offset 5 minute", `.*OFFSET.*`},
		{"calendar and clock units mixed", "every 1 month 1 day", `.*interval shouldn't contain both calendar units and clock units`},
		{"a zero interval", "every 0 hour", `.*interval must be positive`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			facets, err := chsource.RefreshFacets(test.clause)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(facets.IsZero(), qt.IsTrue)
		})
	}
}
