package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/atlasfilter"
)

// timescaleCatalog holds one continuous aggregate in the default schema and
// one in a second schema, so a selector's reach can be asserted rather than
// assumed.
func timescaleCatalog(c *qt.C) *catalog.Database {
	c.Helper()
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public"}, {Name: "app"}},
		Tables:  []catalog.Table{{Name: "readings", Columns: []catalog.Column{{Name: "time"}}}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			tsschema.ObservedContinuousAggregateObject("public", "hourly_totals", tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", HypertableName: "readings"}),
			tsschema.ObservedContinuousAggregateObject("app", "daily_totals", tsschema.ObservedContinuousAggregate{Definition: "SELECT 2", HypertableName: "readings"}),
		)),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}
}

func aggregateNames(c *qt.C, objects schemaext.Objects) []string {
	c.Helper()
	var names []string
	for _, ref := range objects.Refs() {
		names = append(names, tsschema.QualifiedName(ref))
	}
	return names
}

// TestExcludeDatabaseReport_AContinuousAggregateAnswersItsSelector pins that an
// exclude selector naming an aggregate is asked, removes the aggregate, and
// stays a name test: a selector naming nothing is still reported unmatched.
func TestExcludeDatabaseReport_AContinuousAggregateAnswersItsSelector(t *testing.T) {
	tests := []struct {
		name          string
		patterns      []string
		wantUnmatched []string
		wantKept      []string
	}{
		{name: "by name", patterns: []string{"hourly_totals"}, wantKept: []string{"app.daily_totals"}},
		{name: "by type", patterns: []string{"*[type=continuous_aggregate]"}},
		{name: "by schema", patterns: []string{"app"}, wantKept: []string{"public.hourly_totals"}},
		{name: "a name that is not there", patterns: []string{"nosuch_aggregate"}, wantUnmatched: []string{"nosuch_aggregate"}, wantKept: []string{"app.daily_totals", "public.hourly_totals"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			filtered, report, err := atlasfilter.ExcludeDatabaseReport(timescaleCatalog(c), test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.DeepEquals, test.wantUnmatched)
			c.Assert(aggregateNames(c, filtered.FeatureObjects), qt.DeepEquals, test.wantKept)
		})
	}
}

// TestScope_SelectsContinuousAggregatesOnBothSides pins that a schema scope
// narrows the aggregates of a description and of a declaration alike. A
// comparison that narrowed only one side would plan to create or drop the
// aggregates of every schema it was told to leave alone.
func TestScope_SelectsContinuousAggregatesOnBothSides(t *testing.T) {
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			tsschema.DesiredContinuousAggregateObject("public", "hourly_totals", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}),
			tsschema.DesiredContinuousAggregateObject("app", "daily_totals", tsschema.DesiredContinuousAggregate{Body: "SELECT 2"}),
		)),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired)),
	}
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "a schema", scope: atlasfilter.Scope{Schemas: []string{"app"}}, want: []string{"app.daily_totals"}},
		{name: "an include selector", scope: atlasfilter.Scope{Include: []string{"public.hourly_totals"}}, want: []string{"public.hourly_totals"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			described, err := atlasfilter.ScopeDatabase(timescaleCatalog(c), test.scope)
			c.Assert(err, qt.IsNil)
			generated, err := atlasfilter.ScopeGenerated(declared, test.scope)
			c.Assert(err, qt.IsNil)

			c.Assert(aggregateNames(c, described.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(aggregateNames(c, generated.FeatureObjects), qt.DeepEquals, test.want)
		})
	}
}

// TestExcludeDatabase_AHypertableTravelsWithItsTable pins that hypertable
// settings have no selector of their own: they are a facet of the table, so a
// table selector takes them with it and anything else leaves them alone.
func TestExcludeDatabase_AHypertableTravelsWithItsTable(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{name: "the table", patterns: []string{"readings"}},
		{name: "another table", patterns: []string{"other"}, want: []string{"readings"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &catalog.Database{Tables: []catalog.Table{
				{Name: "readings", Columns: []catalog.Column{{Name: "time"}}, Facets: must.Must(schemaext.NewFacets(&tsschema.ObservedHypertable{Column: "time", Dimensions: 1}))},
				{Name: "other"},
			}}

			filtered, err := atlasfilter.ExcludeDatabase(source, test.patterns)

			c.Assert(err, qt.IsNil)
			c.Assert(hypertableTables(c, filtered.Tables), qt.DeepEquals, test.want)
		})
	}
}

// hypertableTables names the tables that carry hypertable settings.
func hypertableTables(c *qt.C, tables []catalog.Table) []string {
	c.Helper()
	var names []string
	for _, table := range tables {
		_, found, err := schemaext.FacetAs[*tsschema.ObservedHypertable](table.Facets, tsschema.HypertableKind)
		c.Assert(err, qt.IsNil)
		if found {
			names = append(names, table.Name)
		}
	}
	return names
}

// appDefaultCatalog is a read of a connection whose own schema is app: the
// reader leaves that schema off what it holds, so the aggregate arrives with no
// schema written.
func appDefaultCatalog(c *qt.C) *catalog.Database {
	c.Helper()
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "public"}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			tsschema.ObservedContinuousAggregateObject("", "daily", tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", HypertableName: "readings"}),
		)),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}
}

// TestFilters_ResolveAnUnqualifiedAggregateUnderTheirOwnDefault pins that an
// aggregate the read left unqualified belongs to the filter's default schema,
// not to the schema its identity was built with. Before this, an identity
// built under PostgreSQL's default answered "public" for it, so a scope of app
// dropped it, an exclude of app.daily missed it, and an exclude of public took
// it.
func TestFilters_ResolveAnUnqualifiedAggregateUnderTheirOwnDefault(t *testing.T) {
	tests := []struct {
		name   string
		filter func(*catalog.Database) (*catalog.Database, error)
		want   []string
	}{
		{name: "a scope of the default schema keeps it", want: []string{"daily"}, filter: func(db *catalog.Database) (*catalog.Database, error) {
			return atlasfilter.ScopeDatabase(db, atlasfilter.Scope{Schemas: []string{"app"}, DefaultSchema: "app"})
		}},
		{name: "a scope of another schema drops it", filter: func(db *catalog.Database) (*catalog.Database, error) {
			return atlasfilter.ScopeDatabase(db, atlasfilter.Scope{Schemas: []string{"public"}, DefaultSchema: "app"})
		}},
		{name: "an exclude of its qualified name takes it", filter: func(db *catalog.Database) (*catalog.Database, error) {
			return atlasfilter.ExcludeDatabaseWithDefaultSchema(db, []string{"app.daily"}, "app")
		}},
		{name: "an exclude of another schema keeps it", want: []string{"daily"}, filter: func(db *catalog.Database) (*catalog.Database, error) {
			return atlasfilter.ExcludeDatabaseWithDefaultSchema(db, []string{"public"}, "app")
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			filtered, err := test.filter(appDefaultCatalog(c))

			c.Assert(err, qt.IsNil)
			c.Assert(aggregateNames(c, filtered.FeatureObjects), qt.DeepEquals, test.want)
		})
	}
}
