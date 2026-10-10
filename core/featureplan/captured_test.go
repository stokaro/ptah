package featureplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

func capturedTable() featureplan.Table {
	return featureplan.Table{Subject: objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "events")}
}

func policyFacets() schemaext.Facets {
	return must.Must(schemaext.NewFacets(&crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}}))
}

// TestTableCapturedKinds pins every slot owned state can sit in on a captured
// table, each on its own: the table, a column, an index, a constraint, and a
// coverage record under the table.
func TestTableCapturedKinds(t *testing.T) {
	coverage := must.Must(crdbschema.RowTTLCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: capturedTable().Subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	tests := []struct {
		name    string
		current catalog.Database
	}{
		{name: "a table facet", current: catalog.Database{Tables: []catalog.Table{{Name: "events", Facets: policyFacets()}}}},
		{name: "a column facet", current: catalog.Database{Tables: []catalog.Table{{Name: "events", Columns: []catalog.Column{{Name: "id", Facets: policyFacets()}}}}}},
		{name: "an index facet", current: catalog.Database{Tables: []catalog.Table{{Name: "events"}}, Indexes: []catalog.Index{{Name: "i", TableName: "events", Facets: policyFacets()}}}},
		{name: "a constraint facet", current: catalog.Database{Tables: []catalog.Table{{Name: "events"}}, Constraints: []catalog.Constraint{{Name: "k", TableName: "events", Facets: policyFacets()}}}},
		{name: "a coverage record", current: catalog.Database{Tables: []catalog.Table{{Name: "events"}}, FeatureCoverage: coverage}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := capturedTable()
			table.Current.Table = test.current.Tables[0]
			table.Current.Indexes = test.current.Indexes
			table.Current.Constraints = test.current.Constraints
			table.Current.FeatureCoverage = test.current.FeatureCoverage
			kinds, err := table.CapturedKinds()
			c.Assert(err, qt.IsNil)
			c.Assert(kinds, qt.DeepEquals, []schemaext.Kind{crdbschema.RowTTLKind})
		})
	}
}

// TestTableCapturedKinds_NoneOnAPlainTable is the control: a captured table
// with no feature state anywhere carries no kind.
func TestTableCapturedKinds_NoneOnAPlainTable(t *testing.T) {
	c := qt.New(t)
	table := capturedTable()
	table.Current.Table = catalog.Table{Name: "events", Columns: []catalog.Column{{Name: "id"}}}

	kinds, err := table.CapturedKinds()

	c.Assert(err, qt.IsNil)
	c.Assert(kinds, qt.HasLen, 0)
}
