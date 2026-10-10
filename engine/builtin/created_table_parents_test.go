package builtin_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
)

// TestBundledOwnersAccountForACreatedTable pins every bundled owner that
// assesses a PostgreSQL-family table: asked about a table the plan creates,
// each answers with a receipt and no step, because the table's CREATE TABLE
// carries the setting it owns. The receipts are compared in sorted order,
// which is not part of the contract.
func TestBundledOwnersAccountForACreatedTable(t *testing.T) {
	tests := []struct {
		name   string
		target string
		facet  schemaext.Value
		want   []string
	}{
		{name: "a hypertable", target: "postgres", facet: &tsschema.DesiredHypertable{Column: "at"}, want: []string{
			"ptah.run/timescaledb/hypertable: partition the table with the create_hypertable call that follows its CREATE TABLE",
		}},
		{name: "a row-level TTL", target: "cockroachdb", facet: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "at"}}, want: []string{
			"ptah.run/cockroachdb/row-ttl: create the row-level TTL with the table; its CREATE TABLE carries the storage parameters",
			"ptah.run/timescaledb/hypertable: partition the table with the create_hypertable call that follows its CREATE TABLE",
		}},
		{name: "a row deletion policy", target: "spanner", facet: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "at", Interval: "30 days"}}, want: []string{
			"ptah.run/spanner/row-deletion-policy: create the row deletion policy with the table; its CREATE TABLE carries the policy",
			"ptah.run/timescaledb/hypertable: partition the table with the create_hypertable call that follows its CREATE TABLE",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(test.target)
			table := schemamodel.Table{StructName: "Event", Name: "events", Facets: must.Must(schemaext.NewFacets(test.facet))}

			result, err := must.Must(builtin.New()).PlanFeatures(t.Context(), featureplan.Request{Target: test.target, Identifiers: semantics,
				Tables: []featureplan.Table{{Subject: objectidentity.NewBuilder(semantics).TableParts("", "events"), Action: featureplan.CreateTable,
					Desired: schemacapture.TableDeclaration{Table: table}}}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			var receipts []string
			for _, parent := range result.Parents {
				receipts = append(receipts, fmt.Sprintf("%s: %s", parent.Kind, parent.Strategy))
				c.Assert(parent.Steps, qt.HasLen, 0)
			}
			slices.Sort(receipts)
			c.Assert(receipts, qt.DeepEquals, test.want)
		})
	}
}
