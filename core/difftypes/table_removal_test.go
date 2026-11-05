package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// TestTableRemovalRetainsItsOwnedSubtree captures the removal of the
// PostgreSQL table public."tenant.orders" from a catalog that also holds
// tenant.orders, a table whose schema and name join to the same text. The
// capture keeps the literal table's children, its row-security policy, its
// table facet and its policy knowledge, and none of the other table's. A
// cloned removal shares no state with the catalog it was captured from or with
// the removal it was cloned from.
func TestTableRemovalRetainsItsOwnedSubtree(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Postgres)
	builder := objectidentity.NewBuilder(semantics)
	parent, nested := builder.TableParts("", "tenant.orders"), builder.TableParts("tenant", "orders")
	policy := pgpolicy.ObservedPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "reader"}},
		Using: new("(id > 0)"), Composition: pgpolicy.Permissive}
	objects := must.Must(schemaext.NewObjects(
		must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("", "tenant.orders", "p_orders"), policy)),
		must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("tenant", "orders", "p_orders"), policy)),
	))
	limited := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "a policy option cannot be reconstructed"}
	coverage := must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: pgpolicy.PolicyKind, Subject: parent, Knowledge: limited},
		{Kind: pgpolicy.PolicyKind, Subject: nested, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not inspected"}},
	}))
	facets := must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true}))
	current := &catalog.Database{
		Tables:         []catalog.Table{{Name: "tenant.orders", Facets: facets, Columns: []catalog.Column{{Name: "id", ColumnDefault: new("7")}}}},
		Indexes:        []catalog.Index{{Name: "literal_index", TableName: "tenant.orders", Columns: []string{"id"}}, {Name: "nested_index", Schema: "tenant", TableName: "orders"}},
		Constraints:    []catalog.Constraint{{Name: "literal_key", TableName: "tenant.orders", Type: "PRIMARY KEY", ColumnNames: []string{"id"}}, {Name: "nested_key", Schema: "tenant", TableName: "orders"}},
		Triggers:       []catalog.Trigger{{Name: "literal_trigger", Table: "tenant.orders"}, {Name: "nested_trigger", Schema: "tenant", Table: "orders"}},
		FeatureObjects: objects, FeatureCoverage: coverage,
	}
	removals := difftypes.TableRemovals{{Name: `"tenant.orders"`, Current: difftypes.TableObservationFor(current, current.Tables[0], platform.Postgres, semantics)}}

	cloned := removals.Clone()
	current.Tables[0].Columns[0].ColumnDefault = new("99")
	current.Indexes[0].Columns[0] = "changed"
	current.Constraints[0].ColumnNames[0] = "changed"
	removals[0].Current.Table.Columns[0].ColumnDefault = new("42")
	removals[0].Current.Indexes[0].Columns[0] = "changed again"
	removals[0].Current.Constraints[0].ColumnNames[0] = "changed again"

	kept := cloned[0].Current
	c.Assert(*kept.Table.Columns[0].ColumnDefault, qt.Equals, "7")
	c.Assert(kept.Indexes, qt.HasLen, 1)
	c.Assert(kept.Indexes[0].Columns, qt.DeepEquals, []string{"id"})
	c.Assert(kept.Constraints, qt.HasLen, 1)
	c.Assert(kept.Constraints[0].ColumnNames, qt.DeepEquals, []string{"id"})
	c.Assert(kept.Triggers, qt.DeepEquals, []catalog.Trigger{{Name: "literal_trigger", Table: "tenant.orders"}})
	c.Assert(kept.Table.Facets.Equal(facets), qt.IsTrue)
	c.Assert(kept.OwnedObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgpolicy.PolicyRef("", "tenant.orders", "p_orders")})
	c.Assert(kept.FeatureCoverage.SubjectRecords(), qt.HasLen, 1)
	c.Assert(kept.FeatureCoverage.Lookup(pgpolicy.PolicyKind, parent), qt.DeepEquals, limited)
}

func TestTableRemovalReportContainsNamesOnly(t *testing.T) {
	for _, test := range []struct {
		name     string
		removals difftypes.TableRemovals
		want     string
	}{
		{name: "absent", want: "null"},
		{name: "empty", removals: difftypes.TableRemovals{}, want: "[]"},
		{name: "ordered names", removals: difftypes.TableRemovals{{Name: "orders"}, {Name: "items"}}, want: `["orders","items"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := json.Marshal(test.removals)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want)
		})
	}
}
