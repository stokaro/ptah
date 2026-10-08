package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/migration/schemadiff/difftypes"
)

type removalFacet struct{ Setting string }

func (*removalFacet) Kind() schemaext.Kind     { return "example.org/removal/table-setting" }
func (v *removalFacet) Clone() schemaext.Value { return &removalFacet{Setting: v.Setting} }
func (v *removalFacet) Equal(other schemaext.Value) bool {
	value, ok := other.(*removalFacet)
	return ok && *v == *value
}

func TestTableRemovalRetainsItsOwnedSubtree(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("ydb")
	parent := objectidentity.NewBuilder(semantics).TableParts("", "tenant.orders")
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	objects, err := schemaext.NewObjects(ydbschema.ObservedObject("", "tenant.orders", stream), ydbschema.ObservedObject("tenant", "orders", stream))
	c.Assert(err, qt.IsNil)
	coverage, err := ydbschema.ChangefeedCoverage(schemaext.Observed, []schemaext.SubjectCoverage{
		{Kind: ydbschema.ChangefeedKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "a child setting cannot be reconstructed"}},
	})
	c.Assert(err, qt.IsNil)
	facets, err := schemaext.NewFacets(&removalFacet{Setting: "preserved"})
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{
		Tables:         []catalog.Table{{Name: "tenant.orders", Facets: facets, Columns: []catalog.Column{{Name: "id", ColumnDefault: new("7")}}}},
		Indexes:        []catalog.Index{{Name: "literal_index", TableName: "tenant.orders", Columns: []string{"id"}}, {Name: "nested_index", Schema: "tenant", TableName: "orders"}},
		Constraints:    []catalog.Constraint{{Name: "literal_key", TableName: "tenant.orders", Type: "PRIMARY KEY", ColumnNames: []string{"id"}}, {Name: "nested_key", Schema: "tenant", TableName: "orders"}},
		Triggers:       []catalog.Trigger{{Name: "literal_trigger", Table: "tenant.orders"}, {Name: "nested_trigger", Schema: "tenant", Table: "orders"}},
		FeatureObjects: objects, FeatureCoverage: coverage,
	}
	removals := difftypes.TableRemovals{{Name: "\"tenant.orders\"", Current: difftypes.TableObservationFor(current, current.Tables[0], "ydb", semantics)}}
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
	c.Assert(kept.OwnedObjects.Refs(), qt.DeepEquals, []objectidentity.ID{ydbschema.ChangefeedRef("", "tenant.orders", "updates")})
	c.Assert(kept.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, parent), qt.DeepEquals, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "a child setting cannot be reconstructed"})
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
