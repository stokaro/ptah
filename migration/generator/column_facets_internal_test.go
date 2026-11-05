package generator

// White-box testing required: this boundary consumes accepted provider reversal
// projections before planning and rendering. The fixture tests the captured
// column state independently of the planner and renderer a public run must
// register, and no built-in owner keeps a column facet a reversal predicts.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// markedColumns is a table tenant.items holding columns, under complete
// marker knowledge.
func markedColumns(columns ...catalog.Column) schemacapture.TableObservation {
	current := schemacapture.TableObservation{Table: catalog.Table{Name: "items", Schema: "tenant", Columns: columns}}
	current.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Observed,
		[]schemaext.KindCoverage{{Model: markerCodecs().Definitions()[0], Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}}, nil))
	return current
}

// columnRecovery predicts the marker number for the column named column of
// table, in schema tenant.
func columnRecovery(semantics identifier.Semantics, table, column string, number int) []schemaext.Reversal {
	subject := objectidentity.NewBuilder(semantics).ColumnParts("tenant", table, column)
	return []schemaext.Reversal{{Change: schemaext.ChangeRecord{Subject: subject},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: constraintMarkerKind, Value: &constraintMarker{Number: number}}}}}
}

// A reversal's forward state for a column lands on that column alone, and the
// capture it was projected from keeps the facet it held.
func TestTableReversalProjectsOnlyTheNamedColumn(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	current := markedColumns(catalog.Column{Name: "id", Facets: markerFacets(1)}, catalog.Column{Name: "label", Facets: markerFacets(2)})

	projected, err := projectTableFeatures(t.Context(), current, columnRecovery(semantics, "items", "label", 3), semantics, markerCodecs())

	c.Assert(err, qt.IsNil)
	c.Assert(projected.Table.Columns, qt.HasLen, 2)
	c.Assert(projected.Table.Columns[0].Facets.Equal(markerFacets(1)), qt.IsTrue)
	c.Assert(projected.Table.Columns[1].Facets.Equal(markerFacets(3)), qt.IsTrue)
	c.Assert(current.Table.Columns[1].Facets.Equal(markerFacets(2)), qt.IsTrue)
}

func TestTableReversalProjectsAColumn_FailurePath(t *testing.T) {
	semantics := identifier.ForDialect("postgres")
	id, label := catalog.Column{Name: "id", Facets: markerFacets(1)}, catalog.Column{Name: "label", Facets: markerFacets(2)}
	tests := []struct {
		name     string
		columns  []catalog.Column
		recovery []schemaext.Reversal
	}{
		{name: "a column of another table", columns: []catalog.Column{id, label}, recovery: columnRecovery(semantics, "another", "label", 3)},
		{name: "a column the table does not hold", columns: []catalog.Column{id, label}, recovery: columnRecovery(semantics, "items", "missing", 3)},
		{name: "a column captured twice", columns: []catalog.Column{id, label, {Name: "label"}}, recovery: columnRecovery(semantics, "items", "label", 3)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			refused, err := projectTableFeatures(t.Context(), markedColumns(tc.columns...), tc.recovery, semantics, markerCodecs())

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(refused, qt.DeepEquals, schemacapture.TableObservation{})
		})
	}
}
