package generator

// White-box testing required: this boundary consumes accepted provider reversal
// projections before planning and rendering. The fixture tests their captured
// state independently of the planner and renderer a public run must register.

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

const constraintMarkerKind schemaext.Kind = "example.org/constraint-marker"

type constraintMarker struct {
	Number int `json:"number"`
}

func (*constraintMarker) Kind() schemaext.Kind     { return constraintMarkerKind }
func (v *constraintMarker) Clone() schemaext.Value { return new(*v) }
func (v *constraintMarker) Equal(other schemaext.Value) bool {
	w, ok := other.(*constraintMarker)
	return ok && *v == *w
}
func markerFacets(number int) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&constraintMarker{Number: number}))
}
func markerCodecs() schemaext.Registry {
	codec := schemaext.ModelCodec[*constraintMarker]{Prototype: &constraintMarker{}, Representation: schemaext.Observed, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["number"],"additionalProperties":false,"properties":{"number":{"type":"integer"}}}`),
		Shape: func(data json.RawMessage) error {
			_, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "marker", Allowed: []string{"number"}, Required: []string{"number"}})
			return err
		}, Validate: func(*constraintMarker) error { return nil },
	}.Codec()
	return must.Must(schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "example.org/constraints", Codec: codec}))
}

func TestTableReversalProjectsOnlyTheNamedConstraint(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	builder := objectidentity.NewBuilder(semantics)
	current := schemacapture.TableObservation{Table: catalog.Table{Name: "items", Schema: "tenant"}, Constraints: []catalog.Constraint{
		{Name: "first", TableName: "items", Schema: "tenant", Type: "FOREIGN KEY", Facets: markerFacets(1)},
		{Name: "second", TableName: "items", Schema: "tenant", Type: "CHECK", Facets: markerFacets(2)},
	}}
	current.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Observed,
		[]schemaext.KindCoverage{{Model: markerCodecs().Definitions()[0], Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}}, nil))
	recovery := []schemaext.Reversal{{Change: schemaext.ChangeRecord{Subject: builder.ConstraintParts("tenant", "items", "first")},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: constraintMarkerKind, Value: &constraintMarker{Number: 3}}}}}
	projected, err := projectTableFeatures(t.Context(), current, recovery, semantics, markerCodecs())
	c.Assert(err, qt.IsNil)
	c.Assert(projected.Constraints[0].Facets.Equal(markerFacets(3)), qt.IsTrue)
	c.Assert(projected.Constraints[1].Facets.Equal(markerFacets(2)), qt.IsTrue)
	c.Assert(current.Constraints[0].Facets.Equal(markerFacets(1)), qt.IsTrue)
	recovery[0].Change.Subject = builder.ConstraintParts("tenant", "another", "first")
	refused, err := projectTableFeatures(t.Context(), current, recovery, semantics, markerCodecs())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(refused, qt.DeepEquals, schemacapture.TableObservation{})
	current.Constraints[0].TableName = "another"
	refused, err = projectTableFeatures(t.Context(), current, nil, semantics, markerCodecs())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(refused, qt.DeepEquals, schemacapture.TableObservation{})
}
