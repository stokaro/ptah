package features_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

const facetKind schemaext.Kind = "example.org/facet"

type facetValue struct {
	Number         int
	Representation schemaext.Representation
}

func (*facetValue) Kind() schemaext.Kind { return facetKind }
func (v *facetValue) Clone() schemaext.Value {
	return &facetValue{Number: v.Number, Representation: v.Representation}
}
func (v *facetValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*facetValue)
	return ok && w != nil && *v == *w
}

type facetService struct {
	calls   int
	failure error
}

func (s *facetService) ConvertFeatures(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	s.calls++
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		v := value.(*facetValue)
		result = append(result, &facetValue{Number: v.Number, Representation: request.To})
	}
	return result, s.failure
}

func facetRuntime(c *qt.C, service *facetService) *engine.Runtime {
	var codecs []schemaext.Codec
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
		codecs = append(codecs, schemaext.Codec{Prototype: &facetValue{}, Representation: representation, Version: 1,
			Definition: json.RawMessage(`{"type":"object","properties":{"Number":{"type":"integer"},"Representation":{"type":"string"}}}`),
			Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*facetValue).Clone(), nil },
			Encode:     encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*facetValue](data) },
		})
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/facets", Targets: []engine.Target{{Name: "postgres", Preparation: schemapreparation.Identity{}, Creations: schemaprojection.IdentityCreations{}}}, Codecs: codecs,
		Conversions: []engine.Conversion{{Target: "postgres", Kinds: []schemaext.Kind{facetKind}, Service: service}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func facetedSchema(c *qt.C) *schemamodel.Database {
	db := &schemamodel.Database{
		Schemas:           []schemamodel.Schema{{Name: "public"}},
		Tables:            []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:            []schemamodel.Field{{Name: "id", StructName: "Item", Type: "bigint"}},
		Indexes:           []schemamodel.Index{{Name: "items_id", StructName: "Item", Fields: []string{"id"}}},
		Constraints:       []schemamodel.Constraint{{Name: "positive", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0"}},
		Enums:             []schemamodel.Enum{{Name: "mood", Values: []string{"happy"}}},
		Domains:           []schemamodel.Domain{{Name: "positive_id", BaseType: "bigint"}},
		CompositeTypes:    []schemamodel.CompositeType{{Name: "pair"}},
		Ranges:            []schemamodel.Range{{Name: "ids", Subtype: "bigint"}},
		Functions:         []schemamodel.Function{{Name: "identity_id", Returns: "bigint", Body: "SELECT 1"}},
		Sequences:         []schemamodel.Sequence{{Name: "counter"}},
		Views:             []schemamodel.View{{Name: "ids_view", Body: "SELECT id FROM items"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "ids_cache", Body: "SELECT id FROM items"}},
		Triggers:          []schemamodel.Trigger{{Name: "watch", Table: "items", Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1"}},
		Roles:             []schemamodel.Role{{Name: "app"}},
	}
	for i, destination := range db.FacetSlots() {
		value, err := schemaext.NewFacets(&facetValue{Number: i + 1, Representation: schemaext.Desired})
		c.Assert(err, qt.IsNil)
		*destination = value
	}
	return db
}

func TestSchemaConversion_AllFacetScopesUseOneBatch(t *testing.T) {
	c := qt.New(t)
	service := &facetService{}
	runtime := facetRuntime(c, service)
	desired := facetedSchema(c)
	c.Assert(desired.FacetSlots(), qt.HasLen, 16)
	observed, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 1)
	c.Assert(observed.FacetSlots(), qt.HasLen, 16)
	for i, facets := range observed.FacetSlots() {
		value, found, err := schemaext.FacetAs[*facetValue](*facets, facetKind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(*value, qt.Equals, facetValue{Number: i + 1, Representation: schemaext.Observed})
	}
	restored, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observed, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 2)
	for i, facets := range restored.FacetSlots() {
		value, found, err := schemaext.FacetAs[*facetValue](*facets, facetKind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(*value, qt.Equals, facetValue{Number: i + 1, Representation: schemaext.Desired})
	}
	value, _, err := schemaext.FacetAs[*facetValue](desired.Facets, facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(value.Representation, qt.Equals, schemaext.Desired)
}

func TestSchemaConversion_ProviderFailureReturnsNoSchema(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider connection closed")
	runtime := facetRuntime(c, &facetService{failure: failure})
	desired := facetedSchema(c)
	result, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.IsNil)
	value, _, err := schemaext.FacetAs[*facetValue](desired.Facets, facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(value.Representation, qt.Equals, schemaext.Desired)
}

func TestSchemaConversion_RefusesFacetsOnFoldedPrimaryKey(t *testing.T) {
	c := qt.New(t)
	service := &facetService{}
	runtime := facetRuntime(c, service)
	facets, err := schemaext.NewFacets(&facetValue{Number: 1, Representation: schemaext.Observed})
	c.Assert(err, qt.IsNil)
	observed := &catalog.Database{Tables: []catalog.Table{{Name: "items"}}, Constraints: []catalog.Constraint{{
		Name: "items_pk", TableName: "items", Type: "PRIMARY KEY", ColumnNames: []string{"id"}, Facets: facets,
	}}}
	result, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observed, "postgres", runtime)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 0)
}

func TestSchemaConversionPreservesFacetBindingsAndExclusions(t *testing.T) {
	c := qt.New(t)
	service := &facetService{}
	runtime := facetRuntime(c, service)
	desired := facetedSchema(c)
	for _, slot := range desired.FacetSlots() {
		*slot = must.Must(slot.WithTargetScope(facetKind, "postgres"))
	}
	desired.Tables[0].Facets = must.Must(desired.Tables[0].Facets.WithTargetScope(facetKind, "foreign"))
	projected := must.Must(schemamodel.ScopeToTarget(desired, must.Must(runtime.ResolveTarget("postgres"))))
	observed := must.Must(goschematodb.ToDBSchema(t.Context(), projected, "postgres", runtime))
	restored := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observed, "postgres", runtime))
	c.Assert(restored.FacetSlots(), qt.HasLen, len(projected.FacetSlots()))
	for i, slot := range restored.FacetSlots() {
		source := projected.FacetSlots()[i]
		c.Assert(slot.TargetScope(facetKind), qt.DeepEquals, source.TargetScope(facetKind))
		c.Assert(slot.Kinds(), qt.DeepEquals, source.Kinds())
		c.Assert(slot.DeclaredKinds(), qt.DeepEquals, source.DeclaredKinds())
	}
	c.Assert(restored.Tables[0].Facets.Len(), qt.Equals, 0)
	c.Assert(restored.Tables[0].Facets.IsZero(), qt.IsFalse)
	c.Assert(desired.Tables[0].Facets.Len(), qt.Equals, 1)
}
