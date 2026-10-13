package features_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
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
	return facetRuntimeWithCreations(c, service, schemaprojection.IdentityCreations{})
}

func facetRuntimeWithCreations(c *qt.C, service *facetService, creations schemaprojection.TableCreationService) *engine.Runtime {
	var codecs []schemaext.Codec
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
		codecs = append(codecs, schemaext.Codec{Prototype: &facetValue{}, Representation: representation, Version: 1,
			Definition: json.RawMessage(`{"type":"object","properties":{"Number":{"type":"integer"},"Representation":{"type":"string"}}}`),
			Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*facetValue).Clone(), nil },
			Encode:     encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*facetValue](data) },
		})
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/facets", Targets: []engine.Target{{Name: "postgres", Preparation: schemapreparation.Identity{}, Creations: creations}}, Codecs: codecs,
		Conversions: []engine.Conversion{{Target: "postgres", Kinds: []schemaext.Kind{facetKind}, Service: service}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func facetedSchema(c *qt.C) *schemamodel.Database {
	db := &schemamodel.Database{
		Schemas:           []schemamodel.Schema{{Name: "public"}},
		Tables:            []schemamodel.Table{{Name: "items", StructName: "Item", PrimaryKey: []string{"id"}}},
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
	c.Assert(desired.FacetSlots(), qt.HasLen, 17)
	observed, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 1)
	c.Assert(observed.FacetSlots(), qt.HasLen, 17)
	// A catalog holds the table's primary key among its constraints, so its
	// slots come in another order: each value is still converted once.
	numbers := make([]int, 0, 17)
	for _, facets := range observed.FacetSlots() {
		value, found, err := schemaext.FacetAs[*facetValue](*facets, facetKind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(value.Representation, qt.Equals, schemaext.Observed)
		numbers = append(numbers, value.Number)
	}
	slices.Sort(numbers)
	c.Assert(numbers, qt.DeepEquals, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17})
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

// TestSchemaConversion_CarriesFacetsOfAFoldedPrimaryKey pins that a primary
// key folded into its table keeps its settings: the table carries them for
// its key, converted with the rest.
func TestSchemaConversion_CarriesFacetsOfAFoldedPrimaryKey(t *testing.T) {
	c := qt.New(t)
	service := &facetService{}
	runtime := facetRuntime(c, service)
	facets, err := schemaext.NewFacets(&facetValue{Number: 1, Representation: schemaext.Observed})
	c.Assert(err, qt.IsNil)
	observed := &catalog.Database{Tables: []catalog.Table{{Name: "items"}}, Constraints: []catalog.Constraint{{
		Name: "items_pk", TableName: "items", Type: "PRIMARY KEY", ColumnNames: []string{"id"}, Facets: facets,
	}}}
	result, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observed, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(service.calls, qt.Equals, 1)
	c.Assert(result.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	value, found, err := schemaext.FacetAs[*facetValue](result.Tables[0].PrimaryKeyFacets, facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(*value, qt.Equals, facetValue{Number: 1, Representation: schemaext.Desired})
}

// TestSchemaConversion_RefusesFacetsOnAKeyWithoutColumns refuses a primary
// key no declaration can write, which would lose its settings.
func TestSchemaConversion_RefusesFacetsOnAKeyWithoutColumns(t *testing.T) {
	c := qt.New(t)
	service := &facetService{}
	runtime := facetRuntime(c, service)
	facets, err := schemaext.NewFacets(&facetValue{Number: 1, Representation: schemaext.Observed})
	c.Assert(err, qt.IsNil)
	observed := &catalog.Database{Tables: []catalog.Table{{Name: "items"}}, Constraints: []catalog.Constraint{{
		Name: "items_pk", TableName: "items", Type: "PRIMARY KEY", Facets: facets,
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

type constraintCreations struct{}

func (constraintCreations) ProjectTableCreations(_ context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, input := range request.Tables {
		output := schemaprojection.TableCreation{Subject: input.Subject}
		for _, constraint := range input.Declaration.Constraints {
			subject, _ := schemacapture.ConstraintSubject(input.Subject, constraint.Type, constraint.Name, request.Identifiers)
			output.Facets = append(output.Facets, schemaext.FacetRecord{Subject: subject, Values: must.Must(schemaext.NewFacets(&facetValue{Number: 20, Representation: schemaext.Desired}))})
			output.Observed = append(output.Observed, schemaext.FacetRecord{Subject: subject, Values: must.Must(schemaext.NewFacets(&facetValue{Number: 30, Representation: schemaext.Observed}))})
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}

func TestSchemaConversionProjectsNamedConstraintFacets(t *testing.T) {
	c := qt.New(t)
	runtime := facetRuntimeWithCreations(c, &facetService{}, constraintCreations{})
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Constraints: []schemamodel.Constraint{{Name: "positive", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0",
			Facets: must.Must(schemaext.NewFacets(&facetValue{Number: 10, Representation: schemaext.Desired}))}}}
	models := runtime.Codecs().Definitions()
	model := slices.IndexFunc(models, func(model schemaext.CodecIdentity) bool { return model.Representation == schemaext.Desired })
	desired.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: models[model], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil))
	projected, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(projected.Constraints, qt.HasLen, 1)
	value, found, err := schemaext.FacetAs[*facetValue](projected.Constraints[0].Facets, facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(*value, qt.Equals, facetValue{Number: 30, Representation: schemaext.Observed})
	restored, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), projected, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Constraints, qt.HasLen, 1)
	source, _, err := schemaext.FacetAs[*facetValue](desired.Constraints[0].Facets, facetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(source.Number, qt.Equals, 10)
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).ConstraintParts("", "items", "positive")
	c.Assert(projected.FeatureCoverage.Lookup(facetKind, subject).State, qt.Equals, schemaext.Complete)
}

// Column shorthand cannot carry a constraint's attached owner state.
func TestSchemaConversionKeepsFacetedSingleColumnConstraints(t *testing.T) {
	for _, kind := range []string{"UNIQUE", "FOREIGN KEY"} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			runtime := facetRuntime(c, &facetService{})
			held := must.Must(schemaext.NewFacets(&facetValue{Number: 7, Representation: schemaext.Observed}))
			observed := &catalog.Database{Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: "integer", IsUnique: kind == "UNIQUE"}}}},
				Constraints: []catalog.Constraint{{Name: "items_id_key", TableName: "items", Type: kind, ColumnName: "id", ColumnNames: []string{"id"},
					ForeignTable: new("parent"), ForeignColumn: new("id"), ForeignColumns: []string{"id"}, Facets: held}}}
			restored, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observed, "postgres", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(restored.Constraints, qt.HasLen, 1)
			value, found, err := schemaext.FacetAs[*facetValue](restored.Constraints[0].Facets, facetKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(*value, qt.Equals, facetValue{Number: 7, Representation: schemaext.Desired})
			c.Assert(restored.Fields[0].Foreign, qt.Equals, "")
			roundTrip, err := goschematodb.ToDBSchema(t.Context(), restored, "postgres", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(roundTrip.Constraints, qt.HasLen, 1)
			c.Assert(roundTrip.Constraints[0].Facets.Equal(held), qt.IsTrue)
		})
	}
}
