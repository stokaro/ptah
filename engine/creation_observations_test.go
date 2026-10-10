package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
	"ptah.run/internal/convert/goschematodb"
)

// observationProvider is a target whose CREATE leaves an observation of its
// model on the table and on index by.id that a conversion of the declared
// values does not give: the conversion doubles a number, the CREATE says 7.
func observationProvider(predict creationFunc) engine.Provider {
	provider := creationProvider(predict)
	provider.Codecs = append(provider.Codecs, conversionCodec(conversionFirst, schemaext.Observed))
	provider.Conversions = []engine.Conversion{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Service: conversionFunc(
		func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
			converted := make([]schemaext.Value, len(request.Values))
			for i, value := range request.Values {
				converted[i] = &conversionValue{ID: conversionFirst, Number: 2 * value.(*conversionValue).Number}
			}
			return converted, nil
		})}}
	return provider
}

func predictObservations(_ context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	table := request.Tables[0].Subject
	index := objectidentity.NewBuilder(request.Identifiers).IndexParts(table.Schema.Source, table.Name.Source, "by.id")
	observed := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 7}))
	return schemaprojection.TableCreationResult{Complete: true, Tables: []schemaprojection.TableCreation{{
		Subject:  table,
		Observed: []schemaext.FacetRecord{{Subject: table, Values: observed}, {Subject: index, Values: observed}},
	}}}, nil
}

// A converted document holds what its CREATE leaves where that is not what
// its declared values convert to. The table's declared value is replaced by
// the observation rather than converted, and the index, which declares none,
// gains one bound to the target. The rule is the contract's, not an owner's:
// this target and its model are invented for the test.
func TestCreationObservationsReplaceTheConversion(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event",
			Facets: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 1}))}},
		Fields:  []schemamodel.Field{{Name: "id", StructName: "Event", Type: "bigint"}},
		Indexes: []schemamodel.Index{{Name: "by.id", StructName: "Event", Fields: []string{"id"}}},
	}

	result, err := goschematodb.ToDBSchema(t.Context(), source, "alternate", mustRuntime(c, observationProvider(predictObservations)))

	c.Assert(err, qt.IsNil)
	table, found, err := schemaext.FacetAs[*conversionValue](result.Tables[0].Facets, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(table.Number, qt.Equals, 7)
	index, found, err := schemaext.FacetAs[*conversionValue](result.Indexes[0].Facets, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(index.Number, qt.Equals, 7)
	c.Assert(result.Indexes[0].Facets.TargetScope(conversionFirst), qt.DeepEquals, []string{"custom"})
	c.Assert(source.Indexes[0].Facets.IsZero(), qt.IsTrue)
}

// Without an observation, the declared value is converted as before.
func TestCreationObservationsLeaveOtherValuesConverted(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event",
			Facets: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 1}))}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "bigint"}},
	}
	none := creationFunc(func(_ context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		return schemaprojection.TableCreationResult{Complete: true, Tables: []schemaprojection.TableCreation{{Subject: request.Tables[0].Subject}}}, nil
	})

	result, err := goschematodb.ToDBSchema(t.Context(), source, "alternate", mustRuntime(c, observationProvider(none)))

	c.Assert(err, qt.IsNil)
	table, _, err := schemaext.FacetAs[*conversionValue](result.Tables[0].Facets, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(table.Number, qt.Equals, 2)
}

// An observation must name a declared owner once and hold values without a
// binding.
func TestCreationObservationsRefuseInvalidRecords(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemaprojection.TableCreationResult)
	}{
		{"an index the table does not declare", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Observed[1].Subject.Name.Source, r.Tables[0].Observed[1].Subject.Name.Normalized = "absent", "absent"
		}},
		{"a repeated owner", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Observed = append(r.Tables[0].Observed, r.Tables[0].Observed[0])
		}},
		{"empty values", func(r *schemaprojection.TableCreationResult) { r.Tables[0].Observed[0].Values = schemaext.Facets{} }},
		{"a binding", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Observed[0].Values = must.Must(r.Tables[0].Observed[0].Values.WithTargetScope(conversionFirst, "custom"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := creationFunc(func(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
				result := must.Must(predictObservations(ctx, request))
				test.change(&result)
				return result, nil
			})

			result, err := mustRuntime(c, observationProvider(service)).ProjectTableCreations(t.Context(), indexCreationRequest())

			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
			c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
		})
	}
}

// An observation stands in for a conversion, so it must be of a model the
// target converts.
func TestCreationObservationsRefuseAModelTheTargetDoesNotConvert(t *testing.T) {
	c := qt.New(t)
	provider := observationProvider(predictObservations)
	provider.Conversions = nil

	result, err := mustRuntime(c, provider).ProjectTableCreations(t.Context(), indexCreationRequest())

	c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
	c.Assert(err, qt.ErrorMatches, `.*creation projection returned an observation of a model the target does not convert: "example.org/first"`)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}
