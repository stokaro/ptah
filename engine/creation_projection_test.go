package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
	"ptah.run/internal/convert/goschematodb"
)

type creationFunc func(context.Context, schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error)

func (f creationFunc) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	return f(ctx, request)
}

func creationProvider(service schemaprojection.TableCreationService) engine.Provider {
	return engine.Provider{ID: "example.org/creation", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Creations: service}},
		Codecs: []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired)},
	}
}

func creationRequest() schemaprojection.TableCreationRequest {
	semantics := identifier.ForDialect("custom")
	return schemaprojection.TableCreationRequest{Target: "alternate", Identifiers: semantics, Tables: []schemaprojection.TableCreationInput{{
		Subject: objectidentity.NewBuilder(semantics).TableParts("", "events"),
		Declaration: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", StructName: "Event"},
			Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "bigint"}},
		},
	}}}
}

func predictCreation(_ context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	return schemaprojection.TableCreationResult{Complete: true, Tables: []schemaprojection.TableCreation{{
		Subject:           request.Tables[0].Subject,
		Facets:            []schemaext.FacetRecord{{Subject: request.Tables[0].Subject, Values: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 2}))}},
		ColumnPrimaryKeys: []string{"id"}, ColumnPrimaryKeysPrepared: true,
	}}}, nil
}

func TestCreationProjectionSeparatesDefaultsFromSource(t *testing.T) {
	c := qt.New(t)
	var received schemaprojection.TableCreationRequest
	var reply schemaprojection.TableCreationResult
	service := creationFunc(func(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		received = request
		reply = must.Must(predictCreation(ctx, request))
		request.Tables[0].Declaration.Fields[0].Name = "mutated by provider"
		return reply, nil
	})
	provider := creationProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Targets[0].Creations = nil
	request := creationRequest()
	result, err := runtime.ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(result.Tables[0].ColumnPrimaryKeys, qt.DeepEquals, []string{"id"})
	value, found, err := schemaext.FacetAs[*conversionValue](result.Tables[0].Facets[0].Values, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Number, qt.Equals, 2)
	c.Assert(request.Tables[0].Declaration.Table.Facets.IsZero(), qt.IsTrue)
	c.Assert(request.Tables[0].Declaration.Fields[0].Name, qt.Equals, "id")
	reply.Tables[0].ColumnPrimaryKeys[0] = "mutated after return"
	c.Assert(result.Tables[0].ColumnPrimaryKeys, qt.DeepEquals, []string{"id"})
}

func TestCreationProjectionRejectsMalformedReplies(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemaprojection.TableCreationResult)
	}{
		{"incomplete", func(r *schemaprojection.TableCreationResult) { r.Complete = false }},
		{"missing table", func(r *schemaprojection.TableCreationResult) { r.Tables = nil }},
		{"repeated table", func(r *schemaprojection.TableCreationResult) { r.Tables = append(r.Tables, r.Tables[0]) }},
		{"changed identity spelling", func(r *schemaprojection.TableCreationResult) { r.Tables[0].Subject.Name.Source = "other" }},
		{"unknown key column", func(r *schemaprojection.TableCreationResult) { r.Tables[0].ColumnPrimaryKeys = []string{"absent"} }},
		{"duplicate key column", func(r *schemaprojection.TableCreationResult) { r.Tables[0].ColumnPrimaryKeys = []string{"id", "id"} }},
		{"missing key receipt", func(r *schemaprojection.TableCreationResult) { r.Tables[0].ColumnPrimaryKeysPrepared = false }},
		{"provider binding", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Facets[0].Values = must.Must(r.Tables[0].Facets[0].Values.WithTargetScope(conversionFirst, "custom"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := creationFunc(func(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
				result := must.Must(predictCreation(ctx, request))
				test.change(&result)
				return result, nil
			})
			result, err := mustRuntime(c, creationProvider(service)).ProjectTableCreations(t.Context(), creationRequest())
			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
			c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
		})
	}
}

func TestCreationProjectionCannotRestoreExcludedModels(t *testing.T) {
	c := qt.New(t)
	request := creationRequest()
	facets := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 1}))
	facets = must.Must(facets.WithTargetScope(conversionFirst, "other"))
	request.Tables[0].Declaration.Table.Facets = must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("custom"))))
	result, err := mustRuntime(c, creationProvider(creationFunc(predictCreation))).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionEnforcesModelOwnership(t *testing.T) {
	c := qt.New(t)
	provider := creationProvider(creationFunc(predictCreation))
	foreign := engine.Provider{ID: "example.org/foreign", Codecs: provider.Codecs}
	provider.Codecs = nil
	result, err := mustRuntime(c, provider, foreign).ProjectTableCreations(t.Context(), creationRequest())
	c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionValidatesComputedValues(t *testing.T) {
	c := qt.New(t)
	provider := creationProvider(creationFunc(predictCreation))
	provider.Codecs[0].Clone = cloneValidPreparedValue
	result, err := mustRuntime(c, provider).ProjectTableCreations(t.Context(), creationRequest())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionProviderFailureDiscardsPartialResult(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider failed")
	service := creationFunc(func(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		return must.Must(predictCreation(ctx, request)), failure
	})
	result, err := mustRuntime(c, creationProvider(service)).ProjectTableCreations(t.Context(), creationRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionCancellationDiscardsReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := creationFunc(func(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		cancel()
		return predictCreation(ctx, request)
	})
	result, err := mustRuntime(c, creationProvider(service)).ProjectTableCreations(ctx, creationRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionRequiresSelectedService(t *testing.T) {
	c := qt.New(t)
	result, err := mustRuntime(c, creationProvider(nil)).ProjectTableCreations(t.Context(), creationRequest())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionRefusesTypedNilRegistration(t *testing.T) {
	c := qt.New(t)
	var service creationFunc
	runtime, err := engine.New(creationProvider(service))
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}

func TestCreationProjectionReachesDocumentConversion(t *testing.T) {
	c := qt.New(t)
	var request schemaprojection.TableCreationRequest
	provider := creationProvider(creationFunc(func(ctx context.Context, input schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		request = input.Clone()
		return predictCreation(ctx, input)
	}))
	provider.Codecs = append(provider.Codecs, conversionCodec(conversionFirst, schemaext.Observed))
	provider.Conversions = []engine.Conversion{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Service: conversionFunc(func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
		return request.Values, nil
	})}}
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "bigint"}},
	}
	result, err := goschematodb.ToDBSchema(t.Context(), source, "alternate", mustRuntime(c, provider))
	c.Assert(err, qt.IsNil)
	c.Assert(request.Target, qt.Equals, "custom")
	value, found, err := schemaext.FacetAs[*conversionValue](result.Tables[0].Facets, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Number, qt.Equals, 2)
	c.Assert(result.Tables[0].Facets.TargetScope(conversionFirst), qt.DeepEquals, []string{"custom"})
	c.Assert(result.Tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(result.FeatureCoverage.Lookup(conversionFirst, request.Tables[0].Subject).State, qt.Equals, schemaext.Complete)
	c.Assert(source.Tables[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(source.Fields[0].Primary, qt.IsFalse)
}

func TestColumnConversionDoesNotPredictTableCreation(t *testing.T) {
	c := qt.New(t)
	// No creation service is installed. A non-key ADD COLUMN on an existing
	// table must not request storage defaults for an invented new table.
	runtime := mustRuntime(c, creationProvider(nil))
	columns, err := goschematodb.Columns(t.Context(), schemamodel.Table{Name: "events", StructName: "Event"},
		[]schemamodel.Field{{Name: "payload", StructName: "Event", Type: "text"}}, "custom", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.HasLen, 1)
	c.Assert(columns[0].Name, qt.Equals, "payload")
	c.Assert(columns[0].IsPrimaryKey, qt.IsFalse)
}
