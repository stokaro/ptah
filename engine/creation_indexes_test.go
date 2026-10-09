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
)

func indexCreationRequest() schemaprojection.TableCreationRequest {
	request := creationRequest()
	request.Tables[0].Declaration.Indexes = []schemamodel.Index{{Name: "by.id", Fields: []string{"id"}}}
	return request
}

func predictIndexCreation(_ context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, table := range request.Tables {
		result.Tables = append(result.Tables, schemaprojection.TableCreation{
			Subject: table.Subject,
			Facets: []schemaext.FacetRecord{{
				Subject: objectidentity.NewBuilder(request.Identifiers).IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, "by.id"),
				Values:  must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 2})),
			}},
		})
	}
	return result, nil
}

func TestCreationProjectionAllowsIndexDefaultsWithoutMutatingSource(t *testing.T) {
	c := qt.New(t)
	request := indexCreationRequest()
	var reply schemaprojection.TableCreationResult
	service := creationFunc(func(ctx context.Context, input schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		reply = must.Must(predictIndexCreation(ctx, input))
		input.Tables[0].Declaration.Indexes[0].Fields[0] = "provider mutation"
		return reply, nil
	})
	result, err := mustRuntime(c, creationProvider(service)).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].Facets, qt.HasLen, 1)
	subject := objectidentity.NewBuilder(request.Identifiers).IndexParts("", "events", "by.id")
	c.Assert(result.Tables[0].Facets[0].Subject, qt.Equals, subject)
	value, found, err := schemaext.FacetAs[*conversionValue](result.Tables[0].Facets[0].Values, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Number, qt.Equals, 2)
	c.Assert(request.Tables[0].Declaration.Indexes[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(request.Tables[0].Declaration.Indexes[0].Fields, qt.DeepEquals, []string{"id"})
	reply.Tables[0].Facets[0] = schemaext.FacetRecord{}
	c.Assert(result.Tables[0].Facets[0].Subject, qt.Equals, subject)
	c.Assert(result.Tables[0].Facets[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionFirst})
}

func TestCreationProjectionRejectsInvalidIndexOwners(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemaprojection.TableCreationResult)
	}{
		{"invented index", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Facets[0].Subject.Name.Normalized = "absent"
			r.Tables[0].Facets[0].Subject.Name.Source = "absent"
		}},
		{"changed source spelling", func(r *schemaprojection.TableCreationResult) { r.Tables[0].Facets[0].Subject.Name.Source = "forged" }},
		{"another parent", func(r *schemaprojection.TableCreationResult) { r.Tables[0].Facets[0].Subject.Parent.Source = "other" }},
		{"repeated owner", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Facets = append(r.Tables[0].Facets, r.Tables[0].Facets[0])
		}},
		{"empty values", func(r *schemaprojection.TableCreationResult) { r.Tables[0].Facets[0].Values = schemaext.Facets{} }},
		{"new scope", func(r *schemaprojection.TableCreationResult) {
			r.Tables[0].Facets[0].Values = must.Must(r.Tables[0].Facets[0].Values.WithTargetScope(conversionFirst, "custom"))
		}},
		{"excluded output", func(r *schemaprojection.TableCreationResult) {
			facets := must.Must(r.Tables[0].Facets[0].Values.WithTargetScope(conversionFirst, "other"))
			r.Tables[0].Facets[0].Values = must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("custom"))))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := creationFunc(func(ctx context.Context, input schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
				reply := must.Must(predictIndexCreation(ctx, input))
				test.change(&reply)
				return reply, nil
			})
			result, err := mustRuntime(c, creationProvider(service)).ProjectTableCreations(t.Context(), indexCreationRequest())
			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
			c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
		})
	}
}

func TestCreationProjectionCannotRestoreExcludedIndexSettings(t *testing.T) {
	c := qt.New(t)
	request := indexCreationRequest()
	facets := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 1}))
	facets = must.Must(facets.WithTargetScope(conversionFirst, "other"))
	request.Tables[0].Declaration.Indexes[0].Facets = must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("custom"))))
	result, err := mustRuntime(c, creationProvider(creationFunc(predictIndexCreation))).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}

func TestCreationProjectionKeepsSameNamedIndexesOnSeparateTables(t *testing.T) {
	c := qt.New(t)
	request := indexCreationRequest()
	sibling := request.Clone().Tables[0]
	sibling.Declaration.Table.Schema = "tenant.archive"
	sibling.Declaration.Table.Name = "events.2026"
	sibling.Subject = objectidentity.NewBuilder(request.Identifiers).TableParts("tenant.archive", "events.2026")
	request.Tables = append(request.Tables, sibling)
	runtime := mustRuntime(c, creationProvider(creationFunc(predictIndexCreation)))
	result, err := runtime.ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].Facets[0].Subject.Key(), qt.Not(qt.Equals), result.Tables[1].Facets[0].Subject.Key())
	c.Assert(result.Tables[1].Facets[0].Subject.Schema.Source, qt.Equals, "tenant.archive")
	c.Assert(result.Tables[1].Facets[0].Subject.Parent.Source, qt.Equals, "events.2026")

	service := creationFunc(func(ctx context.Context, input schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
		reply := must.Must(predictIndexCreation(ctx, input))
		reply.Tables[0].Facets = reply.Tables[1].Facets
		return reply, nil
	})
	result, err = mustRuntime(c, creationProvider(service)).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}
