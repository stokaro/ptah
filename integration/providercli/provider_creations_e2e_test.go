//go:build integration

package providercli_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
)

func creationRequest() schemaprojection.TableCreationRequest {
	rules := comparisonRules()
	subject := objectidentity.NewBuilder(rules).Table("records")
	return schemaprojection.TableCreationRequest{Target: "fixture", Identifiers: rules, Tables: []schemaprojection.TableCreationInput{{Subject: subject,
		Declaration: schemacapture.TableDeclaration{
			Table:       schemamodel.Table{Name: "records", PrimaryKey: []string{"second", "first"}},
			Fields:      []schemamodel.Field{{Name: "first", Type: "INTEGER", Comment: "keep column"}, {Name: "second", Type: "INTEGER"}, {Name: "extra", Type: "TEXT"}},
			Indexes:     []schemamodel.Index{{Name: "by_first", Parts: []schemamodel.IndexPart{{Name: "first", Prefix: "0008"}}}},
			Constraints: []schemamodel.Constraint{{Name: "positive", Type: "CHECK", CheckExpression: "first > 0"}},
		},
	}}}
}

func TestProviderProjectsCapturedCreationsIndependently(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "table-creations")
	request := creationRequest()
	before := request.Clone()
	result, err := runtime.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Tables, qt.HasLen, 1)
	projected := result.Tables[0]
	c.Assert(projected.Subject, qt.DeepEquals, request.Tables[0].Subject)
	c.Assert(projected.ColumnPrimaryKeysPrepared, qt.IsTrue)
	c.Assert(projected.ColumnPrimaryKeys, qt.DeepEquals, []string{"second", "first"})
	owners, err := request.Tables[0].FacetOwners(request.Identifiers)
	c.Assert(err, qt.IsNil)
	c.Assert(owners, qt.HasLen, 4)
	for _, entry := range []struct {
		representation schemaext.Representation
		records        []schemaext.FacetRecord
		payload        string
	}{
		{schemaext.Desired, projected.Facets, `{"seconds":60}`}, {schemaext.Observed, projected.Observed, `{"seconds":90}`},
	} {
		c.Assert(entry.records, qt.HasLen, len(owners))
		for i, record := range entry.records {
			c.Assert(record.Subject, qt.DeepEquals, owners[i])
			encoded, err := runtime.Codecs().EncodeFacets(c.Context(), entry.representation, record.Values)
			c.Assert(err, qt.IsNil)
			c.Assert(encoded, qt.HasLen, 1)
			var wire struct{ Payload []byte }
			c.Assert(json.Unmarshal(encoded[0].Value.Payload, &wire), qt.IsNil)
			c.Assert(string(wire.Payload), qt.Equals, entry.payload)
			c.Assert(record.Values.TargetScope(comparisonFacet), qt.HasLen, 0)
		}
	}
	c.Assert(request.Tables[0].Declaration.Fields, qt.DeepEquals, before.Tables[0].Declaration.Fields)
	c.Assert(request.Tables[0].Declaration.Table.Facets.DeclaredKinds(), qt.HasLen, 0)
	empty, err := runtime.ProjectTableCreations(c.Context(), schemaprojection.TableCreationRequest{Target: "fixture", Identifiers: request.Identifiers})
	c.Assert(err, qt.IsNil)
	c.Assert(empty.Complete, qt.IsTrue)
	c.Assert(empty.Tables, qt.HasLen, 0)
}

func TestProviderCreationRetainsExcludedProperties(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "table-creations")
	request := creationRequest()
	facet := comparisonModel(c, runtime, schemaext.Desired, comparisonFacet, request.Tables[0].Subject, `{"minutes":2}`)
	facets, err := schemaext.NewFacets(facet)
	c.Assert(err, qt.IsNil)
	facets, err = facets.WithTargetScope(comparisonFacet, "other")
	c.Assert(err, qt.IsNil)
	selection, err := schemaext.NewTargetSelection("fixture")
	c.Assert(err, qt.IsNil)
	request.Tables[0].Declaration.Table.Facets, err = facets.ForTarget(selection)
	c.Assert(err, qt.IsNil)
	result, err := runtime.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].Facets, qt.HasLen, 3)
	c.Assert(result.Tables[0].Observed, qt.HasLen, 3)
	for _, record := range result.Tables[0].Facets {
		c.Assert(record.Subject.Kind, qt.Not(qt.Equals), objectidentity.KindTable)
	}
	c.Assert(request.Tables[0].Declaration.Table.Facets.TargetScope(comparisonFacet), qt.DeepEquals, []string{"other"})
}

func TestProviderCreationDiscardsInvalidSuffixAndCancellation(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "table-creations")
	request := creationRequest()
	invalid := schemaprojection.TableCreationInput{Subject: objectidentity.NewBuilder(request.Identifiers).Table("invalid"),
		Declaration: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "invalid"}, Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER"}}}}
	request.Tables = append(request.Tables, invalid)
	result, err := runtime.ProjectTableCreations(c.Context(), request)
	c.Assert(err, qt.IsNotNil)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Tables, qt.HasLen, 0)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = runtime.ProjectTableCreations(ctx, creationRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Tables, qt.HasLen, 0)
}
