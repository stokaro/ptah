//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
)

func TestProviderPreparesCapturedTablesIndependently(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "table-preparation")
	rules := comparisonRules()
	subject := objectidentity.NewBuilder(rules).Table("records")
	facets, err := schemaext.NewFacets(comparisonModel(c, runtime, schemaext.Desired, comparisonFacet, subject, `{"minutes":1}`))
	c.Assert(err, qt.IsNil)
	facets, err = facets.WithTargetScope(comparisonFacet, "fixture")
	c.Assert(err, qt.IsNil)
	request := schemapreparation.Request{Target: "fixture", Identifiers: rules, Tables: []schemapreparation.Table{{
		Subject: subject,
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "records", PrimaryKey: []string{"second", "first"}, Facets: facets},
			Fields: []schemamodel.Field{{Name: "first", Type: "INTEGER", Comment: "keep source"}, {Name: "second", Type: "INTEGER"}, {Name: "extra", Type: "TEXT", Default: "'value'", DefaultSet: true}}},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "No catalog read"},
	}}}
	before := request.Clone()
	result, err := runtime.PrepareTables(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Tables[0].ColumnPrimaryKeysPrepared, qt.IsTrue)
	c.Assert(result.Tables[0].Desired.Fields[0].Primary, qt.IsTrue)
	c.Assert(result.Tables[0].Desired.Fields[1].Primary, qt.IsTrue)
	c.Assert(result.Tables[0].Desired.Fields[2].Primary, qt.IsFalse)
	c.Assert(result.Tables[0].Desired.Fields[0].Comment, qt.Equals, "keep source")
	c.Assert(result.Tables[0].Desired.Fields[2].Default, qt.Equals, "'value'")
	c.Assert(result.Tables[0].CurrentKnowledge, qt.DeepEquals, before.Tables[0].CurrentKnowledge)
	c.Assert(result.Tables[0].Desired.Table.Facets.TargetScope(comparisonFacet), qt.DeepEquals, []string{"fixture"})
	c.Assert(result.Tables[0].ResolvedFacets, qt.HasLen, 1)
	c.Assert(result.Tables[0].ResolvedFacets[0].Subject, qt.DeepEquals, subject)
	c.Assert(result.Tables[0].ResolvedFacets[0].Values.TargetScope(comparisonFacet), qt.HasLen, 0)
	c.Assert(request.Tables[0].Desired.Fields, qt.DeepEquals, before.Tables[0].Desired.Fields)
	// Preparation does not silently opt into identity CREATE prediction.
	_, err = runtime.ProjectTableCreations(c.Context(), schemaprojection.TableCreationRequest{Target: "fixture"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	invalid := request.Tables[0].Clone()
	invalid.Subject = objectidentity.NewBuilder(rules).Table("invalid")
	invalid.Desired.Table.Name = "invalid"
	invalid.Desired.Table.Facets = schemaext.Facets{}
	request.Tables = append(request.Tables, invalid)
	result, err = runtime.PrepareTables(c.Context(), request)
	c.Assert(err, qt.IsNotNil)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Tables, qt.HasLen, 0)
}
