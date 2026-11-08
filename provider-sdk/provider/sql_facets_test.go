package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

const sqlFacetKind = "example.org/fixture/feeds"

// sqlFacetExchange is the SQL schema exchange of an engine that lists
// SQLFacetsFeature, whose reply carries a facet of table a, one of its column
// x and one of its index a_x, of a SQL model of their own that the reply
// covers.
func sqlFacetExchange() (*protocol.Description, *protocol.ReadSQLSchemaRequest, *protocol.SQLSchema) {
	d, request, result := sqlSchemaExchange()
	d.Features = append(d.Features, provider.SQLFacetsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.SQLFacetsFeature)
	model := proto.CloneOf(engine.ValueModels[0])
	model.Kind = sqlFacetKind
	engine.ValueModels = append(engine.ValueModels, model)
	request.TargetFeatures = []string{provider.SQLFacetsFeature}
	result.Coverage.Kinds = append(result.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	result.Facets = []*protocol.Envelope{
		sqlFacet(d, &protocol.Reference{Engine: "fixture", Kind: "table", Name: "a"}),
		sqlFacet(d, &protocol.Reference{Engine: "fixture", Kind: "column", Path: []string{"a"}, Name: "x"}),
		sqlFacet(d, &protocol.Reference{Engine: "fixture", Kind: "index", Path: []string{"a"}, Name: "a_x"}),
	}
	result.Tables[0].Indexes = []*protocol.DeclaredIndex{{Name: "a_x", Fields: []string{"x"}}}
	result.Traces = append(result.Traces, sqlTrace("one", protocol.SQLObjectTrace_FACETS, 0), sqlTrace("two", protocol.SQLObjectTrace_FACETS, 1),
		sqlTrace("two", protocol.SQLObjectTrace_FACETS, 2))
	return d, request, result
}

func sqlFacet(d *protocol.Description, owner *protocol.Reference) *protocol.Envelope {
	return &protocol.Envelope{ProviderId: d.ProviderId, Kind: sqlFacetKind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: testValueDefinition,
		Owner: owner, Payload: []byte(`{}`)}
}

func TestSQLSchemaFacets_HappyPath(t *testing.T) {
	t.Run("a table, a column and an index facet", func(t *testing.T) {
		c := qt.New(t)
		description, request, result := sqlFacetExchange()
		before := proto.CloneOf(result)
		c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
		c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
		c.Assert(proto.Equal(result, before), qt.IsTrue)
	})
	t.Run("an owner naming the default schema", func(t *testing.T) {
		c := qt.New(t)
		description, request, result := sqlFacetExchange()
		for _, facet := range result.Facets {
			facet.Owner.Schema = "main"
		}
		c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
	})
	t.Run("facets of one kind on a table and a column of it", func(t *testing.T) {
		c := qt.New(t)
		description, request, result := sqlFacetExchange()
		result.Facets = append(result.Facets, sqlFacet(description, &protocol.Reference{Engine: "fixture", Kind: "column", Path: []string{"a"}, Name: "id"}))
		result.Traces = append(result.Traces, sqlTrace("one", protocol.SQLObjectTrace_FACETS, 3))
		c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
	})
}

func TestSQLSchemaFacets_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		change  func(*protocol.Description, *protocol.ReadSQLSchemaRequest, *protocol.SQLSchema)
		wantErr string
	}{
		{"a request that does not list the feature", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest, _ *protocol.SQLSchema) {
			r.TargetFeatures = nil
		}, `.*SQL schema: facets were not negotiated`},
		{"a provider that does not list the feature", func(d *protocol.Description, _ *protocol.ReadSQLSchemaRequest, _ *protocol.SQLSchema) {
			d.Features = d.Features[:len(d.Features)-1]
		}, `.*SQL schema: facets were not negotiated`},
		{"a model without the SQL format", func(d *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			d.Engines[0].ValueModels[1].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
			s.Coverage.Kinds[1].Model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
		}, `.*a facet is outside the negotiated and covered SQL models`},
		{"a kind the coverage leaves out", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Coverage.Kinds = s.Coverage.Kinds[:1]
		}, `.*a facet is outside the negotiated and covered SQL models`},
		{"another definition", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].DefinitionSha256 = "other"
		}, `.*a facet is outside the negotiated and covered SQL models`},
		{"a foreign provider", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].ProviderId = "example.org/other"
		}, `.*invalid value envelope or foreign owner`},
		{"an owner of another engine", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Engine = "other"
		}, `.*invalid value subject`},
		{"an undeclared table", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Name = "b"
		}, `.*a facet must belong to a declared table, column or index`},
		{"a table in another schema", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Schema = "other"
		}, `.*a facet must belong to a declared table, column or index`},
		{"an owner with a database", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Database = "db"
		}, `.*a facet must belong to a declared table, column or index`},
		{"an owner with a catalog", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Catalog = "catalog"
		}, `.*a facet must belong to a declared table, column or index`},
		{"an owner with an overload", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Overload = []string{"integer"}
		}, `.*a facet must belong to a declared table, column or index`},
		{"a table owner with a path", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Path = []string{"nested"}
		}, `.*a facet must belong to a declared table, column or index`},
		{"an owner of another kind", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[0].Owner.Kind = "view"
		}, `.*a facet must belong to a declared table, column or index`},
		{"an undeclared column", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[1].Owner.Name = "y"
		}, `.*a facet must belong to a declared table, column or index`},
		{"a column of an undeclared table", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[1].Owner.Path = []string{"b"}
		}, `.*a facet must belong to a declared table, column or index`},
		{"a column owner with a nested path", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[1].Owner.Path = []string{"main", "a"}
		}, `.*a facet must belong to a declared table, column or index`},
		{"an undeclared index", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[2].Owner.Name = "a_y"
		}, `.*a facet must belong to a declared table, column or index`},
		{"an index owner without its table", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Facets[2].Owner.Path = nil
		}, `.*a facet must belong to a declared table, column or index`},
		{"two facets of one kind on a table", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			repeated := proto.CloneOf(s.Facets[0])
			repeated.Owner.Schema = "main"
			s.Facets = append(s.Facets, repeated)
			s.Traces = append(s.Traces, sqlTrace("one", protocol.SQLObjectTrace_FACETS, 3))
		}, `.*an owner holds two facets of one kind`},
		{"a facet without a trace", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Traces = s.Traces[:len(s.Traces)-1]
		}, `.*every object needs exactly one trace`},
		{"a trace past the facets", func(_ *protocol.Description, _ *protocol.ReadSQLSchemaRequest, s *protocol.SQLSchema) {
			s.Traces[len(s.Traces)-1].Index = 3
		}, `.*trace names no object or repeats one`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, result := sqlFacetExchange()
			row.change(description, request, result)
			err := provider.ValidateSQLSchema(description, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
		})
	}
}

// A refused load exposes no schema, facets included.
func TestSQLSchemaFacets_IncompleteReply(t *testing.T) {
	c := qt.New(t)
	description, request, _ := sqlFacetExchange()
	refused := &protocol.SQLSchema{
		Diagnostics: []*protocol.SQLDiagnostic{{Code: protocol.SQLDiagnostic_SYNTAX, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: "two", Span: sqlSpan(6, 11), Message: "unexpected token"}},
		Facets:      []*protocol.Envelope{sqlFacet(description, &protocol.Reference{Engine: "fixture", Kind: "table", Name: "a"})},
	}
	err := provider.ValidateSQLSchema(description, request, refused)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*an incomplete reply exposes no schema`)
}

func TestSQLSchemaFacetsDeclaration_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"an engine feature without the provider's", func(d *protocol.Description) { d.Features = d.Features[:len(d.Features)-1] }},
		{"an engine that reads fragments", func(d *protocol.Description) {
			d.Features = []string{provider.InspectFeature, provider.NormalizeValuesFeature, provider.SQLFacetsFeature}
			d.Engines[0].Features = []string{"retention", provider.SQLFacetsFeature}
			d.Engines[0].SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS}
		}},
		{"an engine without a declaration", func(d *protocol.Description) {
			d.Features = []string{provider.InspectFeature, provider.NormalizeValuesFeature, provider.SQLFacetsFeature}
			d.Engines[0].Features = []string{"retention", provider.SQLFacetsFeature}
			d.Engines[0].SqlSchema = nil
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			description, _, _ := sqlFacetExchange()
			row.change(description)
			err := provider.ValidateDescription(description, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*facets require the provider's feature and the DOCUMENT mode`)
		})
	}
}

// A host that does not list the feature removes it from the negotiated
// description, so a reply carrying facets is refused at both boundaries,
// which validate with that copy.
func TestSQLSchemaFacets_UnlistedHostFeature(t *testing.T) {
	c := qt.New(t)
	description, request, result := sqlFacetExchange()
	negotiated := provider.NegotiatedDescription(description, nil)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.SQLFacetsFeature)
	request.TargetFeatures = nil
	c.Assert(provider.ValidateSQLSchema(negotiated, request, result), qt.ErrorMatches, `.*SQL schema: facets were not negotiated`)
	listed := provider.NegotiatedDescription(description, []string{provider.SQLFacetsFeature})
	request.TargetFeatures = []string{provider.SQLFacetsFeature}
	c.Assert(provider.ValidateSQLSchema(listed, request, result), qt.IsNil)
}
