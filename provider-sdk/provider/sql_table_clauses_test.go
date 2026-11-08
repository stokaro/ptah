package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// ttlClause is the text of a row deletion policy as a SQL document writes it
// after a table's column list, and ttlClauseLength its length in bytes.
const (
	ttlClause       = "ttl  INTERVAL '30 days' ON created_at"
	ttlClauseLength = 37
)

// clauseSpan is where ttlClause stands in its document: from byte 10 of the
// first line.
func clauseSpan() *protocol.SourceSpan {
	return &protocol.SourceSpan{Start: &protocol.SourcePosition{ByteOffset: 10, Line: 1, Column: 11},
		End: &protocol.SourcePosition{ByteOffset: 10 + ttlClauseLength, Line: 1, Column: 11 + ttlClauseLength}}
}

// tableClauseExchange claims the fixture engine's TTL clause in a SQL
// document, which sends the clause as one fragment bound to its table with
// the table as its subject, and decodes it as a facet of the table.
func tableClauseExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, request, result := sourceExchange()
	d.Features = append(d.Features, provider.SQLTableClausesFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.SQLTableClausesFeature)
	model := engine.ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_SQL}
	claim := &protocol.SQLTableClause{Words: []string{"TTL"}, Selector: "row-deletion", Models: []string{model.Kind}}
	engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS, TableClauses: []*protocol.SQLTableClause{claim}}
	value := result.Documents[0].Fragments[0].Values[0]
	table := proto.CloneOf(value.Value.Value.Owner)
	document := request.Documents[0]
	document.Format, document.File = protocol.SourceDocument_SQL, "schema.sql"
	document.Models = []*protocol.ValueModel{proto.CloneOf(model)}
	document.Bindings[0].Object = table
	document.Bindings[0].StructName, document.Bindings[0].EntryKey = "", ""
	document.Fragments[0] = &protocol.SourceFragment{Id: "ttl", Selector: "row-deletion", BindingId: "table", Span: clauseSpan(),
		SqlTableClause: proto.CloneOf(claim), Subject: proto.CloneOf(table), Content: &protocol.SourceFragment_Sql{Sql: ttlClause}}
	result.Documents[0].Coverage.Kinds[0].Model = proto.CloneOf(model)
	result.Documents[0].Fragments[0].Id = "ttl"
	value.Source.FragmentId = "ttl"
	return d, request, result
}

// A claimed clause reaches the provider whole, and decodes to a facet of its
// table.
func TestSQLTableClauses_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a clause decodes to a facet of its table", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := tableClauseExchange()
		before := proto.CloneOf(request)
		c.Assert(ttlClause, qt.HasLen, ttlClauseLength)
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
		c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
		c.Assert(proto.Equal(before, request), qt.IsTrue)
	})
	t.Run("a claim matches the words that open a clause, whatever their case", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := tableClauseExchange()
		engine := d.Engines[0]
		c.Assert(provider.SQLTableClauseClaim(engine, "Ttl\tINTERVAL '1 day' ON at"), qt.Equals, engine.SqlSchema.TableClauses[0])
		c.Assert(provider.SQLTableClauseClaim(engine, "TTL(1)"), qt.Equals, engine.SqlSchema.TableClauses[0])
		c.Assert(provider.SQLTableClauseClaim(engine, "TTLS INTERVAL '1 day'"), qt.IsNil)
		c.Assert(provider.SQLTableClauseClaim(engine, "WITH (fillfactor = 70)"), qt.IsNil)
	})
	t.Run("a claim may open with several words", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := tableClauseExchange()
		claim := d.Engines[0].SqlSchema.TableClauses[0]
		claim.Words = []string{"ROW", "DELETION", "POLICY"}
		fragment := request.Documents[0].Fragments[0]
		fragment.SqlTableClause = proto.CloneOf(claim)
		// 52 bytes, the second line 39 of them.
		sql := "row deletion\npolicy (older_than(at, INTERVAL 1 DAY))"
		fragment.Content = &protocol.SourceFragment_Sql{Sql: sql}
		fragment.Span = &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1},
			End: &protocol.SourcePosition{ByteOffset: 52, Line: 2, Column: 40}}
		c.Assert(sql, qt.HasLen, 52)
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
		c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates the claims away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := tableClauseExchange()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.SQLTableClausesFeature))
		c.Assert(negotiated.Engines[0].SqlSchema.GetTableClauses(), qt.IsNil)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.SQLTableClausesFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.SQLTableClausesFeature)
	})
}

func TestSQLTableClauseDeclarations_FailurePath(t *testing.T) {
	t.Parallel()
	claim := func(d *protocol.Description) *protocol.SQLTableClause { return d.Engines[0].SqlSchema.TableClauses[0] }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"a claim without the engine's feature", "an engine claims table clauses exactly when it lists source.sql-table-clauses", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.SQLTableClausesFeature)
		}},
		{"the feature without a claim", "an engine claims table clauses exactly when it lists source.sql-table-clauses", func(d *protocol.Description) {
			d.Engines[0].SqlSchema.TableClauses = nil
		}},
		{"the engine's feature without the provider's", "table-clause claims need the provider's source.sql-table-clauses and source decoding", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.SQLTableClausesFeature)
		}},
		{"the feature without source decoding", "table-clause claims need the provider's source.sql-table-clauses and source decoding", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.DecodeSourceFeature)
			d.Engines[0].YamlSourceKeys = nil
			d.Engines[0].ValueModels[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}
		}},
		{"the DOCUMENT mode", "table-clause claims need the FRAGMENTS mode", func(d *protocol.Description) {
			d.Features = append(d.Features, provider.ReadSQLSchemaFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.ReadSQLSchemaFeature)
			d.Engines[0].SqlSchema.Mode = protocol.SQLSchemaDeclaration_DOCUMENT
		}},
		{"no word", "incomplete table-clause claim", func(d *protocol.Description) { claim(d).Words = nil }},
		{"no selector", "incomplete table-clause claim", func(d *protocol.Description) { claim(d).Selector = " " }},
		{"no model", "incomplete table-clause claim", func(d *protocol.Description) { claim(d).Models = nil }},
		{"a word in lower case", "a table-clause word is not one upper-case word", func(d *protocol.Description) { claim(d).Words = []string{"ttl"} }},
		{"two words in one", "a table-clause word is not one upper-case word", func(d *protocol.Description) { claim(d).Words = []string{"ROW DELETION"} }},
		{"a word that starts with a digit", "a table-clause word is not one upper-case word", func(d *protocol.Description) { claim(d).Words = []string{"1TTL"} }},
		{"two claims that open with one word", "two table-clause claims open with one word", func(d *protocol.Description) {
			second := proto.CloneOf(claim(d))
			second.Words = []string{"TTL", "BY"}
			d.Engines[0].SqlSchema.TableClauses = append(d.Engines[0].SqlSchema.TableClauses, second)
		}},
		{"an unknown model", "a table-clause claim names an unknown, duplicate or non-SQL desired model", func(d *protocol.Description) {
			claim(d).Models = []string{"example.org/fixture/unknown"}
		}},
		{"a repeated model", "a table-clause claim names an unknown, duplicate or non-SQL desired model", func(d *protocol.Description) {
			claim(d).Models = append(claim(d).Models, claim(d).Models[0])
		}},
		{"a model SQL documents do not state", "a table-clause claim names an unknown, duplicate or non-SQL desired model", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := tableClauseExchange()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+test.want)
		})
	}
}

func TestSQLTableClauseFragments_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.DecodeSourceRequest)
	}{
		{"a changed claim", "table-clause fragment changed its negotiated claim", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].SqlTableClause.Words = []string{"ROW"}
		}},
		{"a changed selector", "table-clause fragment changed its negotiated claim", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].Selector = "other"
		}},
		{"a document of another format", "a table-clause claim outside a SQL document or beside another claim", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Format = protocol.SourceDocument_YAML
		}},
		{"no binding", "table-clause fragment needs its table's binding and that table as its subject", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].BindingId = ""
		}},
		{"another subject", "table-clause fragment needs its table's binding and that table as its subject", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].Subject.Name = "other"
		}},
		{"a column as the subject", "table-clause fragment needs its table's binding and that table as its subject", func(r *protocol.DecodeSourceRequest) {
			fragment := r.Documents[0].Fragments[0]
			fragment.Subject = &protocol.Reference{Engine: "fixture", Kind: "column", Path: []string{fragment.Subject.Name}, Name: "created_at"}
		}},
		{"content that opens with other words", "table-clause fragment does not open with its claim's words", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].Content = &protocol.SourceFragment_Sql{Sql: "TTLX INTERVAL '30 days' ON created_at"}
		}},
		{"a model outside the document's scope", "unknown, changed, duplicate, or unsupported source model", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Models[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := tableClauseExchange()
			test.mutate(request)
			err := provider.ValidateSourceRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+test.want)
		})
	}
}

// A clause declares facets of its table and nothing else.
func TestSQLTableClauseValues_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.DecodedSource)
	}{
		{"a native object", func(v *protocol.DecodedSource) {
			v.Documents[0].Fragments[0].Values[0].Placement = protocol.SourceModelValue_NATIVE_OBJECT
		}},
		{"a facet of another table", func(v *protocol.DecodedSource) {
			v.Documents[0].Fragments[0].Values[0].Value.Value.Owner.Name = "other"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := tableClauseExchange()
			test.mutate(result)
			err := provider.ValidateDecodedSource(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*a value of a table-clause fragment is a facet of the clause's table")
		})
	}
}
