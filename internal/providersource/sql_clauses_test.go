package providersource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/sqlext"
	"ptah.run/internal/providersource"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// clauseEngine claims TTL clauses that declare a retention, and declares a
// model of another kind, which a SQL document of the claims does not enroll.
func clauseEngine() *protocol.Engine {
	return &protocol.Engine{Id: "example", Features: []string{provider.SQLTableClausesFeature},
		ValueModels: []*protocol.ValueModel{
			{Kind: "example/other", Representation: protocol.ValueInput_DESIRED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}},
			{Kind: "example/retention", Representation: protocol.ValueInput_OBSERVED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}},
			{Kind: "example/retention", Representation: protocol.ValueInput_DESIRED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}},
		},
		SqlSchema: &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS,
			TableClauses: []*protocol.SQLTableClause{{Words: []string{"TTL"}, Selector: "ttl", Models: []string{"example/retention"}}}}}
}

// TestSQLDocument_HappyPath captures two clauses of one table and one of
// another: one binding for each table, a fragment for each clause in order,
// each with its claim, its table as binding and subject, its text, and a span
// whose end follows the clause's own lines, a CRLF one included.
func TestSQLDocument_HappyPath(t *testing.T) {
	c := qt.New(t)
	engine := clauseEngine()
	claim := engine.GetSqlSchema().GetTableClauses()[0]
	clauses := []providersource.TableClause{
		{Claim: claim, Table: "items", Text: "TTL 2 MINUTES", Start: sqlext.Position{Offset: 40, Line: 1, Column: 41}},
		{Claim: claim, Schema: "app", Table: "logs", Text: "TTL 1\r\n  SECONDS", Start: sqlext.Position{Offset: 100, Line: 3, Column: 5}},
		{Claim: claim, Table: "items", Text: "TTL 3 SECONDS", Start: sqlext.Position{Offset: 200, Line: 6, Column: 1}},
	}

	request, err := providersource.SQLDocument(c.Context(), engine, "schema.sql", 300, clauses)

	c.Assert(err, qt.IsNil)
	document := request.GetDocuments()[0]
	c.Assert(document.GetFormat(), qt.Equals, protocol.SourceDocument_SQL)
	c.Assert(document.GetFile(), qt.Equals, "schema.sql")
	c.Assert(document.GetByteLength(), qt.Equals, uint64(300))
	c.Assert(document.GetModels(), qt.HasLen, 1)
	c.Assert(proto.Equal(document.GetModels()[0], engine.GetValueModels()[2]), qt.IsTrue)
	items := &protocol.Reference{Engine: "example", Kind: "table", Name: "items"}
	logs := &protocol.Reference{Engine: "example", Kind: "table", Schema: "app", Name: "logs"}
	c.Assert(document.GetBindings(), qt.HasLen, 2)
	c.Assert(proto.Equal(document.GetBindings()[0].GetObject(), items), qt.IsTrue)
	c.Assert(proto.Equal(document.GetBindings()[1].GetObject(), logs), qt.IsTrue)
	fragments := document.GetFragments()
	c.Assert(fragments, qt.HasLen, 3)
	c.Assert(proto.Equal(fragments[0].GetSqlTableClause(), claim), qt.IsTrue)
	c.Assert(fragments[0].GetSelector(), qt.Equals, "ttl")
	c.Assert(fragments[0].GetBindingId(), qt.Equals, fragments[2].GetBindingId())
	c.Assert(fragments[1].GetBindingId(), qt.Not(qt.Equals), fragments[0].GetBindingId())
	c.Assert(proto.Equal(fragments[1].GetSubject(), logs), qt.IsTrue)
	c.Assert(fragments[1].GetSql(), qt.Equals, "TTL 1\r\n  SECONDS")
	c.Assert(proto.Equal(fragments[0].GetSpan(), &protocol.SourceSpan{Start: &protocol.SourcePosition{ByteOffset: 40, Line: 1, Column: 41},
		End: &protocol.SourcePosition{ByteOffset: 53, Line: 1, Column: 54}}), qt.IsTrue)
	c.Assert(proto.Equal(fragments[1].GetSpan(), &protocol.SourceSpan{Start: &protocol.SourcePosition{ByteOffset: 100, Line: 3, Column: 5},
		End: &protocol.SourcePosition{ByteOffset: 116, Line: 4, Column: 10}}), qt.IsTrue)
	c.Assert(request.GetTargetFeatures(), qt.DeepEquals, []string{provider.SQLTableClausesFeature})
}

// TestSQLDocument_FailurePath refuses what the source contract cannot
// carry: a negative length, a position without a line, and a clause that
// runs past the document.
func TestSQLDocument_FailurePath(t *testing.T) {
	claim := clauseEngine().GetSqlSchema().GetTableClauses()[0]
	for _, test := range []struct {
		name    string
		length  int
		clause  providersource.TableClause
		wantErr string
	}{
		{"a negative length", -1, providersource.TableClause{Claim: claim, Table: "items", Text: "TTL 1", Start: sqlext.Position{Line: 1, Column: 1}},
			`a SQL document has a negative length`},
		{"a position without a line", 10, providersource.TableClause{Claim: claim, Table: "items", Text: "TTL 1", Start: sqlext.Position{Column: 1}},
			`SQL clause position 0:1 at byte 0 is outside what a source position holds`},
		{"a clause past the document", 4, providersource.TableClause{Claim: claim, Table: "items", Text: "TTL 1", Start: sqlext.Position{Line: 1, Column: 1}},
			`.*source span is outside the captured document`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			request, err := providersource.SQLDocument(c.Context(), clauseEngine(), "schema.sql", test.length, []providersource.TableClause{test.clause})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(request, qt.IsNil)
		})
	}
}
