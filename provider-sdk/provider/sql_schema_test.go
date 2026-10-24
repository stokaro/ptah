package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const (
	sqlFirstText  = "CREATE TABLE a (id Int);\n"
	sqlSecondText = "ALTER TABLE a ADD x Int;\n"
)

func sqlSpan(from, to uint32) *protocol.SourceSpan {
	return &protocol.SourceSpan{
		Start: &protocol.SourcePosition{ByteOffset: uint64(from), Line: 1, Column: from + 1},
		End:   &protocol.SourcePosition{ByteOffset: uint64(to), Line: 1, Column: to + 1},
	}
}

func sqlTrace(document string, collection protocol.SQLObjectTrace_Collection, index uint32) *protocol.SQLObjectTrace {
	return &protocol.SQLObjectTrace{Collection: collection, Index: index, Source: &protocol.SourceTrace{DocumentId: document, Span: sqlSpan(0, 10)}}
}

// sqlSchemaExchange declares a DOCUMENT engine and returns a two-document load
// with a complete reply that uses every collection of the reply.
func sqlSchemaExchange() (*protocol.Description, *protocol.ReadSQLSchemaRequest, *protocol.SQLSchema) {
	d, _ := valueInputs()
	d.Features = append(d.Features, provider.ReadSQLSchemaFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ReadSQLSchemaFeature)
	engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_DOCUMENT, HeaderKinds: []string{"table", "topic"}}
	model := engine.ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}
	request := &protocol.ReadSQLSchemaRequest{
		Engine:      "fixture",
		Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.Identifiers), DefaultSchema: "main"},
		Documents: []*protocol.SQLDocument{
			{Id: "one", Path: "one.sql", Text: sqlFirstText},
			{Id: "two", Path: "two.sql", Text: sqlSecondText},
		},
		Limits: []*protocol.SchemaCoverageLimit{{Kind: "topic", Name: "events", Reason: "managed elsewhere"}},
	}
	native := &protocol.Envelope{
		ProviderId: d.ProviderId, Kind: model.Kind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: testValueDefinition,
		Owner: &protocol.Reference{Engine: "fixture", Kind: model.Kind, Name: "events"}, Payload: []byte(`{}`),
	}
	one := int64(1)
	result := &protocol.SQLSchema{
		Complete:      true,
		Tables:        []*protocol.DeclaredTable{{Name: "a", Columns: []*protocol.DeclaredColumn{{Name: "id", Type: "Int"}, {Name: "x", Type: "Int"}}}},
		NativeObjects: []*protocol.Envelope{native},
		Declarations: []*protocol.SQLDocumentDeclarations{
			{DocumentId: "one", Objects: []*protocol.SQLDeclaration{{Kind: "table", Name: "a"}}, Tables: []*protocol.SQLTableStatement{{Name: "a"}}},
			{DocumentId: "two"},
		},
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED,
			Kinds: []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}},
		Diagnostics: []*protocol.SQLDiagnostic{{Code: protocol.SQLDiagnostic_UNSUPPORTED, Severity: protocol.SQLDiagnostic_WARNING, DocumentId: "two", Span: sqlSpan(0, 5), Message: "ignored hint"}},
		Views:       []*protocol.SQLView{{Name: "v", Body: "SELECT 1"}},
		Sequences:   []*protocol.SQLSequence{{Name: "s", Increment: &one}},
		Roles:       []*protocol.SQLRole{{Name: "reader"}},
		Grants:      []*protocol.SQLGrant{{Role: "reader", Privileges: []string{"SELECT"}, Target: &protocol.Reference{Kind: "table", Name: "a"}}},
		Enums:       []*protocol.DeclaredEnum{{Name: "kind", Values: []string{"a"}}},
		Namespaces:  []*protocol.DeclaredNamespace{{Name: "main", Subject: &protocol.ComparisonIdentity{Kind: "schema", Name: &protocol.ComparisonPart{Source: "main", Normalized: "main"}}}},
		Traces: []*protocol.SQLObjectTrace{
			sqlTrace("one", protocol.SQLObjectTrace_TABLES, 0),
			sqlTrace("two", protocol.SQLObjectTrace_NATIVE_OBJECTS, 0),
			sqlTrace("one", protocol.SQLObjectTrace_NAMESPACES, 0),
			sqlTrace("one", protocol.SQLObjectTrace_ENUMS, 0),
			sqlTrace("one", protocol.SQLObjectTrace_VIEWS, 0),
			sqlTrace("one", protocol.SQLObjectTrace_SEQUENCES, 0),
			sqlTrace("one", protocol.SQLObjectTrace_ROLES, 0),
			sqlTrace("two", protocol.SQLObjectTrace_GRANTS, 0),
		},
	}
	return d, request, result
}

func TestSQLSchemaExchangeHappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, result := sqlSchemaExchange()
	originalDescription, originalRequest, originalResult := proto.CloneOf(description), proto.CloneOf(request), proto.CloneOf(result)
	c.Assert(provider.ValidateDescription(description, []string{provider.ReadSQLSchemaFeature}), qt.IsNil)
	c.Assert(provider.ValidateReadSQLSchemaRequest(description, request), qt.IsNil)
	c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(originalDescription, description), qt.IsTrue)
	c.Assert(proto.Equal(originalRequest, request), qt.IsTrue)
	c.Assert(proto.Equal(originalResult, result), qt.IsTrue)
}

func TestSQLSchemaIncompleteReplyHappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, _ := sqlSchemaExchange()
	refused := &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{
		{Code: protocol.SQLDiagnostic_SYNTAX, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: "two", Span: sqlSpan(6, 11), Message: "unexpected token"},
	}}
	c.Assert(provider.ValidateSQLSchema(description, request, refused), qt.IsNil)
}

func TestSQLSchemaDeclarationFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"unspecified mode", func(d *protocol.Description) {
			d.Engines[0].SqlSchema.Mode = protocol.SQLSchemaDeclaration_MODE_UNSPECIFIED
		}},
		{"unknown mode", func(d *protocol.Description) { d.Engines[0].SqlSchema.Mode = 99 }},
		{"document mode without engine feature", func(d *protocol.Description) { d.Engines[0].Features = []string{"retention"} }},
		{"engine feature without declaration", func(d *protocol.Description) { d.Engines[0].SqlSchema = nil }},
		{"engine feature without provider feature", func(d *protocol.Description) { d.Features = []string{provider.NormalizeValuesFeature} }},
		{"fragments mode with the feature", func(d *protocol.Description) { d.Engines[0].SqlSchema.Mode = protocol.SQLSchemaDeclaration_FRAGMENTS }},
		{"fragments mode with header kinds", func(d *protocol.Description) {
			d.Engines[0].SqlSchema.Mode = protocol.SQLSchemaDeclaration_FRAGMENTS
			d.Engines[0].Features = []string{"retention"}
		}},
		{"empty header kind", func(d *protocol.Description) { d.Engines[0].SqlSchema.HeaderKinds = []string{""} }},
		{"duplicate header kind", func(d *protocol.Description) { d.Engines[0].SqlSchema.HeaderKinds = []string{"table", "table"} }},
		{"provider feature without a document engine", func(d *protocol.Description) {
			d.Engines[0].SqlSchema = nil
			d.Engines[0].Features = []string{"retention"}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, _, _ := sqlSchemaExchange()
			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
			test.change(description)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSQLSchemaFragmentsDeclarationHappyPath(t *testing.T) {
	c := qt.New(t)
	description, _, _ := sqlSchemaExchange()
	description.Features = []string{provider.NormalizeValuesFeature}
	description.Engines[0].Features = []string{"retention"}
	description.Engines[0].SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS}
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	description.Engines[0].SqlSchema = nil
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
}

func TestSQLSchemaRequestFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.Description, *protocol.ReadSQLSchemaRequest)
	}{
		{"service not advertised", func(d *protocol.Description, _ *protocol.ReadSQLSchemaRequest) { d.Features = nil }},
		{"foreign engine", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Engine = "foreign" }},
		{"engine in fragments mode", func(d *protocol.Description, _ *protocol.ReadSQLSchemaRequest) {
			d.Engines[0].SqlSchema.Mode = protocol.SQLSchemaDeclaration_FRAGMENTS
		}},
		{"unknown target feature", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.TargetFeatures = []string{"absent"} }},
		{"missing identifier rules", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Identifiers = nil }},
		{"no documents", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Documents = nil }},
		{"nil document", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Documents[0] = nil }},
		{"document without ID", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Documents[0].Id = " " }},
		{"duplicate document ID", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Documents[1].Id = "one" }},
		{"document without path", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Documents[0].Path = "" }},
		{"limit of an undeclared kind", func(_ *protocol.Description, r *protocol.ReadSQLSchemaRequest) { r.Limits[0].Kind = "view" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := sqlSchemaExchange()
			c.Assert(provider.ValidateReadSQLSchemaRequest(description, request), qt.IsNil)
			test.change(description, request)
			c.Assert(provider.ValidateReadSQLSchemaRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSQLSchemaResultFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.Description, *protocol.SQLSchema)
	}{
		{"missing declaration", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Declarations = r.Declarations[:1] }},
		{"extra declaration", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Declarations = append(r.Declarations, &protocol.SQLDocumentDeclarations{DocumentId: "three"})
		}},
		{"declarations out of order", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Declarations[0], r.Declarations[1] = r.Declarations[1], r.Declarations[0]
		}},
		{"declaration without a name", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Declarations[0].Objects[0].Name = "" }},
		{"table statement dropped and guarded", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Declarations[0].Tables[0].Drop, r.Declarations[0].Tables[0].Guarded = true, true
		}},
		{"table without a name", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Tables[0].Name = "" }},
		{"duplicate table", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
		{"duplicate column", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Tables[0].Columns[1].Name = "id" }},
		{"column without a type", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Tables[0].Columns[0].Type = "" }},
		{"namespace differs from its subject", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Namespaces[0].Name = "other" }},
		{"duplicate enum", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Enums = append(r.Enums, proto.CloneOf(r.Enums[0]))
		}},
		{"view without a body", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Views[0].Body = "" }},
		{"view named like a table", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Views[0].Name = "a" }},
		{"sequence with a zero increment", func(_ *protocol.Description, r *protocol.SQLSchema) {
			zero := int64(0)
			r.Sequences[0].Increment = &zero
		}},
		{"duplicate sequence", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Sequences = append(r.Sequences, proto.CloneOf(r.Sequences[0]))
		}},
		{"duplicate role", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Roles = append(r.Roles, proto.CloneOf(r.Roles[0]))
		}},
		{"grant without privileges", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Grants[0].Privileges = nil }},
		{"grant without a target kind", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Grants[0].Target.Kind = "" }},
		{"grant without a role", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Grants[0].Role = "" }},
		{"missing coverage", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Coverage = nil }},
		{"observed coverage", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Coverage.Representation = protocol.ValueInput_OBSERVED
		}},
		{"native object kind not covered", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Coverage.Kinds = nil }},
		{"foreign native owner", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.NativeObjects[0].ProviderId = "example.org/other"
		}},
		{"native object of an unknown model", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.NativeObjects[0].Kind = "example.org/fixture/other"
			r.NativeObjects[0].Owner.Kind = "example.org/fixture/other"
		}},
		{"native object of a changed definition", func(_ *protocol.Description, r *protocol.SQLSchema) { r.NativeObjects[0].SchemaVersion = 2 }},
		{"native object of a model without SQL", func(d *protocol.Description, r *protocol.SQLSchema) {
			formats := []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
			d.Engines[0].ValueModels[0].SourceFormats = formats
			r.Coverage.Kinds[0].Model.SourceFormats = formats
		}},
		{"native object identity differs from its kind", func(_ *protocol.Description, r *protocol.SQLSchema) { r.NativeObjects[0].Owner.Kind = "table" }},
		{"duplicate native object", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.NativeObjects = append(r.NativeObjects, proto.CloneOf(r.NativeObjects[0]))
		}},
		{"complete reply with an error", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Diagnostics[0].Severity = protocol.SQLDiagnostic_ERROR
		}},
		{"missing trace", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces = r.Traces[1:] }},
		{"repeated trace", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[1] = proto.CloneOf(r.Traces[0]) }},
		{"trace of no object", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[0].Index = 5 }},
		{"trace of no collection", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[0].Collection = 0 }},
		{"trace of a foreign document", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[0].Source.DocumentId = "foreign" }},
		{"trace naming a fragment", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[0].Source.FragmentId = "fragment" }},
		{"trace with a path", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Traces[0].Source.Path = []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Key{Key: "k"}}}
		}},
		{"trace with a root", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Traces[0].Source.Root = protocol.SourceTrace_CONTENT
		}},
		{"trace without a span", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[0].Source.Span = nil }},
		{"trace span past the text", func(_ *protocol.Description, r *protocol.SQLSchema) {
			r.Traces[0].Source.Span = sqlSpan(0, uint32(len(sqlFirstText))+1)
		}},
		{"trace span reversed", func(_ *protocol.Description, r *protocol.SQLSchema) { r.Traces[0].Source.Span = sqlSpan(8, 3) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, result := sqlSchemaExchange()
			c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
			test.change(description, result)
			c.Assert(provider.ValidateSQLSchema(description, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSQLSchemaMissingReplyFailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, _ := sqlSchemaExchange()
	c.Assert(provider.ValidateSQLSchema(description, request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func TestSQLSchemaDiagnosticsFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.SQLDiagnostic)
	}{
		{"unknown code", func(d *protocol.SQLDiagnostic) { d.Code = 0 }},
		{"unknown severity", func(d *protocol.SQLDiagnostic) { d.Severity = 0 }},
		{"foreign document", func(d *protocol.SQLDiagnostic) { d.DocumentId = "foreign" }},
		{"missing span", func(d *protocol.SQLDiagnostic) { d.Span = nil }},
		{"span past the text", func(d *protocol.SQLDiagnostic) { d.Span = sqlSpan(0, uint32(len(sqlSecondText))+1) }},
		{"empty message", func(d *protocol.SQLDiagnostic) { d.Message = "" }},
		{"message with a line break", func(d *protocol.SQLDiagnostic) { d.Message = "first\nsecond" }},
		{"message beyond the bound", func(d *protocol.SQLDiagnostic) { d.Message = strings.Repeat("x", provider.MaxSQLDiagnosticMessage+1) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := sqlSchemaExchange()
			diagnostic := &protocol.SQLDiagnostic{Code: protocol.SQLDiagnostic_INVALID, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: "two", Span: sqlSpan(0, 5), Message: "unknown column"}
			refused := &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{diagnostic}}
			c.Assert(provider.ValidateSQLSchema(description, request, refused), qt.IsNil)
			test.change(diagnostic)
			c.Assert(provider.ValidateSQLSchema(description, request, refused), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSQLSchemaIncompleteReplyFailurePath(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.SQLSchema)
	}{
		{"no diagnostic", func(r *protocol.SQLSchema) { r.Diagnostics = nil }},
		{"only a warning", func(r *protocol.SQLSchema) { r.Diagnostics[0].Severity = protocol.SQLDiagnostic_WARNING }},
		{"a table prefix", func(r *protocol.SQLSchema) { r.Tables = []*protocol.DeclaredTable{{Name: "a"}} }},
		{"a native object prefix", func(r *protocol.SQLSchema) { r.NativeObjects = []*protocol.Envelope{{}} }},
		{"declarations", func(r *protocol.SQLSchema) { r.Declarations = []*protocol.SQLDocumentDeclarations{{DocumentId: "one"}} }},
		{"coverage", func(r *protocol.SQLSchema) { r.Coverage = &protocol.ModelCoverage{} }},
		{"a view prefix", func(r *protocol.SQLSchema) { r.Views = []*protocol.SQLView{{Name: "v"}} }},
		{"a trace", func(r *protocol.SQLSchema) { r.Traces = []*protocol.SQLObjectTrace{{}} }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := sqlSchemaExchange()
			refused := &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{
				{Code: protocol.SQLDiagnostic_SYNTAX, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: "one", Span: sqlSpan(0, 5), Message: "unexpected token"},
			}}
			c.Assert(provider.ValidateSQLSchema(description, request, refused), qt.IsNil)
			test.change(refused)
			c.Assert(provider.ValidateSQLSchema(description, request, refused), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSQLSchemaFoldsNamesByTheEngineRules(t *testing.T) {
	c := qt.New(t)
	description, request, result := sqlSchemaExchange()
	request.Identifiers.Rules.TableNames = protocol.IdentifierRules_ASCII_CASE_INSENSITIVE
	request.Identifiers.Rules.ColumnNames = protocol.IdentifierRules_ASCII_CASE_INSENSITIVE
	result.Tables = append(result.Tables, &protocol.DeclaredTable{Name: "A"})
	result.Traces = append(result.Traces, sqlTrace("one", protocol.SQLObjectTrace_TABLES, 1))
	c.Assert(provider.ValidateSQLSchema(description, request, result), qt.ErrorIs, provider.ErrProtocol)
	request.Identifiers.Rules.TableNames = protocol.IdentifierRules_EXACT
	request.Identifiers.Rules.ColumnNames = protocol.IdentifierRules_EXACT
	c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
	result.Tables[0].Columns[1].Name = "ID"
	c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
	request.Identifiers.Rules.ColumnNames = protocol.IdentifierRules_ASCII_CASE_INSENSITIVE
	c.Assert(provider.ValidateSQLSchema(description, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestFragmentTraceRefusesAWholeDocumentSpan(t *testing.T) {
	c := qt.New(t)
	description, request, result := sourceExchange()
	c.Assert(provider.ValidateDecodedSource(description, request, result), qt.IsNil)
	result.Documents[0].Fragments[0].Values[0].Source.Span = sourceSpan()
	c.Assert(provider.ValidateDecodedSource(description, request, result), qt.ErrorIs, provider.ErrProtocol)
}
