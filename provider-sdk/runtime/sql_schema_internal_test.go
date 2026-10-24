package runtime

// White-box testing required: malformed non-SDK peers must not bypass client
// validation, a provider must not mutate the original validation input, and the
// loopback exercises both transport boundaries without a process.

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func sqlSchemaDescription() *protocol.Description {
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1",
		Features: []string{provider.ReadSQLSchemaFeature},
		Engines: []*protocol.Engine{{
			Id: "test", Identifiers: rules, Features: []string{provider.ReadSQLSchemaFeature},
			SqlSchema: &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_DOCUMENT},
		}},
	}
}

func sqlSchemaRequest(texts ...string) *protocol.ReadSQLSchemaRequest {
	request := &protocol.ReadSQLSchemaRequest{
		Engine:      "test",
		Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(sqlSchemaDescription().Engines[0].Identifiers)},
	}
	for i, text := range texts {
		request.Documents = append(request.Documents, &protocol.SQLDocument{Id: []string{"one", "two", "three"}[i], Path: []string{"one.sql", "two.sql", "three.sql"}[i], Text: text})
	}
	return request
}

// loopback sends a client call through the server adapter, as a process would,
// without the process.
type loopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
}

func (l *loopback) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest, _ ...grpc.CallOption) (*protocol.SQLSchema, error) {
	l.calls++
	return l.server.ReadSQLSchema(ctx, request)
}

func newSQLLoopback(reader provider.SQLSchemaReader) (*Client, *loopback) {
	description := sqlSchemaDescription()
	peer := &loopback{server: &serverAdapter{server: Server{Description: description, SQLSchemaReader: reader}}}
	return &Client{description: description, rpc: peer}, peer
}

func TestSQLSchemaConformanceHappyPath(t *testing.T) {
	c := qt.New(t)
	client, peer := newSQLLoopback(&dialectReader{})
	request := sqlSchemaRequest(
		"CREATE TABLE users (id Int);\nCREATE TABLE IF NOT EXISTS audit (id Int);\n",
		"ALTER TABLE users ADD name Text;\nDROP TABLE audit;\nCREATE TABLE audit (id Int, at Text);\n",
	)
	result, err := client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(peer.calls, qt.Equals, 1)
	var names []string
	var columns []int
	for _, table := range result.Tables {
		names = append(names, table.Name)
		columns = append(columns, len(table.Columns))
	}
	c.Assert(names, qt.DeepEquals, []string{"users", "audit"})
	c.Assert(columns, qt.DeepEquals, []int{2, 2})
	c.Assert(result.Declarations, qt.HasLen, 2)
	first, second := result.Declarations[0], result.Declarations[1]
	c.Assert(first.DocumentId, qt.Equals, "one")
	c.Assert(first.Objects, qt.HasLen, 2)
	c.Assert(first.Guards, qt.HasLen, 1)
	c.Assert(first.Guards[0].Name, qt.Equals, "audit")
	c.Assert(first.Tables, qt.HasLen, 2)
	c.Assert(first.Tables[1].Guarded, qt.IsTrue)
	c.Assert(second.DocumentId, qt.Equals, "two")
	c.Assert(second.Tables, qt.HasLen, 2)
	c.Assert(second.Tables[0].Drop, qt.IsTrue)
	c.Assert(second.Objects, qt.HasLen, 1)
	c.Assert(second.Objects[0].Name, qt.Equals, "audit")
	c.Assert(result.Traces[1].Source.DocumentId, qt.Equals, "two")
}

func TestSQLSchemaConformanceHonorsLoadOrder(t *testing.T) {
	c := qt.New(t)
	client, _ := newSQLLoopback(&dialectReader{})
	alter, create := "ALTER TABLE users ADD name Text;\n", "CREATE TABLE users (id Int);\n"
	inOrder, err := client.ReadSQLSchema(c.Context(), sqlSchemaRequest(create, alter))
	c.Assert(err, qt.IsNil)
	c.Assert(inOrder.Complete, qt.IsTrue)
	reversed, err := client.ReadSQLSchema(c.Context(), sqlSchemaRequest(alter, create))
	c.Assert(err, qt.IsNil)
	c.Assert(reversed.Complete, qt.IsFalse)
	c.Assert(reversed.Diagnostics, qt.HasLen, 1)
	c.Assert(reversed.Diagnostics[0].DocumentId, qt.Equals, "one")
	c.Assert(reversed.Diagnostics[0].Code, qt.Equals, protocol.SQLDiagnostic_INVALID)
}

func TestSQLSchemaConformanceReportsPositionedRefusals(t *testing.T) {
	c := qt.New(t)
	client, _ := newSQLLoopback(&dialectReader{})
	result, err := client.ReadSQLSchema(c.Context(), sqlSchemaRequest("CREATE TABLE a (id Int);\nGRANT x TO y;\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	diagnostic := result.Diagnostics[0]
	c.Assert(diagnostic.Code, qt.Equals, protocol.SQLDiagnostic_UNSUPPORTED)
	c.Assert(diagnostic.Severity, qt.Equals, protocol.SQLDiagnostic_ERROR)
	c.Assert(diagnostic.Message, qt.Equals, "unsupported statement GRANT")
	c.Assert(proto.Equal(diagnostic.Span, &protocol.SourceSpan{
		Start: &protocol.SourcePosition{ByteOffset: 25, Line: 2, Column: 1},
		End:   &protocol.SourcePosition{ByteOffset: 30, Line: 2, Column: 6},
	}), qt.IsTrue)
}

func TestSQLSchemaRefusedBatchExposesNoPrefix(t *testing.T) {
	c := qt.New(t)
	client, _ := newSQLLoopback(&dialectReader{})
	result, err := client.ReadSQLSchema(c.Context(), sqlSchemaRequest("CREATE TABLE a (id Int);\n", "GRANT x TO y;\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Tables, qt.HasLen, 0)
	c.Assert(result.Declarations, qt.HasLen, 0)
	c.Assert(result.Traces, qt.HasLen, 0)
	leaky, _ := newSQLLoopback(&leakingReader{})
	got, err := leaky.ReadSQLSchema(c.Context(), sqlSchemaRequest("CREATE TABLE a (id Int);\n"))
	c.Assert(err, qt.ErrorMatches, `provider read SQL schema failed \(Internal\)`)
	c.Assert(got, qt.IsNil)
}

func TestSQLSchemaCanceledCallExposesNothing(t *testing.T) {
	c := qt.New(t)
	client, peer := newSQLLoopback(&dialectReader{})
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	result, err := client.ReadSQLSchema(canceled, sqlSchemaRequest("CREATE TABLE a (id Int);\n"))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 0)
	during, stop := context.WithCancel(c.Context())
	server := &serverAdapter{server: Server{Description: sqlSchemaDescription(), SQLSchemaReader: &cancelingReader{cancel: stop}}}
	result, err = server.ReadSQLSchema(during, sqlSchemaRequest("CREATE TABLE a (id Int);\n"))
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
}

func TestSQLSchemaClientValidatesNonSDKResponses(t *testing.T) {
	c := qt.New(t)
	request := sqlSchemaRequest("CREATE TABLE a (id Int);\n")
	valid := &protocol.SQLSchema{Complete: true,
		Tables:       []*protocol.DeclaredTable{{Name: "a", Columns: []*protocol.DeclaredColumn{{Name: "id", Type: "Int"}}}},
		Declarations: []*protocol.SQLDocumentDeclarations{{DocumentId: "one"}},
		Coverage:     &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED},
		Traces: []*protocol.SQLObjectTrace{{Collection: protocol.SQLObjectTrace_TABLES, Source: &protocol.SourceTrace{DocumentId: "one",
			Span: &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 5, Line: 1, Column: 6}}}}},
	}
	peer := &sqlSchemaPeer{result: valid}
	client := &Client{description: sqlSchemaDescription(), rpc: peer}
	result, err := client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result = &protocol.SQLSchema{Complete: true}
	result, err = client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider read SQL schema failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
	request.Engine = "foreign"
	result, err = client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

func TestSQLSchemaServerKeepsCapturedInputImmutable(t *testing.T) {
	c := qt.New(t)
	request := sqlSchemaRequest("CREATE TABLE a (id Int);\n")
	original := proto.CloneOf(request)
	server := &serverAdapter{server: Server{Description: sqlSchemaDescription(), SQLSchemaReader: &mutatingSQLReader{}}}
	result, err := server.ReadSQLSchema(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
}

func TestSQLSchemaServerRefusesBeforeOwnerCode(t *testing.T) {
	c := qt.New(t)
	owner := &countingSQLReader{}
	server := &serverAdapter{server: Server{Description: sqlSchemaDescription(), SQLSchemaReader: owner}}
	request := sqlSchemaRequest("CREATE TABLE a (id Int);\n")
	request.Documents[0].Path = ""
	result, err := server.ReadSQLSchema(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
	absent := &serverAdapter{server: Server{Description: sqlSchemaDescription()}}
	result, err = absent.ReadSQLSchema(c.Context(), sqlSchemaRequest("CREATE TABLE a (id Int);\n"))
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	c.Assert(result, qt.IsNil)
}

func TestSQLSchemaServerRequiresAdvertisedReader(t *testing.T) {
	c := qt.New(t)
	server := Server{Description: sqlSchemaDescription(), Sessions: &sessionsStub{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.SQLSchemaReader = (*dialectReader)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.SQLSchemaReader = &dialectReader{}
	c.Assert(server.Validate(), qt.IsNil)
	server.Description.Features = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type sessionsStub struct{}

func (*sessionsStub) Open(context.Context, *protocol.OpenRequest) (*protocol.Session, error) {
	return nil, nil
}
func (*sessionsStub) Close(context.Context, *protocol.CloseRequest) error { return nil }

type sqlSchemaPeer struct {
	protocol.ProviderClient
	result *protocol.SQLSchema
	err    error
	calls  int
}

func (p *sqlSchemaPeer) ReadSQLSchema(context.Context, *protocol.ReadSQLSchemaRequest, ...grpc.CallOption) (*protocol.SQLSchema, error) {
	p.calls++
	return p.result, p.err
}

type mutatingSQLReader struct{}

func (*mutatingSQLReader) ReadSQLSchema(_ context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	request.Documents[0].Id = "changed"
	return &protocol.SQLSchema{Complete: true}, nil
}

type countingSQLReader struct{ calls int }

func (r *countingSQLReader) ReadSQLSchema(context.Context, *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	r.calls++
	return &protocol.SQLSchema{Complete: true}, nil
}

// leakingReader refuses the load but still returns a table.
type leakingReader struct{}

func (*leakingReader) ReadSQLSchema(context.Context, *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	return &protocol.SQLSchema{
		Tables: []*protocol.DeclaredTable{{Name: "a"}},
		Diagnostics: []*protocol.SQLDiagnostic{{Code: protocol.SQLDiagnostic_INVALID, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: "one",
			Span: &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 1, Line: 1, Column: 2}}, Message: "refused"}},
	}, nil
}

type cancelingReader struct{ cancel context.CancelFunc }

func (r *cancelingReader) ReadSQLSchema(context.Context, *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	r.cancel()
	return &protocol.SQLSchema{Complete: true}, nil
}

// dialectReader reads a tiny table dialect, enough to test load order, later
// documents that alter earlier ones, declarations and positioned refusals:
//
//	CREATE TABLE [IF NOT EXISTS] name (column type, ...);
//	DROP TABLE name;
//	ALTER TABLE name ADD column type;
//
// Every other statement is refused as UNSUPPORTED.
type dialectReader struct{}

type dialectTable struct {
	table    *protocol.DeclaredTable
	document string
	span     *protocol.SourceSpan
}

type dialectLoad struct {
	tables      []*dialectTable
	diagnostics []*protocol.SQLDiagnostic
}

func (r *dialectReader) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	load := &dialectLoad{}
	declarations := make([]*protocol.SQLDocumentDeclarations, 0, len(request.GetDocuments()))
	for _, document := range request.GetDocuments() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declarations = append(declarations, load.run(document))
	}
	if len(load.diagnostics) != 0 {
		return &protocol.SQLSchema{Diagnostics: load.diagnostics}, nil
	}
	result := &protocol.SQLSchema{Complete: true, Declarations: declarations, Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}
	for i, entry := range load.tables {
		result.Tables = append(result.Tables, entry.table)
		result.Traces = append(result.Traces, &protocol.SQLObjectTrace{Collection: protocol.SQLObjectTrace_TABLES, Index: uint32(i),
			Source: &protocol.SourceTrace{DocumentId: entry.document, Span: entry.span}})
	}
	return result, nil
}

func (l *dialectLoad) find(name string) int {
	for i, entry := range l.tables {
		if entry.table.Name == name {
			return i
		}
	}
	return -1
}

func (l *dialectLoad) refuse(document *protocol.SQLDocument, code protocol.SQLDiagnostic_Code, span *protocol.SourceSpan, message string) {
	l.diagnostics = append(l.diagnostics, &protocol.SQLDiagnostic{Code: code, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: document.GetId(), Span: span, Message: message})
}

func (l *dialectLoad) run(document *protocol.SQLDocument) *protocol.SQLDocumentDeclarations {
	declared := &protocol.SQLDocumentDeclarations{DocumentId: document.GetId()}
	offset := 0
	for _, raw := range strings.SplitAfter(document.GetText(), ";") {
		start := offset + len(raw) - len(strings.TrimLeft(raw, " \t\r\n"))
		offset += len(raw)
		statement := strings.TrimSuffix(strings.TrimSpace(raw), ";")
		if statement == "" {
			continue
		}
		span := dialectSpan(document.GetText(), start, start+len(statement))
		words := strings.FieldsFunc(strings.ToUpper(statement), func(r rune) bool { return r == ' ' || r == '(' || r == ')' || r == ',' })
		l.statement(document, declared, span, statement, words)
	}
	return declared
}

func (l *dialectLoad) statement(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, span *protocol.SourceSpan, statement string, words []string) {
	fields := strings.FieldsFunc(statement, func(r rune) bool { return r == ' ' || r == '(' || r == ')' || r == ',' })
	switch {
	case len(words) > 3 && strings.Join(words[:2], " ") == "CREATE TABLE":
		l.create(document, declared, span, fields)
	case len(words) == 3 && strings.Join(words[:2], " ") == "DROP TABLE":
		l.drop(document, declared, span, fields[2])
	case len(words) == 6 && strings.Join(words[:2], " ")+" "+words[3] == "ALTER TABLE ADD":
		l.alter(document, span, fields)
	default:
		start := int(span.Start.ByteOffset) // #nosec G115 -- the offset was made from a test text of a few bytes
		first := dialectSpan(document.GetText(), start, start+len(fields[0]))
		l.refuse(document, protocol.SQLDiagnostic_UNSUPPORTED, first, "unsupported statement "+words[0])
	}
}

func (l *dialectLoad) create(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, span *protocol.SourceSpan, fields []string) {
	rest, guarded := fields[2:], false
	if len(rest) > 3 && strings.EqualFold(strings.Join(rest[:3], " "), "IF NOT EXISTS") {
		rest, guarded = rest[3:], true
	}
	name := rest[0]
	declared.Tables = append(declared.Tables, &protocol.SQLTableStatement{Name: name, Guarded: guarded})
	if l.find(name) >= 0 {
		if !guarded {
			l.refuse(document, protocol.SQLDiagnostic_INVALID, span, "table already exists")
		}
		return
	}
	table := &protocol.DeclaredTable{Name: name}
	for i := 1; i+1 < len(rest); i += 2 {
		table.Columns = append(table.Columns, &protocol.DeclaredColumn{Name: rest[i], Type: rest[i+1]})
	}
	l.tables = append(l.tables, &dialectTable{table: table, document: document.GetId(), span: span})
	declared.Objects = append(declared.Objects, &protocol.SQLDeclaration{Kind: "table", Name: name})
	if guarded {
		declared.Guards = append(declared.Guards, &protocol.SQLDeclaration{Kind: "table", Name: name})
	}
}

func (l *dialectLoad) drop(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, span *protocol.SourceSpan, name string) {
	declared.Tables = append(declared.Tables, &protocol.SQLTableStatement{Name: name, Drop: true})
	index := l.find(name)
	if index < 0 {
		l.refuse(document, protocol.SQLDiagnostic_INVALID, span, "unknown table")
		return
	}
	l.tables = append(l.tables[:index], l.tables[index+1:]...)
	declared.Objects = slicesDeleteDeclaration(declared.Objects, name)
}

func (l *dialectLoad) alter(document *protocol.SQLDocument, span *protocol.SourceSpan, fields []string) {
	index := l.find(fields[2])
	if index < 0 {
		l.refuse(document, protocol.SQLDiagnostic_INVALID, span, "unknown table")
		return
	}
	table := l.tables[index].table
	table.Columns = append(table.Columns, &protocol.DeclaredColumn{Name: fields[4], Type: fields[5]})
}

func slicesDeleteDeclaration(list []*protocol.SQLDeclaration, name string) []*protocol.SQLDeclaration {
	kept := list[:0]
	for _, declaration := range list {
		if declaration.Name != name {
			kept = append(kept, declaration)
		}
	}
	return kept
}

// dialectSpan converts byte offsets into a span with one-based lines and byte
// columns.
func dialectSpan(text string, from, to int) *protocol.SourceSpan {
	position := func(offset int) *protocol.SourcePosition {
		line := 1 + strings.Count(text[:offset], "\n")
		// #nosec G115 -- offsets index a test text of a few bytes
		return &protocol.SourcePosition{ByteOffset: uint64(offset), Line: uint32(line), Column: uint32(offset-strings.LastIndex(text[:offset], "\n")-1) + 1}
	}
	return &protocol.SourceSpan{Start: position(from), End: position(to)}
}
