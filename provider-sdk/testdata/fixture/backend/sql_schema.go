package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// RetentionObjectKind is the model of a retention object the fixture's SQL
// dialect declares on its own, outside any table.
const RetentionObjectKind = "example.org/fixture/retention-object"

// RetentionHeaderKind is the header kind a `ptah:not-described` header of a
// fixture SQL document names to leave retention objects undescribed.
const RetentionHeaderKind = "retention"

// SQLDocumentDescription declares the fixture engine in the DOCUMENT mode: the
// fixture reads every SQL schema document of the engine whole, in the narrow
// dialect [Provider.ReadSQLSchema] describes, and declares retention objects
// as a model of its own whose source format is SQL.
func SQLDocumentDescription() *protocol.Description {
	description := Description()
	description.SemanticIdentity += "-sql-document"
	description.Features = append(description.Features, provider.ReadSQLSchemaFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.ReadSQLSchemaFeature)
	engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_DOCUMENT, HeaderKinds: []string{RetentionHeaderKind}}
	engine.ValueModels = append(engine.ValueModels, retentionObjectModels()...)
	return description
}

func retentionObjectModels() []*protocol.ValueModel {
	definition, err := modeljson.Canonical([]byte(`{"encoding":"json","kind":"` + RetentionObjectKind + `","value":"seconds, one nonnegative integer","schema_version":1}`))
	if err != nil {
		panic(err)
	} // Static owner definition; invalid JSON is a fixture programming error.
	digest := sha256.Sum256(definition)
	var models []*protocol.ValueModel
	for _, representation := range []protocol.ValueInput_Representation{protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED} {
		models = append(models, &protocol.ValueModel{DisplayName: "retention objects", Kind: RetentionObjectKind, Representation: representation,
			SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]),
			CanonicalFormat: "example.org/fixture/retention/seconds-json-v1"})
	}
	models[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}
	return models
}

// ReadSQLSchema reads the fixture's SQL dialect, one statement per semicolon:
//
//	CREATE TABLE [IF NOT EXISTS] name (column type [NOT NULL] [PRIMARY KEY], ...);
//	ALTER TABLE name ADD column type [NOT NULL];
//	DROP TABLE name;
//	CREATE RETENTION name SECONDS n;
//
// A line comment, such as a `ptah:not-described` header, is skipped. Every
// other statement is refused as UNSUPPORTED and named by its first word. Names
// compare without regard to ASCII case, as the engine's identifier rules say.
// Documents run in order, so a later one alters what an earlier one declared.
//
// Three table names select a reply shape for the host's tests: `warned` adds
// a warning to a complete reading, `malformed` leaves its table without the
// trace the protocol requires, and `stalled` waits for cancellation. This
// fixture demonstrates the protocol, not an engine's grammar.
func (*Provider) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	load := &sqlLoad{}
	declarations := make([]*protocol.SQLDocumentDeclarations, 0, len(request.GetDocuments()))
	for _, document := range request.GetDocuments() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declarations = append(declarations, load.run(document))
	}
	if load.stalled {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if len(load.diagnostics) != 0 {
		return &protocol.SQLSchema{Diagnostics: load.diagnostics}, nil
	}
	result := &protocol.SQLSchema{Complete: true, Declarations: declarations, Coverage: load.coverage(request.GetLimits())}
	for i, entry := range load.tables {
		result.Tables = append(result.Tables, entry.table)
		if NormalizeCommonIdentifier(entry.table.GetName()) != "malformed" {
			result.Traces = append(result.Traces, sqlTrace(protocol.SQLObjectTrace_TABLES, i, entry.declared))
		}
	}
	for i, entry := range load.retentions {
		result.NativeObjects = append(result.NativeObjects, entry.object)
		result.Traces = append(result.Traces, sqlTrace(protocol.SQLObjectTrace_NATIVE_OBJECTS, i, entry.declared))
	}
	if load.warned != nil {
		result.Diagnostics = append(result.Diagnostics, &protocol.SQLDiagnostic{Code: protocol.SQLDiagnostic_INVALID, Severity: protocol.SQLDiagnostic_WARNING,
			DocumentId: load.warned.document, Span: load.warned.span, Message: "fixture warns about table warned"})
	}
	return result, nil
}

// sqlPlace is where a statement stands: its document and its span.
type sqlPlace struct {
	document string
	span     *protocol.SourceSpan
}

type sqlTable struct {
	table    *protocol.DeclaredTable
	declared sqlPlace
}

type sqlRetention struct {
	object   *protocol.Envelope
	declared sqlPlace
}

type sqlLoad struct {
	tables      []*sqlTable
	retentions  []*sqlRetention
	diagnostics []*protocol.SQLDiagnostic
	warned      *sqlPlace
	stalled     bool
}

func sqlTrace(collection protocol.SQLObjectTrace_Collection, index int, place sqlPlace) *protocol.SQLObjectTrace {
	// #nosec G115 -- the index counts objects of one fixture reading
	return &protocol.SQLObjectTrace{Collection: collection, Index: uint32(index), Source: &protocol.SourceTrace{DocumentId: place.document, Span: place.span}}
}

func (l *sqlLoad) find(name string) int {
	for i, entry := range l.tables {
		if NormalizeCommonIdentifier(entry.table.GetName()) == NormalizeCommonIdentifier(name) {
			return i
		}
	}
	return -1
}

func (l *sqlLoad) refuse(document *protocol.SQLDocument, code protocol.SQLDiagnostic_Code, span *protocol.SourceSpan, message string) {
	l.diagnostics = append(l.diagnostics, &protocol.SQLDiagnostic{Code: code, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: document.GetId(), Span: span, Message: message})
}

// run reads one document's statements in order and returns what it declares.
func (l *sqlLoad) run(document *protocol.SQLDocument) *protocol.SQLDocumentDeclarations {
	declared := &protocol.SQLDocumentDeclarations{DocumentId: document.GetId()}
	text := document.GetText()
	offset := 0
	for _, raw := range strings.SplitAfter(withoutComments(text), ";") {
		start := offset + len(raw) - len(strings.TrimLeft(raw, " \t\r\n"))
		offset += len(raw)
		statement := strings.TrimSuffix(strings.TrimSpace(raw), ";")
		if statement == "" {
			continue
		}
		place := sqlPlace{document: document.GetId(), span: sqlSpan(text, start, start+len(statement))}
		l.statement(document, declared, place, strings.FieldsFunc(statement, func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == '(' || r == ')' || r == ','
		}))
	}
	return declared
}

// withoutComments blanks every line comment, keeping each byte's offset.
func withoutComments(text string) string {
	lines := strings.SplitAfter(text, "\n")
	for i, line := range lines {
		if index := strings.Index(line, "--"); index >= 0 {
			lines[i] = line[:index] + strings.Repeat(" ", len(strings.TrimSuffix(line[index:], "\n"))) + line[len(strings.TrimSuffix(line, "\n")):]
		}
	}
	return strings.Join(lines, "")
}

func (l *sqlLoad) statement(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, place sqlPlace, fields []string) {
	words := make([]string, len(fields))
	for i, field := range fields {
		words[i] = strings.ToUpper(field)
	}
	opening := strings.Join(words[:min(2, len(words))], " ")
	switch {
	case opening == "CREATE TABLE" && len(words) > 3:
		l.create(document, declared, place, fields)
	case opening == "DROP TABLE" && len(words) == 3:
		l.drop(document, declared, place, fields[2])
	case opening == "ALTER TABLE" && len(words) >= 6 && words[3] == "ADD":
		l.alter(document, place, fields[2], fields[4:])
	case opening == "CREATE RETENTION" && len(words) == 5 && words[3] == "SECONDS":
		l.retention(document, declared, place, fields[2], fields[4])
	default:
		start := int(place.span.GetStart().GetByteOffset()) // #nosec G115 -- the offset indexes a fixture document
		l.refuse(document, protocol.SQLDiagnostic_UNSUPPORTED, sqlSpan(document.GetText(), start, start+len(fields[0])), "unsupported statement "+words[0])
	}
}

func (l *sqlLoad) create(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, place sqlPlace, fields []string) {
	rest, guarded := fields[2:], false
	if len(rest) > 3 && strings.EqualFold(strings.Join(rest[:3], " "), "IF NOT EXISTS") {
		rest, guarded = rest[3:], true
	}
	name := rest[0]
	declared.Tables = append(declared.Tables, &protocol.SQLTableStatement{Name: name, Guarded: guarded})
	if l.find(name) >= 0 {
		if !guarded {
			l.refuse(document, protocol.SQLDiagnostic_INVALID, place.span, "table already exists")
		}
		return
	}
	table := &protocol.DeclaredTable{Name: name}
	columns, ok := sqlColumns(rest[1:])
	if !ok {
		l.refuse(document, protocol.SQLDiagnostic_SYNTAX, place.span, "malformed column list")
		return
	}
	for _, column := range columns {
		table.Columns = append(table.Columns, column)
		if column.GetPrimary() {
			table.PrimaryKey = append(table.PrimaryKey, column.GetName())
		}
	}
	l.tables = append(l.tables, &sqlTable{table: table, declared: place})
	declared.Objects = append(declared.Objects, &protocol.SQLDeclaration{Kind: "table", Name: name})
	if guarded {
		declared.Guards = append(declared.Guards, &protocol.SQLDeclaration{Kind: "table", Name: name})
	}
	switch NormalizeCommonIdentifier(name) {
	case "warned":
		l.warned = &place
	case "stalled":
		l.stalled = true
	}
}

// sqlColumns reads `column type [NOT NULL] [PRIMARY KEY]` definitions, split
// into words.
func sqlColumns(words []string) ([]*protocol.DeclaredColumn, bool) {
	var columns []*protocol.DeclaredColumn
	for i := 0; i < len(words); {
		if i+1 >= len(words) {
			return nil, false
		}
		column := &protocol.DeclaredColumn{Name: words[i], Type: strings.ToUpper(words[i+1]), Nullable: true}
		i += 2
		for i+1 < len(words) && sqlModifier(column, words[i], words[i+1]) {
			i += 2
		}
		columns = append(columns, column)
	}
	return columns, len(columns) > 0
}

// sqlModifier applies a two-word column modifier and reports whether the words
// were one.
func sqlModifier(column *protocol.DeclaredColumn, first, second string) bool {
	switch strings.ToUpper(first + " " + second) {
	case "NOT NULL":
		column.Nullable = false
	case "PRIMARY KEY":
		column.Primary, column.Nullable = true, false
	default:
		return false
	}
	return true
}

func (l *sqlLoad) drop(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, place sqlPlace, name string) {
	declared.Tables = append(declared.Tables, &protocol.SQLTableStatement{Name: name, Drop: true})
	index := l.find(name)
	if index < 0 {
		l.refuse(document, protocol.SQLDiagnostic_INVALID, place.span, "unknown table")
		return
	}
	l.tables = append(l.tables[:index], l.tables[index+1:]...)
	kept := declared.Objects[:0]
	for _, object := range declared.Objects {
		if NormalizeCommonIdentifier(object.GetName()) != NormalizeCommonIdentifier(name) {
			kept = append(kept, object)
		}
	}
	declared.Objects = kept
}

func (l *sqlLoad) alter(document *protocol.SQLDocument, place sqlPlace, name string, definition []string) {
	index := l.find(name)
	if index < 0 {
		l.refuse(document, protocol.SQLDiagnostic_INVALID, place.span, "unknown table")
		return
	}
	columns, ok := sqlColumns(definition)
	if !ok || len(columns) != 1 || columns[0].GetPrimary() {
		l.refuse(document, protocol.SQLDiagnostic_SYNTAX, place.span, "ALTER TABLE adds one column without a key")
		return
	}
	table := l.tables[index].table
	table.Columns = append(table.Columns, columns[0])
}

func (l *sqlLoad) retention(document *protocol.SQLDocument, declared *protocol.SQLDocumentDeclarations, place sqlPlace, name, seconds string) {
	if _, err := strconv.ParseUint(seconds, 10, 64); err != nil {
		l.refuse(document, protocol.SQLDiagnostic_INVALID, place.span, "retention requires a nonnegative number of seconds")
		return
	}
	model := retentionObjectModels()[0]
	payload, err := json.Marshal(map[string]json.RawMessage{"seconds": json.RawMessage(seconds)})
	if err != nil {
		l.refuse(document, protocol.SQLDiagnostic_INVALID, place.span, "retention cannot be encoded")
		return
	}
	l.retentions = append(l.retentions, &sqlRetention{object: &protocol.Envelope{ProviderId: Description().GetProviderId(), Kind: model.GetKind(), SchemaVersion: model.GetSchemaVersion(),
		Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: retentionOwner(name), Payload: payload}, declared: place})
	declared.Objects = append(declared.Objects, &protocol.SQLDeclaration{Kind: "retention", Name: name})
}

func retentionOwner(name string) *protocol.Reference {
	return &protocol.Reference{Engine: "fixture", Kind: RetentionObjectKind, Name: name}
}

// coverage claims every retention object of the load once the load declares
// one or its header names the kind: a whole-kind limit leaves the kind
// undescribed, and a named limit leaves that object undescribed. A load that
// says nothing about retention claims nothing about it.
func (l *sqlLoad) coverage(limits []*protocol.SchemaCoverageLimit) *protocol.ModelCoverage {
	result := &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}
	if len(l.retentions) == 0 && len(limits) == 0 {
		return result
	}
	knowledge := &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
	for _, limit := range limits {
		reason := "the document's header leaves it undescribed"
		if limit.GetName() == "" {
			knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: reason}
			continue
		}
		result.Subjects = append(result.Subjects, &protocol.ModelSubjectCoverage{Kind: RetentionObjectKind, Subject: retentionOwner(limit.GetName()),
			NormalizedSubject: retentionOwner(limit.GetName()), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: reason}})
	}
	result.Kinds = []*protocol.ModelKindCoverage{{Model: proto.CloneOf(retentionObjectModels()[0]), Knowledge: knowledge}}
	return result
}

// sqlSpan converts byte offsets into a span with one-based lines and byte
// columns.
func sqlSpan(text string, from, to int) *protocol.SourceSpan {
	position := func(offset int) *protocol.SourcePosition {
		line := 1 + strings.Count(text[:offset], "\n")
		// #nosec G115 -- offsets index a fixture document
		return &protocol.SourcePosition{ByteOffset: uint64(offset), Line: uint32(line), Column: uint32(offset-strings.LastIndex(text[:offset], "\n")-1) + 1}
	}
	return &protocol.SourceSpan{Start: position(from), End: position(to)}
}

var _ provider.SQLSchemaReader = (*Provider)(nil)
