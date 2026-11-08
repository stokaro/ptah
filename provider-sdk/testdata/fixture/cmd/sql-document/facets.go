package main

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// retentionKind is the fixture's retention value, which a table or a column
// holds as a facet.
const retentionKind = "example.org/fixture/retention"

// retentionStatement is the statement of this command's dialect that declares
// a facet: `ALTER TABLE name SET RETENTION n;` for the table, and `ALTER TABLE
// name ALTER column SET RETENTION n;` for one of its columns. The shared
// fixture dialect reads the rest of a document; it is built against the
// oldest SDK, so the facets live here.
var retentionStatement = regexp.MustCompile(`(?i)\bALTER\s+TABLE\s+(\w+)\s+(?:ALTER\s+(\w+)\s+)?SET\s+RETENTION\s+(\d+)\s*;`)

// declaredFacet is one retention statement and where it stands.
type declaredFacet struct {
	document             *protocol.SQLDocument
	start, end           int
	table, column, value string
}

// facetDescription is the DOCUMENT description of this command: the
// shared one, with schema.sql-facets and schema.read-sql-undeclared listed
// and SQL among the retention value's source formats.
func facetDescription() *protocol.Description {
	description := backend.SQLDocumentDescription()
	description.Features = append(description.Features, provider.SQLFacetsFeature, provider.SQLUndeclaredFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.SQLFacetsFeature, provider.SQLUndeclaredFeature)
	for _, model := range engine.ValueModels {
		if model.GetKind() == retentionKind && model.GetRepresentation() == protocol.ValueInput_DESIRED {
			model.SourceFormats = append(model.SourceFormats, protocol.SourceDocument_SQL)
		}
	}
	return description
}

// retentionModel is the desired retention model as this command declares it.
func retentionModel() *protocol.ValueModel {
	for _, model := range facetDescription().GetEngines()[0].GetValueModels() {
		if model.GetKind() == retentionKind && model.GetRepresentation() == protocol.ValueInput_DESIRED {
			return model
		}
	}
	panic("the fixture declares no desired retention model") // A static description; a missing model is a fixture programming error.
}

// ReadSQLSchema reads the retention statements of each document itself and
// the rest through the shared dialect, with the statements blanked so every
// other offset stays where it was. A complete reading then carries a facet of
// the table or the column each statement names, traced to it, and claims the
// retention model, so a table declared without one holds none. For a request
// that does not list schema.sql-facets a retention statement is refused by
// name, since the reply could not carry it. A view named `tuned` carries
// settings; see tuneViews. Where the host negotiated
// schema.read-sql-undeclared, an ALTER TABLE or a DROP TABLE of a table the
// load does not declare is refused as UNDECLARED, a code the shared dialect,
// built against the oldest SDK, cannot name.
func (p *documentProvider) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	stripped := proto.CloneOf(request)
	var declared []declaredFacet
	for i, document := range request.GetDocuments() {
		text := []byte(document.GetText())
		for _, match := range retentionStatement.FindAllStringSubmatchIndex(document.GetText(), -1) {
			declared = append(declared, declaredFacet{document: document, start: match[0], end: match[1], table: document.GetText()[match[2]:match[3]],
				column: submatch(document.GetText(), match[4], match[5]), value: document.GetText()[match[6]:match[7]]})
			for j := match[0]; j < match[1]; j++ {
				if text[j] != '\n' {
					text[j] = ' '
				}
			}
		}
		stripped.Documents[i].Text = string(text)
	}
	if len(declared) != 0 && !slices.Contains(request.GetTargetFeatures(), provider.SQLFacetsFeature) {
		return facetRefusal(declared[0], protocol.SQLDiagnostic_UNSUPPORTED, "a retention of a table needs "+provider.SQLFacetsFeature), nil
	}
	result, err := p.Provider.ReadSQLSchemaAs(ctx, stripped, undeclaredReading(request))
	if err != nil {
		return nil, err
	}
	tuneViews(result)
	if !result.GetComplete() || len(declared) == 0 {
		return result, nil
	}
	model := retentionModel()
	for _, facet := range declared {
		owner, refusal := facetOwner(result.GetTables(), facet)
		if refusal != nil {
			return refusal, nil
		}
		result.Traces = append(result.Traces, &protocol.SQLObjectTrace{Collection: protocol.SQLObjectTrace_FACETS, Index: uint32(len(result.Facets)), // #nosec G115 -- the index counts facets of one fixture reading
			Source: &protocol.SourceTrace{DocumentId: facet.document.GetId(), Span: facetSpan(facet)}})
		result.Facets = append(result.Facets, &protocol.Envelope{ProviderId: backend.Description().GetProviderId(), Kind: model.GetKind(), SchemaVersion: model.GetSchemaVersion(),
			Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: owner, Payload: []byte(`{"seconds":` + facet.value + `}`)})
	}
	result.Coverage.Kinds = append(result.Coverage.Kinds, &protocol.ModelKindCoverage{Model: model, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	return result, nil
}

// undeclaredReading reads a load with the UNDECLARED code where the host
// negotiated schema.read-sql-undeclared, and as the shared dialect does
// otherwise.
func undeclaredReading(request *protocol.ReadSQLSchemaRequest) backend.SQLReading {
	if slices.Contains(request.GetTargetFeatures(), provider.SQLUndeclaredFeature) {
		return backend.SQLReading{Undeclared: protocol.SQLDiagnostic_UNDECLARED}
	}
	return backend.SQLReading{}
}

func submatch(text string, start, end int) string {
	if start < 0 {
		return ""
	}
	return text[start:end]
}

// facetOwner names the declared table, or the column of it, a statement gives
// a retention, or refuses a name the reading does not declare.
func facetOwner(tables []*protocol.DeclaredTable, facet declaredFacet) (*protocol.Reference, *protocol.SQLSchema) {
	index := slices.IndexFunc(tables, func(table *protocol.DeclaredTable) bool {
		return backend.NormalizeCommonIdentifier(table.GetName()) == backend.NormalizeCommonIdentifier(facet.table)
	})
	if index < 0 {
		return nil, facetRefusal(facet, protocol.SQLDiagnostic_INVALID, "unknown table")
	}
	table := tables[index]
	owner := &protocol.Reference{Engine: "fixture", Kind: "table", Schema: table.GetSchema(), Name: table.GetName()}
	if facet.column == "" {
		return owner, nil
	}
	column := slices.IndexFunc(table.GetColumns(), func(column *protocol.DeclaredColumn) bool {
		return backend.NormalizeCommonIdentifier(column.GetName()) == backend.NormalizeCommonIdentifier(facet.column)
	})
	if column < 0 {
		return nil, facetRefusal(facet, protocol.SQLDiagnostic_INVALID, "unknown column")
	}
	return provider.ColumnReference(owner, table.GetColumns()[column].GetName()), nil
}

// facetRefusal refuses a load at a retention statement, with no schema.
func facetRefusal(facet declaredFacet, code protocol.SQLDiagnostic_Code, message string) *protocol.SQLSchema {
	return &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{{Code: code, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: facet.document.GetId(),
		Span: facetSpan(facet), Message: message}}}
}

// facetSpan is the statement's span, with one-based lines and byte columns.
func facetSpan(facet declaredFacet) *protocol.SourceSpan {
	text := facet.document.GetText()
	position := func(offset int) *protocol.SourcePosition {
		// #nosec G115 -- offsets index a fixture document
		return &protocol.SourcePosition{ByteOffset: uint64(offset), Line: uint32(1 + strings.Count(text[:offset], "\n")),
			Column: uint32(offset-strings.LastIndex(text[:offset], "\n")-1) + 1}
	}
	return &protocol.SourceSpan{Start: position(facet.start), End: position(facet.end)}
}
