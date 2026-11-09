package providersource

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"

	"ptah.run/core/sqlext"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// TableClause is one clause of CREATE TABLE a SQL document of an engine holds
// that a claim of the engine opens: the claim, the table the clause belongs
// to, the clause's text with its opening words, and where it starts in the
// document as written.
type TableClause struct {
	Claim  *protocol.SQLTableClause
	Schema string
	Table  string
	Text   string
	Start  sqlext.Position
}

// SQLDocument captures the claimed clauses of one SQL document of an engine
// as one document of the provider source contract: a binding for each table a
// clause belongs to, and a fragment for each clause, in the order of clauses,
// that carries the claim, binds and names its table and holds the clause's
// text. The document enrolls the models the clauses' claims declare, in the
// engine's order. length is the document's length in bytes and file names it,
// for a reader. The caller validates the request against the negotiated
// description.
func SQLDocument(ctx context.Context, engine *protocol.Engine, file string, length int, clauses []TableClause) (*protocol.DecodeSourceRequest, error) {
	if length < 0 {
		return nil, fmt.Errorf("a SQL document has a negative length")
	}
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_SQL, File: cmp.Or(file, "<unnamed SQL>"), ByteLength: uint64(length)}
	claimed := make(map[string]bool)
	for _, clause := range clauses {
		for _, kind := range clause.Claim.GetModels() {
			claimed[kind] = true
		}
	}
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED && claimed[model.GetKind()] && slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_SQL) {
			document.Models = append(document.Models, proto.CloneOf(model))
		}
	}
	bindings := make(map[[2]string]string)
	for i, clause := range clauses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		table := &protocol.Reference{Engine: engine.GetId(), Kind: "table", Schema: clause.Schema, Name: clause.Table}
		key := [2]string{clause.Schema, clause.Table}
		binding, bound := bindings[key]
		if !bound {
			binding = "table-" + strconv.Itoa(len(bindings))
			bindings[key] = binding
			document.Bindings = append(document.Bindings, &protocol.SourceBinding{Id: binding, Object: proto.CloneOf(table)})
		}
		span, err := clauseSpan(clause.Start, clause.Text)
		if err != nil {
			return nil, err
		}
		document.Fragments = append(document.Fragments, &protocol.SourceFragment{Id: strconv.Itoa(i), SqlTableClause: proto.CloneOf(clause.Claim),
			Selector: clause.Claim.GetSelector(), BindingId: binding, Subject: table, Span: span, Content: &protocol.SourceFragment_Sql{Sql: clause.Text}})
	}
	request := sourceRequest(engine, document)
	if err := provider.ValidateSourceDocuments(engine.GetId(), request.GetDocuments()); err != nil {
		return nil, err
	}
	return request, nil
}

// clauseSpan is the span of text, which starts at start: its end follows the
// text's own lines, which end as a document's do, at LF, CRLF or a lone CR.
func clauseSpan(start sqlext.Position, text string) (*protocol.SourceSpan, error) {
	line, column := start.Line, start.Column
	for i := range len(text) {
		if ch := text[i]; ch == '\n' || ch == '\r' && (i+1 == len(text) || text[i+1] != '\n') {
			line, column = line+1, 1
			continue
		}
		column++
	}
	begin, err := sourcePosition(start.Offset, start.Line, start.Column)
	if err != nil {
		return nil, err
	}
	end, err := sourcePosition(start.Offset+len(text), line, column)
	if err != nil {
		return nil, err
	}
	return &protocol.SourceSpan{Start: begin, End: end}, nil
}

// sourcePosition is a position of the wire, refused where it does not fit it.
func sourcePosition(offset, line, column int) (*protocol.SourcePosition, error) {
	if offset < 0 || line < 1 || column < 1 || line > math.MaxUint32 || column > math.MaxUint32 {
		return nil, fmt.Errorf("SQL clause position %d:%d at byte %d is outside what a source position holds", line, column, offset)
	}
	return &protocol.SourcePosition{ByteOffset: uint64(offset), Line: uint32(line), Column: uint32(column)}, nil
}
