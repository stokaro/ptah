package providersource

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/annotation"
)

// GoClaim is one Go directive a provider owns and the engine that declared it.
type GoClaim struct {
	// Engine is the provider engine whose models the directive declares.
	Engine *protocol.Engine
	// Directive is the claim as the provider negotiated it.
	Directive *protocol.GoSourceDirective
}

// GoClaims indexes the Go directives of a provider's engines by directive
// name. A name belongs to one engine, because the host picks the engine by the
// name a comment carries. The host reads a directive at document and table
// scope; a claim at column or index scope is refused, since the frontend does
// not yet hand a decoder the column or index a comment sits on, and accepting
// the claim would drop the attributes without an error.
func GoClaims(engines []*protocol.Engine) (map[string]GoClaim, error) {
	claims := make(map[string]GoClaim)
	for _, engine := range engines {
		for _, directive := range engine.GetGoSourceDirectives() {
			switch directive.GetScope() {
			case protocol.GoSourceDirective_DOCUMENT, protocol.GoSourceDirective_TABLE:
			default:
				return nil, fmt.Errorf("Go directive %q of engine %q has scope %s, which the host does not read: only document and table scope are hosted",
					directive.GetDirective(), engine.GetId(), directive.GetScope())
			}
			if previous, taken := claims[directive.GetDirective()]; taken {
				return nil, fmt.Errorf("Go directive %q is claimed by engines %q and %q; a directive name has one owner",
					directive.GetDirective(), previous.Engine.GetId(), engine.GetId())
			}
			claims[directive.GetDirective()] = GoClaim{Engine: engine, Directive: directive}
		}
	}
	return claims, nil
}

// GoAnnotations captures the Go directives one file declares for one engine as
// a GO_ANNOTATIONS document. The host has already lexed each comment: a
// fragment carries the attributes as text, never the comment, and the claim the
// provider negotiated. A table-scope directive is bound to the table of its
// struct. Every declaration must name a directive the engine claims, and all
// must come from one file. Without declarations the document only enrolls the
// engine's Go models, which makes the provider state what a Go source covers.
// The caller validates the request against the negotiated description.
func GoAnnotations(ctx context.Context, engine *protocol.Engine, tables annotation.Tables, declarations []annotation.Declaration) (*protocol.DecodeSourceRequest, error) {
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_GO_ANNOTATIONS, File: "<unnamed Go>"}
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED && slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_GO_ANNOTATIONS) {
			document.Models = append(document.Models, proto.CloneOf(model))
		}
	}
	for i, declaration := range declarations {
		if i == 0 {
			document.File, document.ByteLength = cmp.Or(declaration.File, document.File), uint64(max(declaration.FileSize, 0))
		} else if declaration.File != declarations[0].File {
			return nil, fmt.Errorf("Go declarations of %q and %q cannot share a document", declarations[0].File, declaration.File)
		}
	}
	bindings := make([]string, len(tables))
	for i, table := range tables {
		bindings[i] = "table-" + strconv.Itoa(i)
		document.Bindings = append(document.Bindings, &protocol.SourceBinding{Id: bindings[i], StructName: table.Struct,
			Object: &protocol.Reference{Engine: engine.GetId(), Kind: "table", Schema: table.Schema, Name: table.Name}})
	}
	for i, declaration := range declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fragment, err := goFragment(engine, tables, bindings, i, declaration)
		if err != nil {
			return nil, err
		}
		document.Fragments = append(document.Fragments, fragment)
	}
	request := &protocol.DecodeSourceRequest{Engine: engine.GetId(), Documents: []*protocol.SourceDocument{document}}
	if err := provider.ValidateSourceDocuments(engine.GetId(), request.GetDocuments()); err != nil {
		return nil, err
	}
	return request, nil
}

func goFragment(engine *protocol.Engine, tables annotation.Tables, bindings []string, index int, declaration annotation.Declaration) (*protocol.SourceFragment, error) {
	claim := goClaim(engine, declaration.Directive)
	if claim == nil {
		return nil, fmt.Errorf("engine %q claims no Go directive %q", engine.GetId(), declaration.Directive)
	}
	span, err := goSpan(declaration)
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	mapping := &protocol.SourceMapping{}
	for _, key := range slices.Sorted(maps.Keys(declaration.Attributes)) {
		mapping.Entries = append(mapping.Entries, &protocol.SourceEntry{Key: key, KeySpan: span,
			Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Text{Text: declaration.Attributes[key]}}})
	}
	fragment := &protocol.SourceFragment{Id: strconv.Itoa(index), GoDirective: proto.CloneOf(claim), Selector: claim.GetSelector(), Span: span,
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Mapping{Mapping: mapping}}}}
	if claim.GetScope() == protocol.GoSourceDirective_TABLE {
		table, err := tables.Owning(declaration.Struct, "", "the table directive")
		if err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		fragment.BindingId = bindings[table]
	}
	return fragment, nil
}

func goClaim(engine *protocol.Engine, directive string) *protocol.GoSourceDirective {
	for _, claim := range engine.GetGoSourceDirectives() {
		if claim.GetDirective() == directive {
			return claim
		}
	}
	return nil
}

// goSpan converts the position of the declaration's comment. The comment is the
// finest location the frontend knows: the span of an attribute is the span of
// the comment that wrote it.
func goSpan(declaration annotation.Declaration) (*protocol.SourceSpan, error) {
	start, end := declaration.Start, declaration.End
	if start.Line <= 0 || start.Column <= 0 || start.Offset < 0 || end.Offset < start.Offset || end.Offset > declaration.FileSize {
		return nil, fmt.Errorf("Go declaration %q carries no usable source position", declaration.Directive)
	}
	first, err := goPosition(start)
	if err != nil {
		return nil, err
	}
	last, err := goPosition(end)
	if err != nil {
		return nil, err
	}
	return &protocol.SourceSpan{Start: first, End: last}, nil
}

func goPosition(position annotation.SourcePosition) (*protocol.SourcePosition, error) {
	if position.Offset < 0 || position.Line <= 0 || position.Column <= 0 || int64(position.Line) > math.MaxUint32 || int64(position.Column) > math.MaxUint32 {
		return nil, fmt.Errorf("Go source coordinate exceeds protocol bounds")
	}
	return &protocol.SourcePosition{ByteOffset: uint64(position.Offset), Line: uint32(position.Line), Column: uint32(position.Column)}, nil
}
