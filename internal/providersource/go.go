package providersource

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/annotation"
	"ptah.run/internal/annotationmeta"
)

// GoClaim is one Go directive a provider owns and the engine that declared it.
type GoClaim struct {
	// Engine is the provider engine whose models the directive declares.
	Engine *protocol.Engine
	// Directive is the claim as the provider negotiated it.
	Directive *protocol.GoSourceDirective
	// Attributes reports that the claim names one of the frontend's own
	// directives: the provider owns the listed attributes of that directive,
	// not a directive of its own.
	Attributes bool
}

// ownDirectiveScopes are the frontend directives a provider may add attributes
// to, with the claim scope each takes: the directive declares a table, a column
// or an index.
var ownDirectiveScopes = map[string]protocol.GoSourceDirective_Scope{
	"ptah:schema:table": protocol.GoSourceDirective_TABLE,
	"ptah:schema:field": protocol.GoSourceDirective_COLUMN,
	"ptah:schema:index": protocol.GoSourceDirective_INDEX,
}

// frontendDirectives are the directives the Go annotation frontend reads
// without an owner.
var frontendDirectives = sync.OnceValue(func() annotationmeta.Catalog {
	catalog, err := annotationmeta.NewCatalog(annotation.None())
	if err != nil {
		panic(err) // The empty selection is always valid.
	}
	return catalog
})

// GoClaims indexes the Go directives of a provider's engines by directive
// name. A name belongs to one engine, because the host picks the engine by the
// name a comment carries.
//
// A claim names either a directive of the provider's own, at document, table,
// column or index scope, or one of the frontend's own directives. In the second
// case the provider owns the attributes the claim lists and the claim's scope
// is the part the directive declares: table for ptah:schema:table, column for
// ptah:schema:field and index for ptah:schema:index. A claim on another
// frontend directive, a scope that does not fit the directive, a claim without
// attributes and an attribute the directive already has are refused, since the
// frontend would otherwise drop the attributes without an error.
func GoClaims(engines []*protocol.Engine) (map[string]GoClaim, error) {
	claims := make(map[string]GoClaim)
	for _, engine := range engines {
		for _, directive := range engine.GetGoSourceDirectives() {
			attributes, err := checkGoClaim(engine, directive)
			if err != nil {
				return nil, err
			}
			if previous, taken := claims[directive.GetDirective()]; taken {
				return nil, fmt.Errorf("Go directive %q is claimed by engines %q and %q; a directive name has one owner",
					directive.GetDirective(), previous.Engine.GetId(), engine.GetId())
			}
			claims[directive.GetDirective()] = GoClaim{Engine: engine, Directive: directive, Attributes: attributes}
		}
	}
	return claims, nil
}

// checkGoClaim refuses a claim the host cannot read, and reports whether it
// adds attributes to one of the frontend's own directives.
func checkGoClaim(engine *protocol.Engine, claim *protocol.GoSourceDirective) (attributes bool, err error) {
	name := claim.GetDirective()
	own, isOwn := frontendDirectives().Lookup(name)
	if !isOwn {
		if len(claim.GetInherited()) != 0 {
			// A provider's own directive sits beside the frontend's, whose
			// attributes its declaration does not carry.
			return false, fmt.Errorf("Go directive %q of engine %q inherits attributes; only a claim on ptah:schema:table, ptah:schema:field or ptah:schema:index reads the attributes of the directive it extends",
				name, engine.GetId())
		}
		switch claim.GetScope() {
		case protocol.GoSourceDirective_DOCUMENT, protocol.GoSourceDirective_TABLE, protocol.GoSourceDirective_COLUMN, protocol.GoSourceDirective_INDEX:
			return false, nil
		default:
			return false, fmt.Errorf("Go directive %q of engine %q has scope %s, which the host does not read", name, engine.GetId(), claim.GetScope())
		}
	}
	want, supported := ownDirectiveScopes[name]
	switch {
	case !supported:
		return true, fmt.Errorf("Go directive %q of engine %q adds attributes to a frontend directive the host does not read for providers: only ptah:schema:table, ptah:schema:field and ptah:schema:index take them",
			name, engine.GetId())
	case claim.GetScope() != want:
		return true, fmt.Errorf("Go directive %q of engine %q has scope %s, but the attributes of %s belong at scope %s", name, engine.GetId(), claim.GetScope(), name, want)
	case len(claim.GetAttributes()) == 0:
		return true, fmt.Errorf("Go directive %q of engine %q adds no attribute to a frontend directive", name, engine.GetId())
	}
	for _, attribute := range claim.GetAttributes() {
		if slices.ContainsFunc(own.Attributes, func(existing annotation.Attribute) bool { return existing.Name == attribute }) {
			return true, fmt.Errorf("Go directive %q of engine %q adds attribute %q, which the directive declares already", name, engine.GetId(), attribute)
		}
	}
	for _, attribute := range claim.GetInherited() {
		if !slices.ContainsFunc(own.Attributes, func(existing annotation.Attribute) bool { return existing.Name == attribute }) {
			return true, fmt.Errorf("Go directive %q of engine %q inherits attribute %q, which the directive does not declare", name, engine.GetId(), attribute)
		}
	}
	return true, nil
}

// GoAnnotations captures the Go directives one file declares for one engine as
// a GO_ANNOTATIONS document. The host has already lexed each comment: a
// fragment carries the attributes as text, never the comment, and the claim the
// provider negotiated. A table-scope directive is bound to the table its
// table attribute names, or to the table of its struct where it has none. Every declaration must name a directive the engine claims, and all
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
	// The attributes of the directive itself that the claim inherits travel
	// apart from the claim's own: they stay the frontend's.
	mapping, inherited := &protocol.SourceMapping{}, &protocol.SourceMapping{}
	for _, key := range slices.Sorted(maps.Keys(declaration.Attributes)) {
		entry := &protocol.SourceEntry{Key: key, KeySpan: span,
			Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Text{Text: declaration.Attributes[key]}}}
		if slices.Contains(claim.GetInherited(), key) {
			inherited.Entries = append(inherited.Entries, entry)
			continue
		}
		mapping.Entries = append(mapping.Entries, entry)
	}
	fragment := &protocol.SourceFragment{Id: strconv.Itoa(index), GoDirective: proto.CloneOf(claim), Selector: claim.GetSelector(), Span: span,
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Mapping{Mapping: mapping}}}}
	if len(inherited.GetEntries()) != 0 {
		fragment.InheritedAttributes = inherited
	}
	table, subject, err := GoSubject(engine.GetId(), tables, claim, declaration)
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	if table >= 0 {
		fragment.BindingId, fragment.Subject = bindings[table], subject
	}
	return fragment, nil
}

// GoSubject locates the part of a table a Go declaration describes at the
// claim's scope. It returns the position in tables of the table the declaration
// is bound to and, for a column or an index, the reference the provider
// receives as the fragment's subject: kind column or index, the table's schema,
// the table as the only path component, and the name the field or index
// directive writes. A table-scope declaration binds to the table its table
// attribute names, resolved as the table attribute of an index directive is,
// and to the table of its struct where it writes none. A table that the file
// does not declare, or declares in more than one schema, is refused. A
// document-scope claim has no table and returns -1.
//
// The caller compares the owner of a value the provider decoded with this
// reference, so a column directive cannot declare a setting of another column.
func GoSubject(engine string, tables annotation.Tables, claim *protocol.GoSourceDirective, declaration annotation.Declaration) (table int, subject *protocol.Reference, err error) {
	switch claim.GetScope() {
	case protocol.GoSourceDirective_TABLE:
		table, err = tables.Owning(declaration.Struct, tableAttribute(declaration), "the "+declaration.Directive+" directive")
		return table, nil, err
	case protocol.GoSourceDirective_COLUMN, protocol.GoSourceDirective_INDEX:
		kind := annotation.SubjectColumn
		if claim.GetScope() == protocol.GoSourceDirective_INDEX {
			kind = annotation.SubjectIndex
		}
		if declaration.Subject.Kind != kind || declaration.Subject.Name == "" {
			return -1, nil, fmt.Errorf("Go declaration %q carries no %s it describes", declaration.Directive, kind)
		}
		if table, err = tables.Owning(declaration.Struct, declaration.Subject.Table, "the "+string(kind)+" directive"); err != nil {
			return -1, nil, err
		}
		return table, &protocol.Reference{Engine: engine, Kind: string(kind), Schema: tables[table].Schema,
			Path: []string{tables[table].Name}, Name: declaration.Subject.Name}, nil
	default:
		return -1, nil, nil
	}
}

// tableAttribute returns the table a table-scope directive names with its
// table attribute, resolved by [annotation.Tables.Owning] as the frontend
// resolves the table attribute of an index directive. It is empty, meaning the
// table of the declaration's struct, where the attribute is absent. An
// attribute that an owner adds to the frontend's own table directive is not
// a reference: that directive names its table with name.
func tableAttribute(declaration annotation.Declaration) string {
	if declaration.Subject.Kind == annotation.SubjectTable {
		return ""
	}
	return declaration.Attributes["table"]
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

// SameSubject reports whether a provider's reference names the same table
// part as the one the host sent. The engine and display name are not part of
// the identity: the provider may normalize neither.
func SameSubject(got, want *protocol.Reference) bool {
	if got == nil || want == nil {
		return got == want
	}
	return got.GetKind() == want.GetKind() && got.GetDatabase() == want.GetDatabase() && got.GetCatalog() == want.GetCatalog() &&
		got.GetSchema() == want.GetSchema() && slices.Equal(got.GetPath(), want.GetPath()) && got.GetName() == want.GetName() &&
		slices.Equal(got.GetOverload(), want.GetOverload())
}
