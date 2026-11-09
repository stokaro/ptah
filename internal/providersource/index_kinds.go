package providersource

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/yamlext"
	"ptah.run/internal/tableref"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// IndexDeclaration is the frontend's own part of an index declaration whose
// type an engine reads in place of the frontend's common index, as a Go, YAML
// or HCL frontend read it. An index-kind fragment carries it in its inherited
// attributes.
type IndexDeclaration struct {
	// Name is the index's name, which the fragment's subject carries.
	Name string
	// Type is the type the declaration writes, as written.
	Type string
	// Columns are the key columns, in order, and Include the columns the
	// index covers.
	Columns, Include []string
	// Unique marks a unique index.
	Unique bool
	// Comment and Ops are the comment and the operator class the
	// declaration writes, or empty.
	Comment, Ops string
}

// inherited is the frontend's part of the declaration as an index-kind
// fragment carries it: type, columns, include, unique, comment and ops, each
// present only when the declaration writes it, all at span.
func (d IndexDeclaration) inherited(span *protocol.SourceSpan) *protocol.SourceMapping {
	text := func(value string) *protocol.SourceValue {
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Text{Text: value}}
	}
	sequence := func(values []string) *protocol.SourceValue {
		items := &protocol.SourceSequence{}
		for _, value := range values {
			items.Values = append(items.Values, text(value))
		}
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Sequence{Sequence: items}}
	}
	mapping := &protocol.SourceMapping{}
	add := func(key string, value *protocol.SourceValue) {
		mapping.Entries = append(mapping.Entries, &protocol.SourceEntry{Key: key, KeySpan: span, Value: value})
	}
	add(provider.IndexKindType, text(d.Type))
	add(provider.IndexKindColumns, sequence(d.Columns))
	if len(d.Include) != 0 {
		add(provider.IndexKindInclude, sequence(d.Include))
	}
	if d.Unique {
		add(provider.IndexKindUnique, &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Boolean{Boolean: true}})
	}
	if d.Comment != "" {
		add(provider.IndexKindComment, text(d.Comment))
	}
	if d.Ops != "" {
		add(provider.IndexKindOps, text(d.Ops))
	}
	return mapping
}

// indexKindFragment is the fragment of an index declaration of the claim's
// type: bound to the index's table, with the index as its subject, the
// frontend's part inherited and the owner's attributes as content.
func indexKindFragment(engine string, claim *protocol.SourceIndexKind, table *protocol.Reference, binding string, declaration IndexDeclaration,
	content *protocol.SourceMapping, span *protocol.SourceSpan) *protocol.SourceFragment {
	return &protocol.SourceFragment{IndexKind: proto.CloneOf(claim), Selector: claim.GetSelector(), BindingId: binding, Span: span,
		Subject:             &protocol.Reference{Engine: engine, Kind: "index", Schema: table.GetSchema(), Path: []string{table.GetName()}, Name: declaration.Name},
		InheritedAttributes: declaration.inherited(span),
		Content:             &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Mapping{Mapping: content}}}}
}

// RefuseUncarriedIndexAttributes refuses the attributes of the frontend's
// own an index declaration of an engine's claimed type writes beyond what an
// index-kind fragment carries: the engine reads the index in place of the
// frontend, and an attribute the fragment leaves out would be dropped. written
// lists them by name.
func RefuseUncarriedIndexAttributes(engine *protocol.Engine, declaration IndexDeclaration, written []string) error {
	if len(written) == 0 {
		return nil
	}
	return fmt.Errorf("index %q is a %q index, which engine %q reads in place of the frontend; the provider receives no %s of it",
		declaration.Name, declaration.Type, engine.GetId(), strings.Join(written, ", "))
}

// refuseIndexKindScope refuses an index-kind fragment of a model the
// document's format does not take: the provider could not decode the index in
// a document of that format, and the frontend would otherwise drop it. It
// returns the position of the fragment it refuses.
func refuseIndexKindScope(engine *protocol.Engine, document *protocol.SourceDocument) (int, error) {
	scope := make(map[string]bool, len(document.GetModels()))
	for _, model := range document.GetModels() {
		scope[model.GetKind()] = true
	}
	for position, fragment := range document.GetFragments() {
		for _, kind := range fragment.GetIndexKind().GetModels() {
			if !scope[kind] {
				return position, fmt.Errorf("index %q is a %q index, which engine %q reads into model %q, and a %s document does not take that model",
					fragment.GetSubject().GetName(), indexKindType(fragment), engine.GetId(), kind, strings.ToLower(document.GetFormat().String()))
			}
		}
	}
	return -1, nil
}

// indexKindType is the type an index-kind fragment's declaration writes.
func indexKindType(fragment *protocol.SourceFragment) string {
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		if entry.GetKey() == provider.IndexKindType {
			return entry.GetValue().GetText()
		}
	}
	return ""
}

// goIndexAttributes are the attributes of ptah:schema:index an index-kind
// fragment carries in its inherited part, beside table, which binds it.
var goIndexAttributes = []string{"name", "fields", "columns", "include", "unique", "comment", "type", "ops", "table"}

// GoIndexKind is the engine's index-kind claim of a Go declaration: a
// ptah:schema:index directive whose type the claim names, or nil.
func GoIndexKind(engine *protocol.Engine, declaration annotation.Declaration) *protocol.SourceIndexKind {
	if declaration.Directive != "ptah:schema:index" {
		return nil
	}
	return provider.IndexKindClaim(engine, declaration.Attributes["type"])
}

// goIndexKindFragment is the index-kind fragment of an index directive of a
// claimed type: the attributes the engine claims on ptah:schema:index are its
// content, as text, and the frontend's own are its inherited part.
func goIndexKindFragment(engine *protocol.Engine, claim *protocol.SourceIndexKind, tables annotation.Tables, bindings []string, index int,
	declaration annotation.Declaration) (*protocol.SourceFragment, error) {
	span, err := goSpan(declaration)
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	attributes := declaration.Attributes
	read := IndexDeclaration{Name: attributes["name"], Type: attributes["type"], Columns: splitNames(cmp.Or(attributes["fields"], attributes["columns"])),
		Include: splitNames(attributes["include"]), Unique: attributes["unique"] == "true", Comment: attributes["comment"], Ops: attributes["ops"]}
	owned := goClaim(engine, "ptah:schema:index").GetAttributes()
	content := &protocol.SourceMapping{}
	var uncarried []string
	for _, key := range slices.Sorted(maps.Keys(attributes)) {
		switch {
		case slices.Contains(owned, key):
			content.Entries = append(content.Entries, &protocol.SourceEntry{Key: key, KeySpan: span,
				Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Text{Text: attributes[key]}}})
		case !slices.Contains(goIndexAttributes, key):
			uncarried = append(uncarried, strconv.Quote(key)+" attribute")
		}
	}
	if err := RefuseUncarriedIndexAttributes(engine, read, uncarried); err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	if len(read.Columns) == 0 || slices.Contains(read.Columns, "") {
		return nil, &annotation.DeclarationError{Declaration: declaration, Attribute: "fields", Err: fmt.Errorf("index %q names no fields, or an empty one", read.Name)}
	}
	table, err := tables.Owning(declaration.Struct, attributes["table"], "the index directive")
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	fragment := indexKindFragment(engine.GetId(), claim, &protocol.Reference{Schema: tables[table].Schema, Name: tables[table].Name}, bindings[table], read,
		content, span)
	fragment.Id = strconv.Itoa(index)
	return fragment, nil
}

// splitNames reads a comma-separated list of names, trimmed, as the frontend
// reads an index's fields and include columns. An empty list has no names.
func splitNames(written string) []string {
	if strings.TrimSpace(written) == "" {
		return nil
	}
	names := strings.Split(written, ",")
	for i := range names {
		names[i] = strings.TrimSpace(names[i])
	}
	return names
}

// ReadIndexDeclaration reads the frontend's part of an index-kind fragment a
// provider wrote back: the index's name from the subject, and the type, the
// columns, the covered columns, uniqueness, the comment and the operator
// class from the inherited part, which the SDK has validated.
func ReadIndexDeclaration(fragment *protocol.SourceFragment) (IndexDeclaration, error) {
	declaration := IndexDeclaration{Name: fragment.GetSubject().GetName()}
	texts := func(value *protocol.SourceValue) []string {
		var names []string
		for _, item := range value.GetSequence().GetValues() {
			names = append(names, item.GetText())
		}
		return names
	}
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		value := entry.GetValue()
		switch entry.GetKey() {
		case provider.IndexKindType:
			declaration.Type = value.GetText()
		case provider.IndexKindColumns:
			declaration.Columns = texts(value)
		case provider.IndexKindInclude:
			declaration.Include = texts(value)
		case provider.IndexKindUnique:
			declaration.Unique = value.GetBoolean()
		case provider.IndexKindComment:
			declaration.Comment = value.GetText()
		case provider.IndexKindOps:
			declaration.Ops = value.GetText()
		default:
			return IndexDeclaration{}, fmt.Errorf("index %q carries attribute %q, which the frontend does not write", declaration.Name, entry.GetKey())
		}
	}
	return declaration, nil
}

// HCLIndexKind is the engine's index-kind claim of an HCL claim: an index
// block the frontend claimed whole for the type the claim names, or nil.
func HCLIndexKind(engine *protocol.Engine, claim hclext.Claim) *protocol.SourceIndexKind {
	if claim.Index == nil || claim.Index.Owned == nil {
		return nil
	}
	return provider.IndexKindClaim(engine, claim.Index.Type)
}

// hclIndexKindFragment is the index-kind fragment of an index block of a
// claimed type: the attributes of the engine's INDEX_ATTRIBUTES claim the
// block writes are its content, as their kinds read them, and what the
// frontend read of the block is its inherited part.
func hclIndexKindFragment(engine *protocol.Engine, claim hclext.Claim, bind func(schema, name string) string) (*protocol.SourceFragment, error) {
	kind := HCLIndexKind(engine, claim)
	if kind == nil {
		return nil, fmt.Errorf("engine %q reads no index of type %q", engine.GetId(), claim.Index.Type)
	}
	span, err := hclSpan(claim.Range)
	if err != nil {
		return nil, err
	}
	var shapes []*protocol.HCLSourceAttribute
	for _, block := range engine.GetHclSource().GetBlocks() {
		if block.GetSite() == protocol.HCLSourceBlock_INDEX_ATTRIBUTES {
			shapes = block.GetAttributes()
		}
	}
	content, err := hclMapping(engine.GetId(), shapes, claim.Attributes, span)
	if err != nil {
		return nil, err
	}
	table, valid := tableref.Parse(claim.Index.Table)
	if !valid {
		return nil, fmt.Errorf("index %q names no table", claim.Name)
	}
	owned := claim.Index.Owned
	declaration := IndexDeclaration{Name: claim.Name, Type: claim.Index.Type, Columns: owned.Columns, Include: owned.Include, Unique: owned.Unique,
		Comment: owned.Comment, Ops: claim.Index.Operator}
	return indexKindFragment(engine.GetId(), kind, &protocol.Reference{Schema: table.Schema, Name: table.Name}, bind(table.Schema, table.Name), declaration,
		content, span), nil
}

// addIndexKindFragment adds the index-kind fragment of a YAML index entry the
// frontend handed whole to document, as fragment position of the input, where
// the engine reads its type.
func (w *yamlWalk) addIndexKindFragment(engine *protocol.Engine, input yamlext.Document, document *protocol.SourceDocument, position int,
	fragment yamlext.Fragment) error {
	captured, err := w.indexKindFragment(engine, input, fragment)
	if err != nil {
		return fmt.Errorf("YAML %s at %v: %w", document.GetFile(), fragment.Path, err)
	}
	if captured != nil {
		captured.Id = strconv.Itoa(position)
		document.Fragments = append(document.Fragments, captured)
	}
	return nil
}

// indexKindFragment is the index-kind fragment of a YAML index entry the
// frontend handed whole, or nil when the engine does not read its type: the
// values of the engine's keys of index entries are its content, and what the
// frontend read of the entry its inherited part, all at the entry's span.
func (w *yamlWalk) indexKindFragment(engine *protocol.Engine, input yamlext.Document, fragment yamlext.Fragment) (*protocol.SourceFragment, error) {
	claim := provider.IndexKindClaim(engine, fragment.Index.Type)
	if claim == nil {
		return nil, nil
	}
	if fragment.Table < 0 || fragment.Table >= len(input.Tables) {
		return nil, fmt.Errorf("YAML index has no table binding")
	}
	span, err := yamlSpan(fragment.Node)
	if err != nil {
		return nil, err
	}
	content := &protocol.SourceMapping{}
	for _, key := range slices.Sorted(maps.Keys(fragment.Index.Owned)) {
		value, err := w.value(fragment.Index.Owned[key], 0)
		if err != nil {
			return nil, err
		}
		content.Entries = append(content.Entries, &protocol.SourceEntry{Key: key, KeySpan: value.GetSpan(), Value: value})
	}
	table := input.Tables[fragment.Table]
	declaration := IndexDeclaration{Name: fragment.Name, Type: fragment.Index.Type, Columns: fragment.Index.Columns, Include: fragment.Index.Include,
		Unique: fragment.Index.Unique, Comment: fragment.Index.Comment, Ops: fragment.Index.Ops}
	return indexKindFragment(engine.GetId(), claim, &protocol.Reference{Schema: table.Schema, Name: table.Name}, table.Key, declaration, content, span), nil
}
