package provider

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// IndexKindsFeature enables Engine.index_kinds and SourceFragment.index_kind:
// the index types whose declarations an engine reads in place of the
// frontend's common index, and the fragment that carries such a declaration
// whole. It is a two-way feature. A provider lists it, and so does each engine
// that claims a type, beside DecodeSourceFeature. Contract 25.24.
const IndexKindsFeature = "source.index-kinds"

// Index-kind attributes are the keys of the frontend's own part of an index
// declaration, which a fragment of an index-kind claim carries in its
// inherited attributes, in this order.
const (
	// IndexKindType is the declaration's type, as written.
	IndexKindType = "type"
	// IndexKindColumns is the sequence of the index's key columns, in order.
	IndexKindColumns = "columns"
	// IndexKindInclude is the sequence of the columns the index covers.
	IndexKindInclude = "include"
	// IndexKindUnique is true for a unique index, and absent for any other.
	IndexKindUnique = "unique"
	// IndexKindComment is the index's comment.
	IndexKindComment = "comment"
	// IndexKindOps is the operator class the declaration names.
	IndexKindOps = "ops"
)

// indexKindAttributes are the keys of a fragment's inherited attributes, in
// the order the fragment carries them.
func indexKindAttributes() []string {
	return []string{IndexKindType, IndexKindColumns, IndexKindInclude, IndexKindUnique, IndexKindComment, IndexKindOps}
}

// NormalizeIndexType returns a written index type as an index-kind claim
// lists it: in ASCII lower case, trimmed, with every run of white space read
// as one space. A frontend compares a declaration's type with a claim's types
// in this form.
func NormalizeIndexType(written string) string {
	return strings.Join(strings.Fields(asciiLower(written)), " ")
}

func asciiLower(text string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + 'a' - 'A'
		}
		return r
	}, text)
}

// IndexKindClaim returns the claim of engine whose types hold the written
// type, or nil when no claim of the engine names it.
func IndexKindClaim(engine *protocol.Engine, written string) *protocol.SourceIndexKind {
	normalized := NormalizeIndexType(written)
	if normalized == "" {
		return nil
	}
	for _, claim := range engine.GetIndexKinds() {
		if slices.Contains(claim.GetTypes(), normalized) {
			return claim
		}
	}
	return nil
}

// validateIndexKinds checks an engine's index-kind claims: present exactly
// when the engine lists the feature, which needs the provider's and source
// decoding, each type in the form NormalizeIndexType returns and claimed once
// in the engine, and each model a desired model of the engine.
func validateIndexKinds(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), IndexKindsFeature)
	if listed && (!slices.Contains(description.GetFeatures(), IndexKindsFeature) || !slices.Contains(description.GetFeatures(), DecodeSourceFeature)) {
		return sourceError("index-kind claims need the provider's " + IndexKindsFeature + " and source decoding")
	}
	if listed != (len(engine.GetIndexKinds()) != 0) {
		return sourceError("an engine claims index kinds exactly when it lists " + IndexKindsFeature)
	}
	claimed := make(map[string]bool)
	for _, claim := range engine.GetIndexKinds() {
		if len(claim.GetTypes()) == 0 || strings.TrimSpace(claim.GetSelector()) == "" || len(claim.GetModels()) == 0 {
			return sourceError("incomplete index-kind claim")
		}
		for _, written := range claim.GetTypes() {
			if written == "" || NormalizeIndexType(written) != written || claimed[written] {
				return sourceError("an index type is empty, not normalized, or claimed twice")
			}
			claimed[written] = true
		}
		for i, kind := range claim.GetModels() {
			if findValueModel(description, engine.GetId(), kind, protocol.ValueInput_DESIRED) == nil || slices.Contains(claim.GetModels()[:i], kind) {
				return sourceError("an index-kind claim names an unknown or duplicate desired model")
			}
		}
	}
	return nil
}

// negotiateIndexKinds removes the index-kind claims unless the host lists
// IndexKindsFeature.
func negotiateIndexKinds(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, IndexKindsFeature) {
		engine.IndexKinds = nil
	}
}

// validateIndexKindFragments binds the index-kind fragments of a document to
// the engine's negotiated claims: the claim and its selector unchanged, its
// models in the document's scope, the index's table as the binding and the
// index as the subject, the frontend's part in the inherited attributes and
// the engine's claimed attributes of the format's index declaration in the
// content.
func validateIndexKindFragments(description *protocol.Description, engine string, document *protocol.SourceDocument, models map[string]bool) error {
	var declaring *protocol.Engine
	for _, declared := range description.GetEngines() {
		if declared.GetId() == engine {
			declaring = declared
		}
	}
	for _, fragment := range document.GetFragments() {
		claim := fragment.GetIndexKind()
		if claim == nil {
			continue
		}
		if document.GetFormat() == protocol.SourceDocument_SQL {
			return sourceError("an index-kind fragment in a SQL document")
		}
		if !slices.ContainsFunc(declaring.GetIndexKinds(), func(declared *protocol.SourceIndexKind) bool { return proto.Equal(claim, declared) }) ||
			fragment.GetSelector() != claim.GetSelector() {
			return sourceError("index-kind fragment changed its negotiated claim")
		}
		for _, kind := range claim.GetModels() {
			if !models[kind] {
				return sourceError("index-kind fragment model is outside document scope")
			}
		}
		if err := validateIndexKindBinding(document, fragment); err != nil {
			return err
		}
		if err := validateIndexKindInherited(claim, fragment.GetInheritedAttributes()); err != nil {
			return err
		}
		mapping := fragment.GetValue().GetMapping()
		if mapping == nil {
			return sourceError("index-kind fragment content must be a mapping")
		}
		attributes := indexDeclarationAttributes(declaring, document.GetFormat())
		for _, entry := range mapping.GetEntries() {
			if !slices.Contains(attributes, entry.GetKey()) {
				return sourceError("index-kind fragment attribute is not one the engine claims on the index declaration")
			}
		}
	}
	return nil
}

// validateIndexKindBinding requires a table binding of the document and an
// index of that table as the subject.
func validateIndexKindBinding(document *protocol.SourceDocument, fragment *protocol.SourceFragment) error {
	table := indexKindTable(document.GetBindings(), fragment)
	subject := fragment.GetSubject()
	if table == nil || subject.GetKind() != "index" || subject.GetName() == "" || !slices.Equal(subject.GetPath(), []string{table.GetName()}) ||
		subject.GetSchema() != table.GetSchema() || subject.GetDatabase() != table.GetDatabase() || subject.GetCatalog() != table.GetCatalog() {
		return sourceError("index-kind fragment needs its table's binding and an index of that table as its subject")
	}
	return nil
}

// indexKindTable returns the table a fragment's binding names, or nil.
func indexKindTable(bindings []*protocol.SourceBinding, fragment *protocol.SourceFragment) *protocol.Reference {
	for _, binding := range bindings {
		if binding.GetId() == fragment.GetBindingId() && fragment.GetBindingId() != "" && binding.GetObject().GetKind() == "table" {
			return binding.GetObject()
		}
	}
	return nil
}

// validateIndexKindInherited requires the frontend's part of the declaration:
// a type the claim names and the key columns, then at most the covered
// columns, a true unique, a comment and an operator class, in that order.
func validateIndexKindInherited(claim *protocol.SourceIndexKind, inherited *protocol.SourceMapping) error {
	order := indexKindAttributes()
	position := -1
	written := make(map[string]bool)
	for _, entry := range inherited.GetEntries() {
		at := slices.Index(order, entry.GetKey())
		if at <= position {
			return sourceError("index-kind attribute is unknown, repeated or out of order")
		}
		position = at
		written[entry.GetKey()] = true
		value := entry.GetValue()
		var valid bool
		switch entry.GetKey() {
		case IndexKindType:
			valid = slices.Contains(claim.GetTypes(), NormalizeIndexType(value.GetText()))
		case IndexKindColumns:
			valid = columnNames(value) && len(value.GetSequence().GetValues()) != 0
		case IndexKindInclude:
			valid = columnNames(value)
		case IndexKindUnique:
			valid = value.GetBoolean()
		default:
			_, valid = value.GetValue().(*protocol.SourceValue_Text)
		}
		if !valid {
			return sourceError("index-kind attribute " + entry.GetKey() + " has no valid value")
		}
	}
	if !written[IndexKindType] || !written[IndexKindColumns] {
		return sourceError("index-kind fragment needs its type and its columns")
	}
	return nil
}

// columnNames reports whether a value is a sequence of non-empty texts.
func columnNames(value *protocol.SourceValue) bool {
	sequence := value.GetSequence()
	if sequence == nil {
		return false
	}
	for _, item := range sequence.GetValues() {
		if item.GetText() == "" {
			return false
		}
	}
	return true
}

// indexDeclarationAttributes returns the attributes an engine claims on the
// frontend's index declaration of a format: those of its INDEX-scope Go claim
// on ptah:schema:index, its INDEX-scope YAML keys, or its INDEX_ATTRIBUTES HCL
// claim.
func indexDeclarationAttributes(engine *protocol.Engine, format protocol.SourceDocument_Format) []string {
	var attributes []string
	switch format {
	case protocol.SourceDocument_GO_ANNOTATIONS:
		for _, claim := range engine.GetGoSourceDirectives() {
			if claim.GetScope() == protocol.GoSourceDirective_INDEX && claim.GetDirective() == "ptah:schema:index" {
				attributes = append(attributes, claim.GetAttributes()...)
			}
		}
	case protocol.SourceDocument_YAML:
		for _, claim := range engine.GetYamlSourceKeys() {
			if claim.GetScope() == protocol.YAMLSourceKey_INDEX {
				attributes = append(attributes, claim.GetKey())
			}
		}
	case protocol.SourceDocument_HCL:
		for _, block := range engine.GetHclSource().GetBlocks() {
			if block.GetSite() == protocol.HCLSourceBlock_INDEX_ATTRIBUTES {
				attributes = append(attributes, hclAttributeNames(block.GetAttributes())...)
			}
		}
	}
	return attributes
}

// validateIndexKindValue requires a value decoded from an index-kind fragment
// to be a facet owned by the index's table.
func validateIndexKindValue(document *protocol.SourceDocument, fragment *protocol.SourceFragment, value *protocol.SourceModelValue) error {
	if fragment.GetIndexKind() == nil {
		return nil
	}
	if value.GetPlacement() != protocol.SourceModelValue_FACET || !sameTable(value.GetValue().GetValue().GetOwner(), indexKindTable(document.GetBindings(), fragment)) {
		return sourceError("a value of an index-kind fragment is a facet of the index's table")
	}
	return nil
}

// sameTable reports whether owner names the table table names.
func sameTable(owner, table *protocol.Reference) bool {
	return table != nil && owner.GetKind() == "table" && owner.GetName() == table.GetName() && owner.GetSchema() == table.GetSchema() &&
		owner.GetDatabase() == table.GetDatabase() && owner.GetCatalog() == table.GetCatalog() && slices.Equal(owner.GetPath(), table.GetPath())
}
