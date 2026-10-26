package provider

import (
	"encoding/json"
	"fmt"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

const (
	// MaxSourceDepth bounds nested evaluated values and blocks before transport.
	MaxSourceDepth = 64
	// MaxSourceNodes bounds the complete source batch, independently of byte size.
	MaxSourceNodes = 65536
)

// ValidateSourceDocuments validates a captured frontend batch without reading
// files, evaluating expressions, or interpreting owner syntax. It retains exact
// number text and refuses ambiguous mappings and missing authored positions.
// Inherited defaults may lack positions because the source did not write them.
func ValidateSourceDocuments(engine string, documents []*protocol.SourceDocument) error {
	if !validName(engine) {
		return sourceError("invalid engine")
	}
	batch := sourceWalk{engine: engine, values: make(map[*protocol.SourceValue]bool), blocks: make(map[*protocol.SourceBlock]bool)}
	ids := make(map[string]bool)
	for _, document := range documents {
		if document.GetId() == "" || ids[document.GetId()] || document.GetFile() == "" {
			return sourceError("missing or duplicate document identity")
		}
		ids[document.GetId()] = true
		if err := batch.document(document); err != nil {
			return err
		}
	}
	return nil
}

type sourceWalk struct {
	engine string
	length uint64
	nodes  int
	values map[*protocol.SourceValue]bool
	blocks map[*protocol.SourceBlock]bool
	// encoded walks fragments an owner wrote, which carry no position.
	encoded bool
}

type sourceOrigin uint8

const (
	authoredSource sourceOrigin = iota
	inheritedSource
)

func sourceError(reason string) error {
	return fmt.Errorf("%w: source batch: %s", ErrProtocol, reason)
}

func (w *sourceWalk) document(document *protocol.SourceDocument) error {
	if err := w.node(0); err != nil {
		return err
	}
	w.length = document.GetByteLength()
	switch document.GetFormat() {
	case protocol.SourceDocument_SQL, protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS:
	default:
		return sourceError("unknown source format")
	}
	if len(document.GetLimits()) != 0 && document.GetFormat() != protocol.SourceDocument_HCL && document.GetFormat() != protocol.SourceDocument_GO_ANNOTATIONS {
		return sourceError("header limits require HCL or Go annotations")
	}
	bindings := make(map[string]bool)
	for _, binding := range document.GetBindings() {
		if err := w.node(0); err != nil {
			return err
		}
		if binding.GetId() == "" || bindings[binding.GetId()] || !validValueReference(binding.GetObject(), w.engine) {
			return sourceError("invalid or duplicate source binding")
		}
		bindings[binding.GetId()] = true
	}
	fragments := make(map[string]bool)
	for _, fragment := range document.GetFragments() {
		if fragment.GetId() == "" || fragments[fragment.GetId()] || fragment.GetSelector() == "" {
			return sourceError("missing or duplicate fragment identity")
		}
		fragments[fragment.GetId()] = true
		if fragment.GetBindingId() != "" && !bindings[fragment.GetBindingId()] {
			return sourceError("unknown source binding")
		}
		if fragment.GetSubject() != nil && !validValueReference(fragment.GetSubject(), w.engine) {
			return sourceError("invalid fragment subject")
		}
		if err := w.fragment(document.GetFormat(), fragment); err != nil {
			return err
		}
	}
	return nil
}

func (w *sourceWalk) fragment(format protocol.SourceDocument_Format, fragment *protocol.SourceFragment) error {
	if err := w.node(0); err != nil {
		return err
	}
	if err := foreignGrammarClaim(format, fragment); err != nil {
		return err
	}
	if err := w.span(fragment.GetSpan(), authoredSource); err != nil {
		return err
	}
	if inherited := fragment.GetInheritedAttributes(); inherited != nil {
		if err := w.mapping(inherited, 0, inheritedSource); err != nil {
			return err
		}
	}
	switch content := fragment.GetContent().(type) {
	case *protocol.SourceFragment_Value:
		if content == nil || !valueContentFormat(format, fragment) {
			return sourceError("value content does not match source format")
		}
		return w.value(content.Value, 0, authoredSource)
	case *protocol.SourceFragment_Block:
		if content == nil || format != protocol.SourceDocument_HCL {
			return sourceError("block content does not match source format")
		}
		return w.block(content.Block, 0)
	case *protocol.SourceFragment_Sql:
		if content == nil || format != protocol.SourceDocument_SQL || strings.TrimSpace(content.Sql) == "" {
			return sourceError("SQL content does not match source format")
		}
		if uint64(len(content.Sql)) != fragment.GetSpan().GetEnd().GetByteOffset()-fragment.GetSpan().GetStart().GetByteOffset() {
			return sourceError("SQL source length disagrees with its span")
		}
		return nil
	default:
		return sourceError("fragment content is missing")
	}
}

// valueContentFormat reports whether a fragment of format may carry value
// content: a YAML or Go fragment, and an HCL attribute claim, which carries the
// owner's attributes of the frontend's block as a mapping.
func valueContentFormat(format protocol.SourceDocument_Format, fragment *protocol.SourceFragment) bool {
	switch format {
	case protocol.SourceDocument_YAML, protocol.SourceDocument_GO_ANNOTATIONS:
		return true
	case protocol.SourceDocument_HCL:
		return fragment.GetHclBlock() != nil && !hclOwnBlock(fragment.GetHclBlock().GetSite())
	default:
		return false
	}
}

// foreignGrammarClaim refuses a grammar claim on a source format it does not name.
func foreignGrammarClaim(format protocol.SourceDocument_Format, fragment *protocol.SourceFragment) error {
	if format != protocol.SourceDocument_YAML && fragment.GetYamlKey() != nil {
		return sourceError("YAML grammar claim on another source format")
	}
	if format != protocol.SourceDocument_GO_ANNOTATIONS && fragment.GetGoDirective() != nil {
		return sourceError("Go directive claim on another source format")
	}
	if format != protocol.SourceDocument_HCL && fragment.GetHclBlock() != nil {
		return sourceError("HCL block claim on another source format")
	}
	return nil
}

func (w *sourceWalk) span(span *protocol.SourceSpan, origin sourceOrigin) error {
	if w.encoded {
		if span != nil {
			return sourceError("an encoded fragment carries a position")
		}
		return nil
	}
	if span == nil && origin == inheritedSource {
		return nil
	}
	start, end := span.GetStart(), span.GetEnd()
	if !validSourcePosition(start) || !validSourcePosition(end) {
		return sourceError("source position is missing")
	}
	if start.GetByteOffset() > end.GetByteOffset() || end.GetByteOffset() > w.length || start.GetLine() > end.GetLine() {
		return sourceError("source span is outside the captured document")
	}
	if start.GetLine() == end.GetLine() && (start.GetColumn() > end.GetColumn() ||
		end.GetByteOffset()-start.GetByteOffset() != uint64(end.GetColumn()-start.GetColumn())) {
		return sourceError("source byte columns disagree with offsets")
	}
	if end.GetLine() > start.GetLine() && end.GetByteOffset()-start.GetByteOffset() < uint64(end.GetLine()-start.GetLine())+uint64(end.GetColumn()-1) {
		return sourceError("source lines disagree with offsets")
	}
	return nil
}

func validSourcePosition(position *protocol.SourcePosition) bool {
	if position == nil || position.GetLine() == 0 || position.GetColumn() == 0 {
		return false
	}
	if position.GetLine() == 1 {
		return position.GetByteOffset() == uint64(position.GetColumn()-1)
	}
	return position.GetByteOffset() >= uint64(position.GetLine()-1)+uint64(position.GetColumn()-1)
}

func (w *sourceWalk) node(depth int) error {
	w.nodes++
	if depth > MaxSourceDepth || w.nodes > MaxSourceNodes {
		return sourceError("source structure exceeds batch limits")
	}
	return nil
}

func (w *sourceWalk) mapping(mapping *protocol.SourceMapping, depth int, origin sourceOrigin) error {
	if mapping == nil {
		return sourceError("mapping is missing")
	}
	keys := make(map[string]bool)
	for _, entry := range mapping.GetEntries() {
		if entry == nil || keys[entry.GetKey()] {
			return sourceError("missing mapping entry or duplicate key")
		}
		keys[entry.GetKey()] = true
		if err := w.span(entry.GetKeySpan(), origin); err != nil {
			return err
		}
		if err := w.value(entry.GetValue(), depth+1, origin); err != nil {
			return err
		}
	}
	return nil
}

func (w *sourceWalk) value(value *protocol.SourceValue, depth int, origin sourceOrigin) error {
	if value == nil || w.values[value] {
		return sourceError("missing or cyclic source value")
	}
	if err := w.node(depth); err != nil {
		return err
	}
	if err := w.span(value.GetSpan(), origin); err != nil {
		return err
	}
	w.values[value] = true
	defer delete(w.values, value)
	switch content := value.GetValue().(type) {
	case *protocol.SourceValue_Mapping:
		if content != nil {
			return w.mapping(content.Mapping, depth, origin)
		}
	case *protocol.SourceValue_Sequence:
		if content != nil {
			return w.sequence(content.Sequence, depth, origin)
		}
	default:
		if validSourceScalar(value, w.engine) {
			return nil
		}
	}
	return sourceError("invalid or missing source value variant")
}

func validSourceScalar(value *protocol.SourceValue, engine string) bool {
	switch content := value.GetValue().(type) {
	case *protocol.SourceValue_Null:
		return content != nil && content.Null != nil
	case *protocol.SourceValue_Boolean:
		return content != nil
	case *protocol.SourceValue_Text:
		return content != nil
	case *protocol.SourceValue_Number:
		return content != nil && exactNumber(content.Number)
	case *protocol.SourceValue_Sql:
		return content != nil && strings.TrimSpace(content.Sql) != ""
	case *protocol.SourceValue_Reference:
		return content != nil && validValueReference(content.Reference, engine)
	default:
		return false
	}
}

func (w *sourceWalk) sequence(sequence *protocol.SourceSequence, depth int, origin sourceOrigin) error {
	if sequence == nil {
		return sourceError("sequence is missing")
	}
	for _, child := range sequence.GetValues() {
		if err := w.value(child, depth+1, origin); err != nil {
			return err
		}
	}
	return nil
}

func exactNumber(value string) bool {
	return len(value) != 0 && strings.TrimSpace(value) == value &&
		(value[0] == '-' || (value[0] >= '0' && value[0] <= '9')) && json.Valid([]byte(value))
}

func (w *sourceWalk) block(block *protocol.SourceBlock, depth int) error {
	if block == nil || block.GetType() == "" || w.blocks[block] {
		return sourceError("missing or cyclic evaluated block")
	}
	if err := w.node(depth); err != nil {
		return err
	}
	if err := w.span(block.GetSpan(), authoredSource); err != nil {
		return err
	}
	for _, label := range block.GetLabels() {
		if err := w.node(depth + 1); err != nil {
			return err
		}
		if err := w.span(label.GetSpan(), authoredSource); err != nil {
			return err
		}
	}
	w.blocks[block] = true
	defer delete(w.blocks, block)
	if block.GetAttributes() != nil {
		if err := w.mapping(block.GetAttributes(), depth, authoredSource); err != nil {
			return err
		}
	}
	for _, child := range block.GetChildren() {
		if err := w.block(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}
