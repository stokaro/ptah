package provider

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ResolveSourceTrace resolves typed source paths against a validated captured
// document. The returned span is a copy; an inherited default may have no span.
// Missing paths are errors, distinct from defaults without authored positions.
// This function never opens the file named by the document.
func ResolveSourceTrace(engine string, document *protocol.SourceDocument, trace *protocol.SourceTrace) (*protocol.SourceSpan, error) {
	if err := ValidateSourceDocuments(engine, []*protocol.SourceDocument{document}); err != nil {
		return nil, err
	}
	if trace == nil || document.GetId() != trace.GetDocumentId() {
		return nil, sourceError("unknown trace document")
	}
	for _, fragment := range document.GetFragments() {
		if fragment.GetId() == trace.GetFragmentId() {
			return resolveFragmentTrace(fragment, trace)
		}
	}
	return nil, sourceError("unknown trace fragment")
}

type traceCursor struct {
	value   *protocol.SourceValue
	block   *protocol.SourceBlock
	mapping *protocol.SourceMapping
	span    *protocol.SourceSpan
}

func resolveFragmentTrace(fragment *protocol.SourceFragment, trace *protocol.SourceTrace) (*protocol.SourceSpan, error) {
	if len(trace.GetPath()) > MaxSourceDepth {
		return nil, sourceError("trace exceeds depth limit")
	}
	cursor := traceCursor{}
	switch trace.GetRoot() {
	case protocol.SourceTrace_CONTENT:
		cursor.value, cursor.block, cursor.span = fragment.GetValue(), fragment.GetBlock(), fragment.GetSpan()
	case protocol.SourceTrace_INHERITED_ATTRIBUTES:
		cursor.mapping = fragment.GetInheritedAttributes()
		if cursor.mapping == nil {
			return nil, sourceError("trace has no inherited attributes")
		}
	default:
		return nil, sourceError("unknown trace root")
	}
	for _, step := range trace.GetPath() {
		next, err := cursor.step(step)
		if err != nil {
			return nil, err
		}
		cursor = next
	}
	return proto.CloneOf(cursor.span), nil
}

func (c traceCursor) step(step *protocol.SourcePathStep) (traceCursor, error) {
	switch part := step.GetStep().(type) {
	case *protocol.SourcePathStep_Key:
		if part != nil {
			mapping := c.mapping
			if c.value != nil {
				mapping = c.value.GetMapping()
			}
			return traceMappingKey(mapping, part.Key)
		}
	case *protocol.SourcePathStep_Index:
		if part != nil && c.value.GetSequence() != nil && uint64(part.Index) < uint64(len(c.value.GetSequence().GetValues())) {
			return traceValue(c.value.GetSequence().GetValues()[part.Index]), nil
		}
	case *protocol.SourcePathStep_Attribute:
		if part != nil && c.block != nil {
			return traceMappingKey(c.block.GetAttributes(), part.Attribute)
		}
	case *protocol.SourcePathStep_Child:
		if part != nil && c.block != nil && uint64(part.Child) < uint64(len(c.block.GetChildren())) {
			block := c.block.GetChildren()[part.Child]
			return traceCursor{block: block, span: block.GetSpan()}, nil
		}
	case *protocol.SourcePathStep_Label:
		if part != nil && c.block != nil && uint64(part.Label) < uint64(len(c.block.GetLabels())) {
			return traceCursor{span: c.block.GetLabels()[part.Label].GetSpan()}, nil
		}
	}
	return traceCursor{}, sourceError("trace step does not resolve")
}

func traceMappingKey(mapping *protocol.SourceMapping, key string) (traceCursor, error) {
	for _, entry := range mapping.GetEntries() {
		if entry.GetKey() == key {
			return traceValue(entry.GetValue()), nil
		}
	}
	return traceCursor{}, sourceError("trace key does not resolve")
}

func traceValue(value *protocol.SourceValue) traceCursor {
	return traceCursor{value: value, span: value.GetSpan()}
}
