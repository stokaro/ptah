package backend

import (
	"context"
	"encoding/json"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// DecodeSource interprets the fixture's retention extension from evaluated
// YAML, HCL, or Go annotation data. It resolves written binding IDs against the
// whole document, including bindings declared after a fragment. SQL is refused.
// This fixture demonstrates transport semantics, not an engine implementation.
func (p *Provider) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	result := &protocol.DecodedSource{Complete: true}
	for _, document := range request.GetDocuments() {
		decoded := &protocol.DecodedSourceDocument{Id: document.GetId(), Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}
		for _, model := range document.GetModels() {
			decoded.Coverage.Kinds = append(decoded.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		for _, fragment := range document.GetFragments() {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			value, knowledge, err := p.decodeRetention(ctx, document, fragment)
			if err != nil {
				return nil, err
			}
			decoded.Fragments = append(decoded.Fragments, &protocol.DecodedSourceFragment{Id: fragment.GetId(), Values: []*protocol.SourceModelValue{value}})
			decoded.Coverage.Subjects = append(decoded.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: value.GetValue().GetValue().GetKind(), Subject: proto.CloneOf(value.GetValue().GetValue().GetOwner()),
				NormalizedSubject: proto.CloneOf(value.GetValue().GetNormalizedOwner()), Knowledge: &protocol.ModelKnowledge{State: knowledge}})
		}
		result.Documents = append(result.Documents, decoded)
	}
	return result, nil
}

func (p *Provider) decodeRetention(ctx context.Context, document *protocol.SourceDocument, fragment *protocol.SourceFragment) (*protocol.SourceModelValue, protocol.ModelKnowledge_State, error) {
	mapping, err := retentionMapping(fragment)
	if err != nil {
		return nil, 0, err
	}
	fields, err := retentionSourceFields(document, fragment, mapping)
	if err != nil {
		return nil, 0, err
	}
	model := valueModels()[0]
	input := &protocol.ValueInput{Id: fragment.GetId(), Representation: protocol.ValueInput_DESIRED, Value: &protocol.Envelope{
		ProviderId: Description().GetProviderId(), Kind: model.GetKind(), SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: fields.subject, Payload: fields.payload,
	}}
	normalized, err := p.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{input}})
	if err != nil {
		return nil, 0, err
	}
	return &protocol.SourceModelValue{Placement: protocol.SourceModelValue_FACET, Value: normalized.Values[0],
		Source: &protocol.SourceTrace{DocumentId: document.GetId(), FragmentId: fragment.GetId(), Root: protocol.SourceTrace_CONTENT}}, fields.knowledge, nil
}

func retentionMapping(fragment *protocol.SourceFragment) (*protocol.SourceMapping, error) {
	if fragment.GetSelector() != "retention" || len(fragment.GetInheritedAttributes().GetEntries()) != 0 {
		return nil, fmt.Errorf("unsupported retention fragment or inherited fields")
	}
	switch content := fragment.GetContent().(type) {
	case *protocol.SourceFragment_Value:
		if mapping := content.Value.GetMapping(); mapping != nil {
			return mapping, nil
		}
	case *protocol.SourceFragment_Block:
		block := content.Block
		if block.GetType() == "retention" && len(block.GetLabels()) == 0 && len(block.GetChildren()) == 0 {
			return block.GetAttributes(), nil
		}
	}
	return nil, fmt.Errorf("retention requires a mapping or an unlabeled retention block")
}

type retentionSource struct {
	subject   *protocol.Reference
	payload   []byte
	knowledge protocol.ModelKnowledge_State
}

func retentionSourceFields(document *protocol.SourceDocument, fragment *protocol.SourceFragment, mapping *protocol.SourceMapping) (retentionSource, error) {
	subject := proto.CloneOf(fragment.GetSubject())
	bindingID := fragment.GetBindingId()
	fields := make(map[string]json.RawMessage)
	knowledge := protocol.ModelKnowledge_DEFAULTED
	for _, entry := range mapping.GetEntries() {
		switch entry.GetKey() {
		case "on":
			text, ok := entry.GetValue().GetValue().(*protocol.SourceValue_Text)
			if !ok || text.Text == "" || bindingID != "" || subject != nil {
				return retentionSource{}, fmt.Errorf("retention requires one unambiguous subject")
			}
			bindingID = text.Text
		case "seconds", "minutes":
			number, ok := entry.GetValue().GetValue().(*protocol.SourceValue_Number)
			if !ok {
				return retentionSource{}, fmt.Errorf("retention requires an exact number")
			}
			fields[entry.GetKey()] = json.RawMessage(number.Number)
			knowledge = protocol.ModelKnowledge_COMPLETE
		default:
			return retentionSource{}, fmt.Errorf("unsupported retention field %q", entry.GetKey())
		}
	}
	if bindingID != "" {
		if subject != nil {
			return retentionSource{}, fmt.Errorf("conflicting retention subjects")
		}
		for _, binding := range document.GetBindings() {
			if binding.GetId() == bindingID {
				subject = proto.CloneOf(binding.GetObject())
			}
		}
	}
	if subject == nil || subject.GetKind() != "table" {
		return retentionSource{}, fmt.Errorf("retention table binding is unknown")
	}
	payload, err := json.Marshal(fields)
	return retentionSource{subject: subject, payload: payload, knowledge: knowledge}, err
}

var _ provider.SourceDecoder = (*Provider)(nil)
