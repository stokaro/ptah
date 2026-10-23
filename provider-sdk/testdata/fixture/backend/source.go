package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

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
	column := ""
	for _, entry := range mapping.GetEntries() {
		// A platform property spells its unit with the engine's prefix.
		switch strings.TrimPrefix(entry.GetKey(), "platform.fixture.") {
		case "column":
			text, ok := entry.GetValue().GetValue().(*protocol.SourceValue_Text)
			if !ok || text.Text == "" {
				return retentionSource{}, fmt.Errorf("retention column requires a name")
			}
			column = text.Text
		case "on":
			text, ok := entry.GetValue().GetValue().(*protocol.SourceValue_Text)
			if !ok || text.Text == "" || bindingID != "" || subject != nil {
				return retentionSource{}, fmt.Errorf("retention requires one unambiguous subject")
			}
			bindingID = text.Text
		case "table":
			// The host resolves the table attribute of a Go directive into
			// the binding, so the value only repeats it.
			if document.GetFormat() != protocol.SourceDocument_GO_ANNOTATIONS || bindingID == "" {
				return retentionSource{}, fmt.Errorf("retention does not accept a table attribute here")
			}
		case "seconds", "minutes":
			number, err := retentionNumber(document.GetFormat(), entry.GetValue())
			if err != nil {
				return retentionSource{}, err
			}
			fields[strings.TrimPrefix(entry.GetKey(), "platform.fixture.")] = json.RawMessage(number)
			knowledge = protocol.ModelKnowledge_COMPLETE
		default:
			return retentionSource{}, fmt.Errorf("unsupported retention field %q", entry.GetKey())
		}
	}
	// A fragment that names its subject, as a Go column or index directive
	// does, also carries the binding of the table it belongs to. The binding is
	// context then, and resolves nothing.
	if bindingID != "" && subject == nil {
		for _, binding := range document.GetBindings() {
			if binding.GetId() == bindingID {
				subject = proto.CloneOf(binding.GetObject())
			}
		}
	}
	// A column setting written on the directive of its table: the value is
	// owned by the column of that table the directive names.
	if column != "" && subject != nil && subject.GetKind() == "table" {
		subject = &protocol.Reference{Engine: subject.GetEngine(), Kind: "column", Schema: subject.GetSchema(), Path: []string{subject.GetName()}, Name: column}
	}
	if subject == nil || !slices.Contains([]string{"table", "column", "index"}, subject.GetKind()) {
		return retentionSource{}, fmt.Errorf("retention table binding is unknown")
	}
	payload, err := json.Marshal(fields)
	return retentionSource{subject: subject, payload: payload, knowledge: knowledge}, err
}

// retentionNumber returns the exact number a retention field holds. A Go
// annotation attribute is always text, because the host lexes the comment and
// does not guess a type, so the owner reads an unsigned integer out of it. The
// other formats carry numbers and refuse text.
func retentionNumber(format protocol.SourceDocument_Format, value *protocol.SourceValue) (string, error) {
	switch content := value.GetValue().(type) {
	case *protocol.SourceValue_Number:
		return content.Number, nil
	case *protocol.SourceValue_Text:
		if number, err := strconv.ParseUint(content.Text, 10, 64); err == nil && format == protocol.SourceDocument_GO_ANNOTATIONS {
			return strconv.FormatUint(number, 10), nil
		}
	}
	return "", fmt.Errorf("retention requires an exact number")
}

var _ provider.SourceDecoder = (*Provider)(nil)
