// Package main runs the fixture with the HCL claims of contract 25.3: a
// top-level retention block that names its table, a retention block in a
// table, and a retention attribute of the frontend's index block that reads
// the index type with it. A document's ptah:not-described retention entries
// name tables whose retention it leaves undescribed, and the program refuses
// a document that also declares one of them. A decode reply records no
// subject a document leaves undescribed, so refusing is all a provider can do
// with such an entry.
//
// A retention suits an ordered index only, so the program refuses one on an
// index whose type says it is a hash index.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

const retentionKind = "example.org/fixture/retention"

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-hcl-claims"
	description.Features = append(description.Features, provider.HCLSourceFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.HCLSourceFeature)
	seconds := []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}}
	models := []string{retentionKind}
	engine.HclSource = &protocol.HCLSourceDeclaration{HeaderKinds: []string{"retention"}, Blocks: []*protocol.HCLSourceBlock{
		{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "retention", Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME, Attributes: seconds,
			Selector: "retention", Models: models},
		{Site: protocol.HCLSourceBlock_IN_TABLE, Type: "retention", Attributes: seconds, Selector: "retention", Models: models},
		{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES, Type: "index", Attributes: []*protocol.HCLSourceAttribute{{Name: "retention_seconds", Kind: protocol.HCLSourceAttribute_TEXT}},
			Selector: "retention", Models: models, Inherited: []string{"type"}},
	}}
	owner := &hclClaims{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type hclClaims struct{ *backend.Provider }

// DecodeSource reads the program's HCL fragments and leaves every other
// format to the base fixture. Each fragment declares the retention of what it
// is about: the table a top-level block names, the table a block stands in,
// or the index whose attribute it is.
func (p *hclClaims) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	if request.GetDocuments()[0].GetFormat() != protocol.SourceDocument_HCL {
		return p.Provider.DecodeSource(ctx, request)
	}
	result := &protocol.DecodedSource{Complete: true}
	for _, document := range request.GetDocuments() {
		decoded := &protocol.DecodedSourceDocument{Id: document.GetId(), Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}
		for _, model := range document.GetModels() {
			decoded.Coverage.Kinds = append(decoded.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		undescribed := make(map[string]bool)
		for _, limit := range document.GetLimits() {
			undescribed[limit.GetName()] = true
		}
		for _, fragment := range document.GetFragments() {
			value, err := p.retention(ctx, document, fragment)
			if err != nil {
				return nil, err
			}
			if owner := value.GetValue().GetValue().GetOwner(); owner.GetKind() == "table" && undescribed[owner.GetName()] {
				return nil, fmt.Errorf("the document leaves the retention of table %q undescribed and declares it", owner.GetName())
			}
			decoded.Fragments = append(decoded.Fragments, &protocol.DecodedSourceFragment{Id: fragment.GetId(), Values: []*protocol.SourceModelValue{value}})
			decoded.Coverage.Subjects = append(decoded.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: retentionKind,
				Subject: proto.CloneOf(value.GetValue().GetValue().GetOwner()), NormalizedSubject: proto.CloneOf(value.GetValue().GetNormalizedOwner()),
				Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		result.Documents = append(result.Documents, decoded)
	}
	return result, nil
}

// retention decodes one fragment's retention for what the fragment is about.
func (p *hclClaims) retention(ctx context.Context, document *protocol.SourceDocument, fragment *protocol.SourceFragment) (*protocol.SourceModelValue, error) {
	attributes, owner := fragment.GetValue().GetMapping(), fragment.GetSubject()
	switch fragment.GetHclBlock().GetSite() {
	case protocol.HCLSourceBlock_TOP_LEVEL:
		// The host sends the schema the block names, when it names one, and
		// the name as the labels.
		labels := fragment.GetBlock().GetLabels()
		owner = &protocol.Reference{Engine: "fixture", Kind: "table", Name: labels[len(labels)-1].GetValue()}
		if len(labels) == 2 {
			owner.Schema = labels[0].GetValue()
		}
		attributes = fragment.GetBlock().GetAttributes()
	case protocol.HCLSourceBlock_IN_TABLE:
		for _, binding := range document.GetBindings() {
			if binding.GetId() == fragment.GetBindingId() {
				owner = proto.CloneOf(binding.GetObject())
			}
		}
		attributes = fragment.GetBlock().GetAttributes()
	case protocol.HCLSourceBlock_INDEX_ATTRIBUTES:
		for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
			if entry.GetKey() == "type" && strings.EqualFold(entry.GetValue().GetText(), "hash") {
				return nil, fmt.Errorf("a fixture retention needs an ordered index, and index %q is a hash index", owner.GetName())
			}
		}
	}
	fields := make(map[string]uint64)
	for _, entry := range attributes.GetEntries() {
		number, err := strconv.ParseUint(entry.GetValue().GetText(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("retention %s requires an exact number", entry.GetKey())
		}
		fields["seconds"] = number
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	model := backend.Description().GetEngines()[0].GetValueModels()[0]
	input := &protocol.ValueInput{Id: fragment.GetId(), Representation: protocol.ValueInput_DESIRED, Value: &protocol.Envelope{ProviderId: "example.org/fixture",
		Kind: retentionKind, SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: owner, Payload: payload}}
	normalized, err := p.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{input}})
	if err != nil {
		return nil, err
	}
	return &protocol.SourceModelValue{Placement: protocol.SourceModelValue_FACET, Value: normalized.GetValues()[0],
		Source: &protocol.SourceTrace{DocumentId: document.GetId(), FragmentId: fragment.GetId(), Root: protocol.SourceTrace_CONTENT}}, nil
}
