// Package main runs the fixture with the HCL claim of contract 25.22: a
// table's retention in seconds as the `seconds` override of the table's
// `platform "fixture"` block. The retention model lists no HCL, so an HCL
// document describes a retention only for a table that writes the block. A
// document's ptah:not-described retention entries are the engine's header
// kind, so a document that writes one is sent to the provider for its account.
//
// It writes a table's retention back in HCL as the same block. With
// FIXTURE_PLATFORM_PARTIAL=1 it also reports a loss of each retention it
// writes, as a provider whose group spells part of a facet does. With
// FIXTURE_PLATFORM_UNWRITTEN=1 it writes no group and reports each table's
// retention as a loss, as a provider whose group cannot spell a table's
// setting does.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

const retentionKind = "example.org/fixture/retention"

func main() {
	owner := &platformClaims{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SourceEncoder: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// claim is the program's claim of the fixture engine's table platform group.
func claim() *protocol.HCLSourceBlock {
	return &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TABLE_PLATFORM, Type: "fixture", Selector: "retention", Models: []string{retentionKind},
		Attributes: []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_STRING}}}
}

// description is the base fixture with the platform claim, a retention model
// that lists no HCL, the retention header kind, and source encoding.
func description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-hcl-platform"
	description.Features = append(description.Features, provider.HCLSourceFeature, provider.HCLPlatformFeature, provider.EncodeSourceFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.HCLSourceFeature, provider.HCLPlatformFeature, provider.EncodeSourceFeature)
	engine.ValueModels[0].SourceFormats = slices.DeleteFunc(engine.ValueModels[0].SourceFormats, func(format protocol.SourceDocument_Format) bool {
		return format == protocol.SourceDocument_HCL
	})
	engine.HclSource = &protocol.HCLSourceDeclaration{HeaderKinds: []string{"retention"}, Blocks: []*protocol.HCLSourceBlock{claim()}}
	return description
}

type platformClaims struct{ *backend.Provider }

// DecodeSource reads each platform group of an HCL document as the retention
// of the table the group stands in, and leaves every other format to the base
// fixture.
func (p *platformClaims) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	if request.GetDocuments()[0].GetFormat() != protocol.SourceDocument_HCL {
		return p.Provider.DecodeSource(ctx, request)
	}
	result := &protocol.DecodedSource{Complete: true}
	for _, document := range request.GetDocuments() {
		decoded := &protocol.DecodedSourceDocument{Id: document.GetId(), Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}
		for _, model := range document.GetModels() {
			decoded.Coverage.Kinds = append(decoded.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(model),
				Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		for _, fragment := range document.GetFragments() {
			value, err := p.retention(ctx, document, fragment)
			if err != nil {
				return nil, err
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

// retention decodes one group's seconds as the retention of its table.
func (p *platformClaims) retention(ctx context.Context, document *protocol.SourceDocument, fragment *protocol.SourceFragment) (*protocol.SourceModelValue, error) {
	var owner *protocol.Reference
	for _, binding := range document.GetBindings() {
		if binding.GetId() == fragment.GetBindingId() {
			owner = proto.CloneOf(binding.GetObject())
		}
	}
	fields := make(map[string]uint64)
	for _, entry := range fragment.GetValue().GetMapping().GetEntries() {
		number, err := strconv.ParseUint(entry.GetValue().GetText(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("retention %s requires an exact number", entry.GetKey())
		}
		fields[entry.GetKey()] = number
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

// EncodeSource writes a table's retention in HCL as the table's platform
// group. Any other retention has no HCL spelling and is a loss.
func (p *platformClaims) EncodeSource(_ context.Context, request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	if request.GetFormat() != protocol.SourceDocument_HCL {
		return nil, fmt.Errorf("the fixture writes HCL only")
	}
	result := &protocol.EncodedSource{Complete: true}
	for _, value := range request.GetValues() {
		owner := value.GetValue().GetValue().GetOwner()
		if owner.GetKind() != "table" {
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(), Message: "the retention of " + owner.GetKind() + " " +
				strconv.Quote(owner.GetName()) + " has no HCL spelling"})
			continue
		}
		if os.Getenv("FIXTURE_PLATFORM_UNWRITTEN") == "1" {
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(),
				Message: "the retention of table " + strconv.Quote(owner.GetName()) + " has no HCL spelling"})
			continue
		}
		var seconds struct {
			Seconds uint64 `json:"seconds"`
		}
		if err := json.Unmarshal(value.GetValue().GetCanonicalProjection(), &seconds); err != nil {
			return nil, err
		}
		group := claim()
		result.Fragments = append(result.Fragments, &protocol.EncodedFragment{ValueIds: []string{value.GetValue().GetId()},
			Fragment: &protocol.SourceFragment{Id: strconv.Itoa(len(result.Fragments)), HclBlock: group, Selector: group.GetSelector(), BindingId: value.GetBindingId(),
				Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{
					Entries: []*protocol.SourceEntry{{Key: "seconds", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: strconv.FormatUint(seconds.Seconds, 10)}}}}}}}}}})
		if os.Getenv("FIXTURE_PLATFORM_PARTIAL") == "1" {
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(),
				Message: "the retention tiers of table " + strconv.Quote(owner.GetName()) + " are not represented in HCL"})
		}
	}
	return result, nil
}
