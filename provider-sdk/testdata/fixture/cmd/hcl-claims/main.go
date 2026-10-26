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
//
// It writes the retention of a table back in HCL as the block in the table,
// and of an index as the index's attribute; HCL has no spelling it writes for
// a column's retention, which it reports as a loss. In Go annotations it
// writes each as the directive or attribute it reads, and an INTEGER column
// as an int64 field.
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
	owner := &hclClaims{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SourceEncoder: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// description is the base fixture with the program's HCL claims, the Go
// claims beyond table scope, and source encoding.
func description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-hcl-claims"
	description.Features = append(description.Features, provider.HCLSourceFeature, provider.EncodeSourceFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.HCLSourceFeature, provider.EncodeSourceFeature)
	if sourceLimitsEnabled() {
		description.Features = append(description.Features, provider.SourceLimitsFeature)
		engine.Features = append(engine.Features, provider.SourceLimitsFeature)
		engine.GoHeaderKinds = []string{"retention"}
	}
	if os.Getenv("FIXTURE_SOURCE_GROUPS") == "1" {
		description.Features = append(description.Features, provider.EncodeGroupsFeature)
		engine.Features = append(engine.Features, provider.EncodeGroupsFeature)
	}
	engine.GoSourceDirectives = append(engine.GoSourceDirectives, backend.ScopedGoSourceDirectives()...)
	seconds := []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}}
	models := []string{retentionKind}
	engine.HclSource = &protocol.HCLSourceDeclaration{HeaderKinds: []string{"retention"}, Blocks: []*protocol.HCLSourceBlock{
		{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "retention", Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME, Attributes: seconds,
			Selector: "retention", Models: models},
		{Site: protocol.HCLSourceBlock_IN_TABLE, Type: "retention", Attributes: seconds, Selector: "retention", Models: models},
		{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES, Type: "index", Attributes: []*protocol.HCLSourceAttribute{{Name: "retention_seconds", Kind: protocol.HCLSourceAttribute_TEXT}},
			Selector: "retention", Models: models, Inherited: []string{"type"}},
	}}
	return description
}

type hclClaims struct{ *backend.Provider }

// DecodeSource reads the program's HCL fragments and leaves every other
// format to the base fixture. Each fragment declares the retention of what it
// is about: the table a top-level block names, the table a block stands in,
// or the index whose attribute it is.
func (p *hclClaims) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	if request.GetDocuments()[0].GetFormat() != protocol.SourceDocument_HCL {
		result, err := p.Provider.DecodeSource(ctx, request)
		if err != nil {
			return nil, err
		}
		return decodeLimits(request, result)
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
	return decodeLimits(request, result)
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

// EncodeSource writes each retention in HCL as the program reads it back: a
// table's as a retention block in the table, an index's as its
// retention_seconds attribute. A column's has no HCL spelling and is a loss.
func (p *hclClaims) EncodeSource(_ context.Context, request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	if request.GetFormat() == protocol.SourceDocument_GO_ANNOTATIONS {
		return encodeGo(request)
	}
	if request.GetFormat() != protocol.SourceDocument_HCL {
		return nil, fmt.Errorf("the fixture writes HCL and Go annotations only")
	}
	claims := make(map[protocol.HCLSourceBlock_Site]*protocol.HCLSourceBlock)
	for _, block := range description().GetEngines()[0].GetHclSource().GetBlocks() {
		claims[block.GetSite()] = block
	}
	result := &protocol.EncodedSource{Complete: true}
	for _, value := range request.GetValues() {
		var seconds struct {
			Seconds uint64 `json:"seconds"`
		}
		if err := json.Unmarshal(value.GetValue().GetCanonicalProjection(), &seconds); err != nil {
			return nil, err
		}
		text := func(key string) *protocol.SourceMapping {
			return &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: key,
				Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: strconv.FormatUint(seconds.Seconds, 10)}}}}}
		}
		owner := value.GetValue().GetValue().GetOwner()
		switch owner.GetKind() {
		case "table":
			claim := claims[protocol.HCLSourceBlock_IN_TABLE]
			result.Fragments = append(result.Fragments, &protocol.EncodedFragment{ValueIds: []string{value.GetValue().GetId()}, Placement: protocol.EncodedFragment_WITHIN_TABLE,
				Fragment: &protocol.SourceFragment{Id: strconv.Itoa(len(result.Fragments)), HclBlock: claim, Selector: claim.GetSelector(), BindingId: value.GetBindingId(),
					Content: &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: claim.GetType(), Attributes: text("seconds")}}}})
		case "index":
			claim := claims[protocol.HCLSourceBlock_INDEX_ATTRIBUTES]
			result.Fragments = append(result.Fragments, &protocol.EncodedFragment{ValueIds: []string{value.GetValue().GetId()},
				Fragment: &protocol.SourceFragment{Id: strconv.Itoa(len(result.Fragments)), HclBlock: claim, Selector: claim.GetSelector(), BindingId: value.GetBindingId(),
					Subject: proto.CloneOf(owner),
					Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: text("retention_seconds")}}}}})
		default:
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(), Message: "the retention of " + owner.GetKind() + " " +
				strconv.Quote(owner.GetName()) + " has no HCL spelling"})
		}
	}
	return encodeLimits(request, result)
}

// encodeGo writes each retention as a Go annotation the program reads back: a
// table's with its own ptah:fixture:retention directive, a column's and an
// index's as the seconds attribute of the frontend's field and index
// directives. An INTEGER column is an int64 field.
func encodeGo(request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	claims := make(map[string]*protocol.GoSourceDirective)
	for _, claim := range description().GetEngines()[0].GetGoSourceDirectives() {
		claims[claim.GetDirective()] = claim
	}
	directives := map[string]string{"table": "ptah:fixture:retention", "column": "ptah:schema:field", "index": "ptah:schema:index"}
	result := &protocol.EncodedSource{Complete: true}
	for _, value := range request.GetValues() {
		var seconds struct {
			Seconds uint64 `json:"seconds"`
		}
		if err := json.Unmarshal(value.GetValue().GetCanonicalProjection(), &seconds); err != nil {
			return nil, err
		}
		owner := value.GetValue().GetValue().GetOwner()
		claim := claims[directives[owner.GetKind()]]
		if claim == nil {
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(), Message: "the retention of " + owner.GetKind() + " " +
				strconv.Quote(owner.GetName()) + " has no Go spelling"})
			continue
		}
		fragment := &protocol.SourceFragment{Id: strconv.Itoa(len(result.Fragments)), GoDirective: claim, Selector: claim.GetSelector(), BindingId: value.GetBindingId(),
			Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{
				{Key: "seconds", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: strconv.FormatUint(seconds.Seconds, 10)}}}}}}}}}
		if owner.GetKind() != "table" {
			fragment.Subject = proto.CloneOf(owner)
		}
		result.Fragments = append(result.Fragments, &protocol.EncodedFragment{Fragment: fragment, ValueIds: []string{value.GetValue().GetId()}})
	}
	for _, column := range request.GetColumns() {
		if strings.EqualFold(column.GetType(), "INTEGER") {
			result.FieldTypes = append(result.FieldTypes, &protocol.EncodedFieldType{BindingId: column.GetBindingId(), Column: column.GetName(), GoType: "int64"})
		}
	}
	return encodeLimits(request, result)
}
