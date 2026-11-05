// Package main runs the fixture with the index-kind claim of contract 25.24:
// the fixture engine reads a "tree" index, also spelled "global tree", in
// place of the frontend's common index. The frontend builds no index for such
// a declaration and sends it whole. The program reads every tree index of a
// table, with its fanout, the attribute it claims on a Go index directive, a
// YAML index entry and an HCL index block, into one facet of the table: the
// table's tree index set, which lists each index's name, type, columns,
// covered columns, uniqueness, comment, operator class and fanout.
//
// It writes the set back, in Go annotations and HCL, as one index declaration
// of each index. With FIXTURE_TREE_PARTIAL=1 it also reports a loss of each set
// it writes, as a provider whose index parts spell part of a facet does. A
// fanout written on an index of another type is refused: only a tree index
// fans out.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

// treeKind is the model of a table's tree index set.
const treeKind = "example.org/fixture/tree-indexes"

const treeDefinition = `{"encoding":"json","kind":"example.org/fixture/tree-indexes","schema_version":1,` +
	`"value":"the tree indexes of one table, by name, each with its type, columns, include, unique, comment, ops and fanout"}`

func main() {
	owner := &treeIndexes{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SourceEncoder: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// treeModel is the desired model of a table's tree index set, which every
// source format the fixture reads takes.
func treeModel() *protocol.ValueModel {
	definition, err := modeljson.Canonical([]byte(treeDefinition))
	if err != nil {
		panic(err) // A static definition; invalid JSON is a fixture programming error.
	}
	digest := sha256.Sum256(definition)
	return &protocol.ValueModel{DisplayName: "tree index sets", Kind: treeKind, Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json",
		DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]), CanonicalFormat: treeKind + "/json-v1",
		SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS}}
}

// indexKind is the program's claim of the tree index type.
func indexKind() *protocol.SourceIndexKind {
	return &protocol.SourceIndexKind{Types: []string{"tree", "global tree"}, Selector: "tree", Models: []string{treeKind}}
}

// description is the base fixture with the tree index set model, the claim of
// the tree type, the fanout attribute of each format's index declaration,
// and source encoding in groups.
func description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-index-kinds"
	features := []string{provider.IndexKindsFeature, provider.EntryKeysFeature, provider.HCLSourceFeature, provider.EncodeSourceFeature, provider.EncodeGroupsFeature}
	description.Features = append(description.Features, features...)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, features...)
	// The retention model lists no HCL here: the program reads an HCL
	// document's tree indexes alone.
	engine.ValueModels[0].SourceFormats = slices.DeleteFunc(engine.ValueModels[0].SourceFormats, func(format protocol.SourceDocument_Format) bool {
		return format == protocol.SourceDocument_HCL
	})
	engine.ValueModels = append(engine.ValueModels, treeModel())
	models := []string{treeKind}
	engine.IndexKinds = []*protocol.SourceIndexKind{indexKind()}
	engine.GoSourceDirectives = append(engine.GoSourceDirectives, &protocol.GoSourceDirective{Scope: protocol.GoSourceDirective_INDEX,
		Directive: "ptah:schema:index", Selector: "fanout", Attributes: []string{"fanout"}, Models: models})
	engine.YamlSourceKeys = append(engine.YamlSourceKeys, &protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_INDEX, Key: "fanout", Selector: "fanout",
		Models: models})
	engine.HclSource = &protocol.HCLSourceDeclaration{Blocks: []*protocol.HCLSourceBlock{
		{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES, Type: "index", Selector: "fanout", Models: models,
			Attributes: []*protocol.HCLSourceAttribute{{Name: "fanout", Kind: protocol.HCLSourceAttribute_TEXT}}},
	}}
	return description
}

type treeIndexes struct{ *backend.Provider }

// treeIndex is one index of a table's tree index set.
type treeIndex struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Columns []string `json:"columns"`
	Include []string `json:"include,omitempty"`
	Unique  bool     `json:"unique,omitempty"`
	Comment string   `json:"comment,omitempty"`
	Ops     string   `json:"ops,omitempty"`
	Fanout  uint64   `json:"fanout,omitempty"`
}

// NormalizeValues writes a tree index set with its indexes by name, and leaves
// every other value to the base fixture.
func (p *treeIndexes) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.GetValues() {
		value := input.GetValue()
		if value.GetKind() != treeKind {
			single := proto.CloneOf(request)
			single.Values = []*protocol.ValueInput{input}
			normalized, err := p.Provider.NormalizeValues(ctx, single)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, normalized.GetValues()...)
			continue
		}
		var indexes []treeIndex
		if err := json.Unmarshal(value.GetPayload(), &indexes); err != nil {
			return nil, fmt.Errorf("a tree index set is a list of indexes: %w", err)
		}
		slices.SortFunc(indexes, func(a, b treeIndex) int { return cmp.Compare(a.Name, b.Name) })
		payload, err := json.Marshal(indexes)
		if err != nil {
			return nil, err
		}
		normalized := proto.CloneOf(value)
		normalized.Payload = payload
		result.Values = append(result.Values, &protocol.NormalizedValue{Id: input.GetId(), Value: normalized, NormalizedOwner: proto.CloneOf(value.GetOwner()),
			CanonicalFormat: treeKind + "/json-v1", CanonicalProjection: bytes.Clone(payload)})
	}
	return result, nil
}

// DecodeSource reads the tree indexes of each table into the table's tree
// index set, returned with the first of its indexes, and leaves every other
// fragment to the base fixture. A fanout an index of another type writes is
// refused; an HCL index block of another type that writes none declares
// nothing.
func (p *treeIndexes) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	forwarded := proto.CloneOf(request)
	for _, document := range forwarded.GetDocuments() {
		for _, fragment := range document.GetFragments() {
			if fragment.GetSelector() == "fanout" && len(fragment.GetValue().GetMapping().GetEntries()) != 0 {
				return nil, fmt.Errorf("index %q writes a fanout, which only a tree index takes", fragment.GetSubject().GetName())
			}
		}
		document.Fragments = slices.DeleteFunc(document.Fragments, ownFragment)
	}
	decoded, err := p.Provider.DecodeSource(ctx, forwarded)
	if err != nil {
		return nil, err
	}
	for i, document := range request.GetDocuments() {
		answered, err := p.answer(ctx, document, decoded.GetDocuments()[i])
		if err != nil {
			return nil, err
		}
		decoded.GetDocuments()[i] = answered
	}
	return decoded, nil
}

// answer joins the base fixture's answer of a document's other fragments with
// a tree index set for each table that writes a tree index.
func (p *treeIndexes) answer(ctx context.Context, document *protocol.SourceDocument, base *protocol.DecodedSourceDocument) (*protocol.DecodedSourceDocument, error) {
	sets := make(map[string][]treeIndex)
	first := make(map[string]string)
	for _, fragment := range document.GetFragments() {
		if fragment.GetIndexKind() == nil {
			continue
		}
		index, err := readTreeIndex(fragment)
		if err != nil {
			return nil, err
		}
		if _, seen := first[fragment.GetBindingId()]; !seen {
			first[fragment.GetBindingId()] = fragment.GetId()
		}
		sets[fragment.GetBindingId()] = append(sets[fragment.GetBindingId()], index)
	}
	answered := &protocol.DecodedSourceDocument{Id: base.GetId(), Coverage: base.GetCoverage()}
	results := base.GetFragments()
	for _, fragment := range document.GetFragments() {
		if !ownFragment(fragment) {
			answered.Fragments, results = append(answered.Fragments, results[0]), results[1:]
			continue
		}
		decoded := &protocol.DecodedSourceFragment{Id: fragment.GetId()}
		if first[fragment.GetBindingId()] == fragment.GetId() {
			value, err := p.treeSet(ctx, document, fragment, sets[fragment.GetBindingId()])
			if err != nil {
				return nil, err
			}
			decoded.Values = []*protocol.SourceModelValue{value}
			answered.Coverage.Subjects = append(answered.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: treeKind,
				Subject: proto.CloneOf(value.GetValue().GetValue().GetOwner()), NormalizedSubject: proto.CloneOf(value.GetValue().GetNormalizedOwner()),
				Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		answered.Fragments = append(answered.Fragments, decoded)
	}
	return answered, nil
}

// ownFragment reports whether the program reads a fragment rather than the
// base fixture: a tree index, or an index's fanout attribute.
func ownFragment(fragment *protocol.SourceFragment) bool {
	return fragment.GetIndexKind() != nil || fragment.GetSelector() == "fanout"
}

// readTreeIndex reads one tree index from its fragment: the frontend's part
// from the inherited attributes, and the fanout from the content, as text in
// Go and HCL and a number in YAML.
func readTreeIndex(fragment *protocol.SourceFragment) (treeIndex, error) {
	index := treeIndex{Name: fragment.GetSubject().GetName()}
	names := func(value *protocol.SourceValue) []string {
		var written []string
		for _, item := range value.GetSequence().GetValues() {
			written = append(written, item.GetText())
		}
		return written
	}
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		switch value := entry.GetValue(); entry.GetKey() {
		case provider.IndexKindType:
			index.Type = value.GetText()
		case provider.IndexKindColumns:
			index.Columns = names(value)
		case provider.IndexKindInclude:
			index.Include = names(value)
		case provider.IndexKindUnique:
			index.Unique = value.GetBoolean()
		case provider.IndexKindComment:
			index.Comment = value.GetText()
		case provider.IndexKindOps:
			index.Ops = value.GetText()
		}
	}
	for _, entry := range fragment.GetValue().GetMapping().GetEntries() {
		written := cmp.Or(entry.GetValue().GetText(), entry.GetValue().GetNumber())
		fanout, err := strconv.ParseUint(written, 10, 64)
		if err != nil || fanout < 2 {
			return treeIndex{}, fmt.Errorf("index %q: fanout %q is not a whole number of at least 2", index.Name, written)
		}
		index.Fanout = fanout
	}
	return index, nil
}

// treeSet is the tree index set of the fragment's table.
func (p *treeIndexes) treeSet(ctx context.Context, document *protocol.SourceDocument, fragment *protocol.SourceFragment, indexes []treeIndex) (*protocol.SourceModelValue, error) {
	var owner *protocol.Reference
	for _, binding := range document.GetBindings() {
		if binding.GetId() == fragment.GetBindingId() {
			owner = proto.CloneOf(binding.GetObject())
		}
	}
	payload, err := json.Marshal(indexes)
	if err != nil {
		return nil, err
	}
	model := treeModel()
	input := &protocol.ValueInput{Id: "tree." + fragment.GetId(), Representation: protocol.ValueInput_DESIRED, Value: &protocol.Envelope{ProviderId: "example.org/fixture",
		Kind: treeKind, SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: owner, Payload: payload}}
	normalized, err := p.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{input}})
	if err != nil {
		return nil, err
	}
	return &protocol.SourceModelValue{Placement: protocol.SourceModelValue_FACET, Value: normalized.GetValues()[0],
		Source: &protocol.SourceTrace{DocumentId: document.GetId(), FragmentId: fragment.GetId(), Root: protocol.SourceTrace_INHERITED_ATTRIBUTES}}, nil
}

// EncodeSource writes each tree index set as one index part of each index, in
// one group, in Go annotations and HCL. Any other value is a loss.
func (p *treeIndexes) EncodeSource(_ context.Context, request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	result := &protocol.EncodedSource{Complete: true}
	for _, value := range request.GetValues() {
		if value.GetValue().GetValue().GetKind() != treeKind {
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(), Message: "the fixture writes tree index sets only"})
			continue
		}
		var indexes []treeIndex
		if err := json.Unmarshal(value.GetValue().GetCanonicalProjection(), &indexes); err != nil {
			return nil, err
		}
		group := &protocol.EncodedSourceGroup{ValueIds: []string{value.GetValue().GetId()}}
		for _, index := range indexes {
			group.Parts = append(group.Parts, &protocol.EncodedSourcePart{Fragment: treePart(request, value, index)})
		}
		result.Groups = append(result.Groups, group)
		if os.Getenv("FIXTURE_TREE_PARTIAL") == "1" {
			result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: value.GetValue().GetId(),
				Message: "the tree depth of table " + strconv.Quote(value.GetValue().GetValue().GetOwner().GetName()) + " is not represented in the source"})
		}
	}
	return result, nil
}

// treePart is the index part of one tree index: its fanout as text in Go and
// as a number in HCL.
func treePart(request *protocol.EncodeSourceRequest, value *protocol.EncodeSourceValue, index treeIndex) *protocol.SourceFragment {
	text := func(written string) *protocol.SourceValue {
		return &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: written}}
	}
	sequence := func(names []string) *protocol.SourceValue {
		items := &protocol.SourceSequence{}
		for _, name := range names {
			items.Values = append(items.Values, text(name))
		}
		return &protocol.SourceValue{Value: &protocol.SourceValue_Sequence{Sequence: items}}
	}
	inherited := &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: provider.IndexKindType, Value: text(index.Type)},
		{Key: provider.IndexKindColumns, Value: sequence(index.Columns)}}}
	if len(index.Include) != 0 {
		inherited.Entries = append(inherited.Entries, &protocol.SourceEntry{Key: provider.IndexKindInclude, Value: sequence(index.Include)})
	}
	if index.Unique {
		inherited.Entries = append(inherited.Entries, &protocol.SourceEntry{Key: provider.IndexKindUnique, Value: &protocol.SourceValue{Value: &protocol.SourceValue_Boolean{Boolean: true}}})
	}
	if index.Comment != "" {
		inherited.Entries = append(inherited.Entries, &protocol.SourceEntry{Key: provider.IndexKindComment, Value: text(index.Comment)})
	}
	if index.Ops != "" {
		inherited.Entries = append(inherited.Entries, &protocol.SourceEntry{Key: provider.IndexKindOps, Value: text(index.Ops)})
	}
	content := &protocol.SourceMapping{}
	if index.Fanout != 0 {
		fanout := text(strconv.FormatUint(index.Fanout, 10))
		if request.GetFormat() == protocol.SourceDocument_HCL {
			fanout = &protocol.SourceValue{Value: &protocol.SourceValue_Number{Number: strconv.FormatUint(index.Fanout, 10)}}
		}
		content.Entries = append(content.Entries, &protocol.SourceEntry{Key: "fanout", Value: fanout})
	}
	owner := value.GetValue().GetValue().GetOwner()
	return &protocol.SourceFragment{Id: value.GetBindingId() + "." + index.Name, IndexKind: indexKind(), Selector: indexKind().GetSelector(), BindingId: value.GetBindingId(),
		Subject:             &protocol.Reference{Engine: "fixture", Kind: "index", Database: owner.GetDatabase(), Schema: owner.GetSchema(), Path: []string{owner.GetName()}, Name: index.Name},
		InheritedAttributes: inherited, Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: content}}}}
}
