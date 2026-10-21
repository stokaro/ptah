// Package providersource converts captured frontend data to the provider source
// contract. It does not interpret engine syntax or call a provider process.
package providersource

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/yamlext"
)

// YAML captures one engine's negotiated YAML keys and all common table bindings.
// Missing keys still enroll their models so the owner must state their coverage.
// Other engines' fragments are excluded, without guessing a default engine.
// The caller validates the complete request against the negotiated description.
func YAML(ctx context.Context, engine *protocol.Engine, input yamlext.Document) (*protocol.DecodeSourceRequest, error) {
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_YAML,
		File: cmp.Or(input.Filename, "<unnamed YAML>"), ByteLength: uint64(len(input.Source))}
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED && slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_YAML) {
			document.Models = append(document.Models, proto.CloneOf(model))
		}
	}
	for _, table := range input.Tables {
		document.Bindings = append(document.Bindings, &protocol.SourceBinding{Id: table.Key, EntryKey: table.Key, StructName: table.Struct,
			Object: &protocol.Reference{Engine: engine.GetId(), Kind: "table", Schema: table.Schema, Name: table.Name}})
	}
	walk := yamlWalk{ctx: ctx, source: input.Source, active: make(map[*yamlext.Node]bool)}
	for i, fragment := range input.Fragments {
		claim := yamlClaim(engine, fragment)
		if claim == nil {
			continue
		}
		value, err := walk.value(fragment.Node, 0)
		if err != nil {
			return nil, fmt.Errorf("YAML %s at %v: %w", document.GetFile(), fragment.Path, err)
		}
		captured := &protocol.SourceFragment{Id: strconv.Itoa(i), YamlKey: proto.CloneOf(claim), Selector: claim.GetSelector(),
			Span: value.GetSpan(), Content: &protocol.SourceFragment_Value{Value: value}}
		if fragment.Table >= 0 {
			if fragment.Table >= len(input.Tables) {
				return nil, fmt.Errorf("YAML fragment has no table binding")
			}
			captured.BindingId = input.Tables[fragment.Table].Key
		}
		document.Fragments = append(document.Fragments, captured)
	}
	request := &protocol.DecodeSourceRequest{Engine: engine.GetId(), Documents: []*protocol.SourceDocument{document}}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateSourceDocuments(engine.GetId(), request.GetDocuments()); err != nil {
		return nil, err
	}
	return request, nil
}

func yamlClaim(engine *protocol.Engine, fragment yamlext.Fragment) *protocol.YAMLSourceKey {
	scope := protocol.YAMLSourceKey_DOCUMENT
	if fragment.Table >= 0 {
		scope = protocol.YAMLSourceKey_TABLE
	}
	if len(fragment.Path) == 0 {
		return nil
	}
	key := fragment.Path[len(fragment.Path)-1]
	for _, claim := range engine.GetYamlSourceKeys() {
		if claim.GetScope() == scope && claim.GetKey() == key {
			return claim
		}
	}
	return nil
}
