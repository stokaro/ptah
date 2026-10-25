// Package main runs the fixture with the entry keys of contract 25.3: the
// retention key of a YAML column and index entry, and the index type the
// index key and the attributes of the frontend's index directive read with
// their own.
//
// A retention suits an ordered index only, so the program refuses one on an
// index whose type, which stays the frontend's key, says it is a hash index.
// That refusal is what shows the inherited type reached the provider.
package main

import (
	"context"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-entry-keys"
	description.Features = append(description.Features, provider.EntryKeysFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.EntryKeysFeature)
	models := []string{"example.org/fixture/retention"}
	engine.YamlSourceKeys = append(engine.YamlSourceKeys,
		&protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_COLUMN, Key: "retention", Selector: "retention", Models: models},
		&protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_INDEX, Key: "retention", Selector: "retention", Models: models, Inherited: []string{"type"}},
	)
	for _, claim := range backend.ScopedGoSourceDirectives() {
		if claim.GetDirective() == "ptah:schema:index" {
			claim.Inherited = []string{"type"}
		}
		engine.GoSourceDirectives = append(engine.GoSourceDirectives, claim)
	}
	owner := &entryKeys{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type entryKeys struct{ *backend.Provider }

// DecodeSource reads the inherited index type of each fragment, refuses a
// retention on a hash index, and decodes the rest as the base fixture does. A
// Go index directive that writes the type and none of the program's
// attributes declares nothing.
func (p *entryKeys) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	forwarded := proto.CloneOf(request)
	quiet := make(map[string]bool)
	for _, document := range forwarded.GetDocuments() {
		kept := make([]*protocol.SourceFragment, 0, len(document.GetFragments()))
		for _, fragment := range document.GetFragments() {
			if indexType(fragment) == "hash" {
				return nil, fmt.Errorf("a fixture retention needs an ordered index, and index %q is a hash index", fragment.GetSubject().GetName())
			}
			ownKeys := len(fragment.GetValue().GetMapping().GetEntries())
			fragment.InheritedAttributes = nil
			if fragment.GetValue().GetMapping() != nil && ownKeys == 0 {
				quiet[document.GetId()+"/"+fragment.GetId()] = true
				continue
			}
			kept = append(kept, fragment)
		}
		document.Fragments = kept
	}
	decoded, err := p.Provider.DecodeSource(ctx, forwarded)
	if err != nil {
		return nil, err
	}
	for i, document := range request.GetDocuments() {
		results := decoded.GetDocuments()[i].GetFragments()
		answered := make([]*protocol.DecodedSourceFragment, 0, len(document.GetFragments()))
		for _, fragment := range document.GetFragments() {
			if quiet[document.GetId()+"/"+fragment.GetId()] {
				answered = append(answered, &protocol.DecodedSourceFragment{Id: fragment.GetId()})
				continue
			}
			answered, results = append(answered, results[0]), results[1:]
		}
		decoded.GetDocuments()[i].Fragments = answered
	}
	return decoded, nil
}

// indexType is the index type a fragment inherits, or empty.
func indexType(fragment *protocol.SourceFragment) string {
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		if entry.GetKey() == "type" {
			return entry.GetValue().GetText()
		}
	}
	return ""
}
