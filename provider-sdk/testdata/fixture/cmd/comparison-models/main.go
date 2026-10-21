// Package main exercises contextual comparison through an independent provider.
// Retention is a synthetic model; its changes are not executable SQLite DDL.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

const nativeKind = "example.org/fixture/retention-object"
const facetKind = "example.org/fixture/retention"
const changeKind = "example.org/fixture/retention-change"

func main() {
	description := comparisonDescription()
	owner := &comparer{Provider: &backend.Provider{}, description: description}
	err := runtime.Serve(runtime.Server{Description: description,
		Sessions:        owner,
		Inspector:       owner,
		Renderer:        owner,
		Executor:        owner,
		SQLAnalyzer:     owner,
		ValueNormalizer: owner,
		SourceDecoder:   owner,
		ValueConverter:  owner,
		ModelComparer:   owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type comparer struct {
	*backend.Provider
	description *protocol.Description
}

func comparisonDescription() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-contextual-comparison"
	description.Features = append(description.Features, provider.CompareModelsFeature)
	engine := description.Engines[0]
	for _, model := range engine.GetValueModels() {
		native := proto.CloneOf(model)
		native.Kind, native.SourceFormats = nativeKind, nil
		engine.ValueModels = append(engine.ValueModels, native)
	}
	definition := []byte(`{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"type":"integer","minimum":0},"after":{"type":"integer","minimum":0}}}`)
	definition, err := modeljson.Canonical(definition)
	if err != nil {
		panic(err)
	} // Invalid static JSON is a fixture programming error.
	digest := sha256.Sum256(definition)
	engine.ValueModels = append(engine.ValueModels,
		&protocol.ValueModel{Kind: changeKind,
			Representation:   protocol.ValueInput_CHANGE,
			SchemaVersion:    1,
			Encoding:         "json",
			DefinitionJson:   definition,
			DefinitionSha256: hex.EncodeToString(digest[:]),
			CanonicalFormat:  "retention-change-v1"})
	engine.Comparisons = []*protocol.ModelComparison{
		{Id: "objects", Placement: protocol.ModelComparison_OBJECTS, Kinds: []string{nativeKind}, ChangeKinds: []string{changeKind}, Actions: []string{"rotate"}},
		{Id: "facets", Placement: protocol.ModelComparison_FACETS, Kinds: []string{facetKind}, ChangeKinds: []string{changeKind}, OwnerKinds: []string{"table", "column", "constraint"}},
	}
	return description
}
