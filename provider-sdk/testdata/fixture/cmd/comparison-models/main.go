// Package main exercises contextual comparison through an independent provider.
// Retention is a synthetic model; its changes are not executable SQLite DDL.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/protobuf/proto"

	"ptah.run/provider-sdk/modeljson"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

const nativeKind = "example.org/fixture/retention-object"
const facetKind = "example.org/fixture/retention"
const changeKind = "example.org/fixture/retention-change"

// anchorKind is the object model of the anchors comparison, which the
// fixture declares only with FIXTURE_DECLARED_OBJECTS=on.
const anchorKind = "example.org/fixture/retention-anchor"

// declaredObjects reports the mode in which the objects comparison reads the
// declared anchors (provider.DeclaredObjectsFeature): each retention object
// keeps its rows in the anchor of its name, so it refuses one whose anchor
// the schema does not declare.
func declaredObjects() bool {
	return os.Getenv("FIXTURE_DECLARED_OBJECTS") == "on"
}

// declaredFacets reports the mode in which the objects comparison reads the
// declared retention facets (provider.DeclaredFacetsFeature): each retention
// object follows the retention facet of the table of its name, so it refuses
// one whose table declares no retention facet.
func declaredFacets() bool {
	return os.Getenv("FIXTURE_DECLARED_FACETS") == "on"
}

// currentContext reports the mode in which the objects comparison of the
// declared facets mode also reads the current retention facets
// (provider.CurrentContextFeature): a retention object may follow the facet a
// table holds, which the plan keeps, as well as one it declares.
func currentContext() bool {
	return os.Getenv("FIXTURE_CURRENT_CONTEXT") == "on"
}

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
	// The planning policy lets the host send a rotation as the operator
	// wrote it; the objects comparison reads the object's name from it. The
	// declared relations tell it which tables the desired schema declares, so
	// it refuses a retention object that takes one's name, with a reason.
	description.Features = append(description.Features, provider.CompareModelsFeature, provider.PlanningPolicyFeature, provider.DeclaredRelationsFeature,
		provider.ErrorReasonsFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.PlanningPolicyFeature, provider.DeclaredRelationsFeature)
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
	if declaredObjects() {
		withAnchors(description)
	}
	if declaredFacets() {
		description.Features = append(description.Features, provider.DeclaredFacetsFeature)
		engine.Features = append(engine.Features, provider.DeclaredFacetsFeature)
		engine.Comparisons[0].DeclaredFacetKinds = []string{facetKind}
		if currentContext() {
			description.Features = append(description.Features, provider.CurrentContextFeature)
			engine.Features = append(engine.Features, provider.CurrentContextFeature)
		}
	}
	return description
}

// withAnchors adds the anchors comparison, whose declared objects the objects
// comparison reads under provider.DeclaredObjectsFeature.
func withAnchors(description *protocol.Description) {
	description.Features = append(description.Features, provider.DeclaredObjectsFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.DeclaredObjectsFeature)
	for _, model := range engine.GetValueModels() {
		if model.GetKind() != nativeKind {
			continue
		}
		anchor := proto.CloneOf(model)
		anchor.Kind = anchorKind
		engine.ValueModels = append(engine.ValueModels, anchor)
	}
	engine.Comparisons[0].DeclaredKinds = []string{anchorKind}
	engine.Comparisons = append(engine.Comparisons,
		&protocol.ModelComparison{Id: "anchors", Placement: protocol.ModelComparison_OBJECTS, Kinds: []string{anchorKind}, ChangeKinds: []string{changeKind}})
}
