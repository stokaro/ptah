// Package main counts the objects an object comparison compares, over RPC.
// It matches a retention object both sides hold with the same value and
// reports every other object as undecided, so it never builds a change.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	"ptah.run/provider-sdk/modeljson"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

const changeKind = "example.org/fixture/retention-change"

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-sdk-comparison-counts"
	description.Features = append(description.Features, provider.CompareModelsFeature, provider.ComparisonCountsFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.ComparisonCountsFeature)
	definition, err := modeljson.Canonical([]byte(`{"type":"object"}`))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	digest := sha256.Sum256(definition)
	engine.ValueModels = append(engine.ValueModels, &protocol.ValueModel{Kind: changeKind, Representation: protocol.ValueInput_CHANGE, SchemaVersion: 1,
		Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]), CanonicalFormat: "retention-change-v1"})
	engine.Comparisons = []*protocol.ModelComparison{{Id: "objects", Placement: protocol.ModelComparison_OBJECTS,
		Kinds: []string{"example.org/fixture/retention"}, ChangeKinds: []string{changeKind}}}
	owner := &countingOwner{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, ModelComparer: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type countingOwner struct {
	*backend.Provider
}

// CompareModels counts its matched objects only for a request whose target
// features list the feature; the SDK refuses counts in any other reply.
func (*countingOwner) CompareModels(ctx context.Context, request *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.ComparedModels{Complete: true, Desired: proto.CloneOf(request.GetDesired())}
	var matched uint32
	for _, desired := range request.GetDesired().GetRecords() {
		current := findRecord(request.GetCurrent(), desired)
		if desired.GetValue() == nil || current.GetValue() == nil ||
			!bytes.Equal(desired.GetValue().GetCanonicalProjection(), current.GetValue().GetCanonicalProjection()) {
			result.Undecided = append(result.Undecided, undecided(desired))
			continue
		}
		matched++
	}
	for _, current := range request.GetCurrent().GetRecords() {
		if findRecord(request.GetDesired(), current) == nil {
			result.Undecided = append(result.Undecided, undecided(current))
		}
	}
	if slices.Contains(request.GetTargetFeatures(), provider.ComparisonCountsFeature) {
		result.Counts = []*protocol.ComparisonCount{{Kind: changeKind, Matched: matched}}
	}
	return result, nil
}

func findRecord(state *protocol.ComparisonState, subject *protocol.ComparisonRecord) *protocol.ComparisonRecord {
	for _, record := range state.GetRecords() {
		if record.GetKind() == subject.GetKind() && provider.CompareIdentities(record.GetSubject(), subject.GetSubject()) == 0 {
			return record
		}
	}
	return nil
}

func undecided(record *protocol.ComparisonRecord) *protocol.ComparisonUndecided {
	return &protocol.ComparisonUndecided{Kind: record.GetKind(), Subject: proto.CloneOf(record.GetSubject()), Reason: "This program builds no change"}
}
