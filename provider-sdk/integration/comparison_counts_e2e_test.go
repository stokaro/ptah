//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// An independent provider counts what it compared for a host that lists the
// feature. The same comparison for a host that does not list it carries no
// counts, and that host cannot ask for them.
func TestIndependentProviderCountsComparedObjectsOnlyWhenNegotiated(t *testing.T) {
	c := qt.New(t)
	binary := buildSDKFixture(c, "sdk-comparison-counts")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	options := transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.CompareModelsFeature}, HostFeatures: []string{provider.ComparisonCountsFeature}}

	counting := start(c, options)
	c.Assert(counting.Description().Engines[0].Features, qt.Contains, provider.ComparisonCountsFeature)
	compared, err := counting.CompareModels(c.Context(), countedComparison(c, counting))
	c.Assert(err, qt.IsNil)
	c.Assert(compared.Counts, qt.HasLen, 1)
	// The object only the desired state holds is undecided and counted nowhere.
	c.Assert(proto.Equal(compared.Counts[0], &protocol.ComparisonCount{Kind: "example.org/fixture/retention-change", Matched: 1}), qt.IsTrue)
	c.Assert(compared.Undecided, qt.HasLen, 1)
	c.Assert(compared.Undecided[0].Subject.Name.Source, qt.Equals, "orphans")

	options.HostFeatures = nil
	plain := start(c, options)
	c.Assert(plain.Description().Engines[0].Features, qt.Not(qt.Contains), provider.ComparisonCountsFeature)
	compared, err = plain.CompareModels(c.Context(), countedComparison(c, plain))
	c.Assert(err, qt.IsNil)
	c.Assert(compared.Counts, qt.HasLen, 0)
	c.Assert(compared.Undecided, qt.HasLen, 1)
	forced := countedComparison(c, plain)
	forced.TargetFeatures = append(forced.TargetFeatures, provider.ComparisonCountsFeature)
	compared, err = plain.CompareModels(c.Context(), forced)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(compared, qt.IsNil)
}

// countedComparison compares, under the target features the client
// negotiated, one retention object both sides hold with the same value and
// one only the desired state holds.
func countedComparison(c *qt.C, client *transport.Client) *protocol.CompareModelsRequest {
	c.Helper()
	engine := client.Description().Engines[0]
	model := engine.ValueModels[0]
	desired, err := client.NormalizeValues(c.Context(), &protocol.NormalizeValuesRequest{Engine: "fixture", TargetFeatures: slices.Clone(engine.Features),
		Values: []*protocol.ValueInput{retentionInput(model.DefinitionSha256, "items", `{"seconds":60}`), retentionInput(model.DefinitionSha256, "orphans", `{"seconds":60}`)}})
	c.Assert(err, qt.IsNil)
	observed, err := client.ConvertValues(c.Context(), &protocol.ConvertValuesRequest{Engine: "fixture", TargetFeatures: slices.Clone(engine.Features),
		From: protocol.ValueInput_DESIRED, To: protocol.ValueInput_OBSERVED, Values: desired.Values[:1]})
	c.Assert(err, qt.IsNil)
	record := func(name string, value *protocol.NormalizedValue) *protocol.ComparisonRecord {
		return &protocol.ComparisonRecord{Kind: model.Kind, Subject: &protocol.ComparisonIdentity{Kind: model.Kind, Name: &protocol.ComparisonPart{Source: name, Normalized: name}}, Value: value}
	}
	return &protocol.CompareModelsRequest{Engine: "fixture", ComparisonId: "objects", TargetFeatures: slices.Clone(engine.Features),
		Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.Identifiers), DefaultSchema: engine.DefaultSchema}, Kinds: []string{model.Kind},
		Desired: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{},
			Records: []*protocol.ComparisonRecord{record("items", desired.Values[0]), record("orphans", desired.Values[1])}},
		Current: &protocol.ComparisonState{Representation: protocol.ValueInput_OBSERVED, Coverage: &protocol.ComparisonCoverage{},
			Records: []*protocol.ComparisonRecord{record("items", observed.Values[0])}},
	}
}
