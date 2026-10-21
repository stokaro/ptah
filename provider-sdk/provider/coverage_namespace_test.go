package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func scopedModelCoverage() (*protocol.Description, *protocol.ModelCoverage) {
	description, coverage := modelCoverage()
	coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "outside the selected namespace"}
	coverage.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{{
		Scope: &protocol.ModelNamespace{
			Database:   &protocol.NamespaceComponent{Source: "Production", Normalized: "production"},
			Catalog:    &protocol.NamespaceComponent{},
			Schema:     &protocol.NamespaceComponent{Source: "Private", Normalized: "private"},
			PathPrefix: []*protocol.NamespaceComponent{{Source: "a/b", Normalized: "a/b"}, {Source: "C", Normalized: "c"}},
		},
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE},
	}}
	return description, coverage
}

func TestCoverageNamespacePreservesComponentsAndPresence(t *testing.T) {
	c := qt.New(t)
	description, coverage := scopedModelCoverage()
	wildcard := proto.CloneOf(coverage.Kinds[0].Regions[0])
	wildcard.Scope.Catalog = nil
	wildcard.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNREPRESENTABLE, Reason: "catalog unavailable"}
	path := proto.CloneOf(coverage.Kinds[0].Regions[0])
	path.Scope.PathPrefix = []*protocol.NamespaceComponent{{Source: "a", Normalized: "a"}, {Source: "b/c", Normalized: "b/c"}}
	coverage.Kinds[0].Regions = append(coverage.Kinds[0].Regions, wildcard, path)
	original := proto.CloneOf(coverage)
	c.Assert(provider.ValidateModelCoverage(description, "fixture", coverage), qt.IsNil)
	c.Assert(proto.Equal(coverage, original), qt.IsTrue)
	wire, err := proto.Marshal(coverage)
	c.Assert(err, qt.IsNil)
	decoded := &protocol.ModelCoverage{}
	c.Assert(proto.Unmarshal(wire, decoded), qt.IsNil)
	c.Assert(proto.Equal(decoded, original), qt.IsTrue)
	c.Assert(decoded.Kinds[0].Regions[0].Scope.Catalog, qt.IsNotNil)
	c.Assert(decoded.Kinds[0].Regions[1].Scope.Catalog, qt.IsNil)
}

func TestCoverageNamespaceRefusesInvalidOrAmbiguousRegions(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ModelKindCoverage)
	}{
		{"nil region", func(k *protocol.ModelKindCoverage) { k.Regions[0] = nil }},
		{"nil scope", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope = nil }},
		{"unbounded", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope = &protocol.ModelNamespace{} }},
		{"missing normalized qualifier", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.Database.Normalized = "" }},
		{"missing source qualifier", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.Schema.Source = "" }},
		{"invalid UTF-8 qualifier", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.Database.Source = "\xff" }},
		{"nil path component", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.PathPrefix[0] = nil }},
		{"empty path component", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.PathPrefix[0] = &protocol.NamespaceComponent{} }},
		{"missing normalized path", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.PathPrefix[0].Normalized = "" }},
		{"invalid UTF-8 path", func(k *protocol.ModelKindCoverage) { k.Regions[0].Scope.PathPrefix[0].Normalized = "\xff" }},
		{"nil knowledge", func(k *protocol.ModelKindCoverage) { k.Regions[0].Knowledge = nil }},
		{"absence", func(k *protocol.ModelKindCoverage) { k.Regions[0].Knowledge.State = protocol.ModelKnowledge_ABSENT }},
		{"default", func(k *protocol.ModelKindCoverage) { k.Regions[0].Knowledge.State = protocol.ModelKnowledge_DEFAULTED }},
		{"unknown state", func(k *protocol.ModelKindCoverage) { k.Regions[0].Knowledge.State = 99 }},
		{"unexplained unknown", func(k *protocol.ModelKindCoverage) {
			k.Regions[0].Knowledge.State = protocol.ModelKnowledge_UNINSPECTED
		}},
		{"duplicate normalized namespace", func(k *protocol.ModelKindCoverage) {
			other := proto.CloneOf(k.Regions[0])
			other.Scope.Database.Source = "PRODUCTION"
			k.Regions = append(k.Regions, other)
		}},
		{"region limit", func(k *protocol.ModelKindCoverage) { k.Regions = make([]*protocol.ModelNamespaceKnowledge, 4097) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, coverage := scopedModelCoverage()
			c.Assert(provider.ValidateModelCoverage(description, "fixture", coverage), qt.IsNil)
			test.mutate(coverage.Kinds[0])
			c.Assert(provider.ValidateModelCoverage(description, "fixture", coverage), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
