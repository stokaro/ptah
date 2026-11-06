package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// twoTablePlanning is planningExchange with a second table, D, that declares
// and observes no setting and whose coverages hold the kind messages of the
// first table's coverages: the shape a host builds, which names one list of
// kinds for every table of a request.
func twoTablePlanning() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningExchange()
	first := request.Tables[0]
	second := proto.CloneOf(first)
	second.Subject.Name = &protocol.ComparisonPart{Source: "D", Normalized: "D"}
	second.Desired.Name, second.Current.Name = "D", "D"
	for _, pair := range [][2]*protocol.CapturedFeatures{{first.Desired.Features, second.Desired.Features}, {first.Current.Features, second.Current.Features}} {
		pair[1].Facets.Records = nil
		pair[1].Facets.Coverage.Kinds = pair[0].Facets.Coverage.Kinds
	}
	request.Tables = append(request.Tables, second)
	receipt := proto.CloneOf(result.Parents[0])
	receipt.Receipt.Subject = proto.CloneOf(second.Subject)
	result.Parents = append(result.Parents, receipt)
	return d, request, result
}

// facetCoverages returns the desired and observed facet coverage of each
// table of request, table by table.
func facetCoverages(request *protocol.PlanFeaturesRequest) []*protocol.ComparisonCoverage {
	var coverages []*protocol.ComparisonCoverage
	for _, table := range request.GetTables() {
		coverages = append(coverages, table.GetDesired().GetFeatures().GetFacets().GetCoverage(), table.GetCurrent().GetFeatures().GetFacets().GetCoverage())
	}
	return coverages
}

func TestShareCoverage_HappyPath(t *testing.T) {
	t.Run("a repeated list travels once and the request keeps every list", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := twoTablePlanning()
		c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
		before := proto.CloneOf(request)

		sent, isPlanning := provider.ShareCoverage(request).(*protocol.PlanFeaturesRequest)

		c.Assert(isPlanning, qt.IsTrue)
		coverages := facetCoverages(sent)
		c.Assert(coverages[0].GetKinds(), qt.HasLen, 1)
		c.Assert(coverages[1].GetKinds(), qt.HasLen, 1)
		c.Assert(coverages[0].GetKindsKey(), qt.Not(qt.Equals), uint32(0))
		c.Assert(coverages[1].GetKindsKey(), qt.Not(qt.Equals), coverages[0].GetKindsKey())
		c.Assert(coverages[2].GetKinds(), qt.HasLen, 0)
		c.Assert(coverages[2].GetKindsOf(), qt.Equals, coverages[0].GetKindsKey())
		c.Assert(coverages[3].GetKinds(), qt.HasLen, 0)
		c.Assert(coverages[3].GetKindsOf(), qt.Equals, coverages[1].GetKindsKey())
		c.Assert(proto.Equal(request, before), qt.IsTrue)
		c.Assert(proto.Size(sent) < proto.Size(request), qt.IsTrue)
	})
	t.Run("lists that differ travel whole", func(t *testing.T) {
		c := qt.New(t)
		_, request, _ := twoTablePlanning()
		for _, coverage := range facetCoverages(request)[2:] {
			kind := coverage.Kinds[0]
			coverage.Kinds = []*protocol.ComparisonKindCoverage{{Model: kind.Model, Sources: []*protocol.ComparisonKnowledgeSource{proto.CloneOf(kind.Sources[0])}}}
			coverage.Kinds[0].Sources[0].Outside.Reason = "another read"
		}

		c.Assert(provider.ShareCoverage(request), qt.Equals, proto.Message(request))
	})
	t.Run("kinds that differ in a field this SDK does not know travel whole", func(t *testing.T) {
		c := qt.New(t)
		_, request, _ := twoTablePlanning()
		for _, coverage := range facetCoverages(request)[2:] {
			kind := coverage.Kinds[0]
			coverage.Kinds = []*protocol.ComparisonKindCoverage{{Model: kind.Model, Sources: kind.Sources}}
			coverage.Kinds[0].ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
		}

		c.Assert(provider.ShareCoverage(request), qt.Equals, proto.Message(request))
	})
	t.Run("kinds of one model message with equal content are one list", func(t *testing.T) {
		c := qt.New(t)
		_, request, _ := twoTablePlanning()
		for _, coverage := range facetCoverages(request)[2:] {
			kind := coverage.Kinds[0]
			coverage.Kinds = []*protocol.ComparisonKindCoverage{{Model: kind.Model, Sources: []*protocol.ComparisonKnowledgeSource{proto.CloneOf(kind.Sources[0])}}}
		}

		sent := facetCoverages(provider.ShareCoverage(request).(*protocol.PlanFeaturesRequest))

		c.Assert(sent[2].GetKindsOf(), qt.Equals, sent[0].GetKindsKey())
		c.Assert(sent[2].GetKinds(), qt.HasLen, 0)
	})
	t.Run("a coverage without kinds travels as it is", func(t *testing.T) {
		c := qt.New(t)
		_, request, _ := twoTablePlanning()
		for _, coverage := range facetCoverages(request) {
			coverage.Kinds = nil
		}

		c.Assert(provider.ShareCoverage(request), qt.Equals, proto.Message(request))
	})
}

func TestResolveSharedCoverage_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, request, _ := twoTablePlanning()
	received := proto.CloneOf(provider.ShareCoverage(request).(*protocol.PlanFeaturesRequest))

	err := provider.ResolveSharedCoverage(received)

	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(received, request), qt.IsTrue)
	coverages := facetCoverages(received)
	// The coverages that shared a list hold the same kind messages, and the
	// copy CloneSharingModels makes for a service keeps them shared.
	c.Assert(coverages[2].GetKinds()[0], qt.Equals, coverages[0].GetKinds()[0])
	c.Assert(coverages[3].GetKinds()[0], qt.Equals, coverages[1].GetKinds()[0])
	copied := facetCoverages(provider.CloneSharingModels(received))
	c.Assert(copied[2].GetKinds()[0], qt.Equals, copied[0].GetKinds()[0])
	c.Assert(copied[2].GetKinds()[0], qt.Not(qt.Equals), coverages[0].GetKinds()[0])
}

// A message whose shared lists do not resolve is refused, before any service
// or validator reads it.
func TestResolveSharedCoverage_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		// change breaks the shared request: first defines a list, second
		// names it.
		change  func(first, second *protocol.ComparisonCoverage)
		wantErr string
	}{
		{name: "a key two coverages define", wantErr: `.*shared coverage key 1 is defined twice or without kinds`,
			change: func(first, second *protocol.ComparisonCoverage) {
				second.Kinds, second.KindsKey, second.KindsOf = first.Kinds, first.KindsKey, 0
			}},
		{name: "a key defined without kinds", wantErr: `.*shared coverage key 1 is defined twice or without kinds`,
			change: func(first, _ *protocol.ComparisonCoverage) { first.Kinds = nil }},
		{name: "kinds beside the key of other kinds", wantErr: `.*shared coverage names no kinds of the message under key 1`,
			change: func(first, second *protocol.ComparisonCoverage) { second.Kinds = first.Kinds }},
		{name: "a key no coverage defines", wantErr: `.*shared coverage names no kinds of the message under key 9`,
			change: func(_, second *protocol.ComparisonCoverage) { second.KindsOf = 9 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, request, _ := twoTablePlanning()
			received := proto.CloneOf(provider.ShareCoverage(request).(*protocol.PlanFeaturesRequest))
			coverages := facetCoverages(received)
			test.change(coverages[0], coverages[2])

			err := provider.ResolveSharedCoverage(received)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// Without the feature nothing resolves a shared list, and a validator refuses
// a coverage that still names one, whichever half it carries.
func TestSharedCoverageWithoutResolution_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		table int
	}{
		{name: "a coverage that defines a list", table: 0},
		{name: "a coverage that names a list", table: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := twoTablePlanning()
			sent := provider.ShareCoverage(request).(*protocol.PlanFeaturesRequest)
			unresolved := proto.CloneOf(request)
			unresolved.Tables[test.table] = sent.Tables[test.table]

			err := provider.ValidatePlanningRequest(d, unresolved)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*comparison coverage names a shared list of kinds the SDK did not resolve`)
		})
	}
}
