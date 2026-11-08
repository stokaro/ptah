package provider_test

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// countedKind is the one change kind of the counting exchange.
const countedKind = "example.org/fixture/retention-change"

// countingExchange is a comparison of the placement that negotiated counts:
// the provider, the engine and the request list the feature. A facet
// comparison's settings belong to tables. Its reply holds changes changes of
// the change kind, each of an object of its own, and no counts.
func countingExchange(placement protocol.ModelComparison_Placement, changes int) (*protocol.Description, *protocol.CompareModelsRequest, *protocol.ComparedModels) {
	d, request, result := comparisonExchange()
	d.Features = append(d.Features, provider.ComparisonCountsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ComparisonCountsFeature)
	service := engine.Comparisons[0]
	service.Placement, service.OwnerKinds = placement, map[protocol.ModelComparison_Placement][]string{protocol.ModelComparison_FACETS: {"table"}}[placement]
	request.TargetFeatures = append(request.TargetFeatures, provider.ComparisonCountsFeature)
	template := result.Changes[0]
	result.Changes = nil
	for i := range changes {
		change := proto.CloneOf(template)
		name := fmt.Sprintf("object-%d", i)
		change.Subject.Name = &protocol.ComparisonPart{Source: name, Normalized: name}
		change.Value.Id = name
		result.Changes = append(result.Changes, change)
	}
	return d, request, result
}

func withoutCounts(features []string) []string {
	return slices.DeleteFunc(slices.Clone(features), func(feature string) bool { return feature == provider.ComparisonCountsFeature })
}

func TestComparisonCounts_HappyPath(t *testing.T) {
	objects, facets := protocol.ModelComparison_OBJECTS, protocol.ModelComparison_FACETS
	for _, test := range []struct {
		name      string
		placement protocol.ModelComparison_Placement
		changes   int
		counts    []*protocol.ComparisonCount
	}{
		{"a matched object that differs", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1}}},
		{"an added object", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Added: 1}}},
		{"a removed object", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Removed: 1}}},
		{"matched objects that do not differ", objects, 0, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 4}}},
		{"a kind the batch holds nothing of", objects, 0, []*protocol.ComparisonCount{{Kind: countedKind}}},
		{"an addition, a removal and a changed object beside an unchanged one", objects, 3, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 2, Added: 1, Removed: 1}}},
		{"counts whose sum does not fit in 32 bits", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: math.MaxUint32, Added: 1}}},
		{"no counts", objects, 2, nil},
		{"a table whose setting both sides hold and differ on", facets, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1}}},
		{"a table only the desired setting stands for, beside unchanged ones", facets, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 3, Added: 1}}},
		{"a facet comparison that does not count", facets, 1, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := countingExchange(test.placement, test.changes)
			result.Counts = test.counts
			before := proto.CloneOf(result)
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateComparedModels(d, request, result), qt.IsNil)
			c.Assert(proto.Equal(result, before), qt.IsTrue)
		})
	}
}

func TestComparisonCounts_FailurePath(t *testing.T) {
	objects, facets := protocol.ModelComparison_OBJECTS, protocol.ModelComparison_FACETS
	contradicted := func(matched, added, removed, changes uint64) string {
		return fmt.Sprintf(`comparison counted %d matched, %d added and %d removed objects of change kind %q, which its %d changes of the kind contradict`,
			matched, added, removed, countedKind, changes)
	}
	for _, test := range []struct {
		name      string
		placement protocol.ModelComparison_Placement
		changes   int
		counts    []*protocol.ComparisonCount
		want      string
	}{
		{"an addition and a removal from one change", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Added: 1, Removed: 1}}, contradicted(0, 1, 1, 1)},
		{"more additions than changes", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1, Added: 2}}, contradicted(1, 2, 0, 1)},
		{"more removals than changes", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1, Removed: 2}}, contradicted(1, 0, 2, 1)},
		{"an addition with no change", objects, 0, []*protocol.ComparisonCount{{Kind: countedKind, Added: 1}}, contradicted(0, 1, 0, 0)},
		{"additions and removals whose sum does not fit in 32 bits", objects, 1,
			[]*protocol.ComparisonCount{{Kind: countedKind, Matched: 1, Added: math.MaxUint32, Removed: 1}}, contradicted(1, math.MaxUint32, 1, 1)},
		{"more changes than counted objects", objects, 3, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1, Added: 1}}, contradicted(1, 1, 0, 3)},
		{"a change and no counted object", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind}}, contradicted(0, 0, 0, 1)},
		{"a facet change and no counted table", facets, 1, []*protocol.ComparisonCount{{Kind: countedKind}}, contradicted(0, 0, 0, 1)},
		{"the model kind of the changes", objects, 1, []*protocol.ComparisonCount{{Kind: "example.org/fixture/retention", Matched: 1}},
			`comparison counted a change kind "example.org/fixture/retention" the service lacks, or counted it twice`},
		{"an empty kind", objects, 0, []*protocol.ComparisonCount{{Matched: 1}},
			`comparison counted a change kind "" the service lacks, or counted it twice`},
		{"a kind counted twice", objects, 1, []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1}, {Kind: countedKind, Matched: 1}},
			`comparison counted a change kind "` + countedKind + `" the service lacks, or counted it twice`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := countingExchange(test.placement, test.changes)
			c.Assert(provider.ValidateComparedModels(d, request, result), qt.IsNil)
			result.Counts = test.counts
			err := provider.ValidateComparedModels(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// Counts the request did not negotiate are refused, and the same reply
// without them is accepted.
func TestComparisonCountsNegotiation_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.CompareModelsRequest)
	}{
		{"the host did not list the feature", "comparison counts need models.compare-counts", func(d *protocol.Description, r *protocol.CompareModelsRequest) {
			negotiated := provider.NegotiatedDescription(d, withoutCounts(provider.TwoWayFeatures()))
			proto.Reset(d)
			proto.Merge(d, negotiated)
			r.TargetFeatures = withoutCounts(r.TargetFeatures)
		}},
		{"the provider did not list the feature", "comparison counts need models.compare-counts", func(d *protocol.Description, _ *protocol.CompareModelsRequest) {
			d.Features = withoutCounts(d.Features)
		}},
		{"the engine did not list the feature", "comparison counts need models.compare-counts", func(d *protocol.Description, r *protocol.CompareModelsRequest) {
			d.Engines[0].Features = withoutCounts(d.Engines[0].Features)
			r.TargetFeatures = withoutCounts(r.TargetFeatures)
		}},
		{"the request did not list the feature", "comparison counts need models.compare-counts", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.TargetFeatures = withoutCounts(r.TargetFeatures)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := countingExchange(protocol.ModelComparison_OBJECTS, 1)
			test.mutate(d, request)
			c.Assert(provider.ValidateComparedModels(d, request, result), qt.IsNil)
			result.Counts = []*protocol.ComparisonCount{{Kind: countedKind, Matched: 1}}
			err := provider.ValidateComparedModels(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// The feature is two-way: a host that does not list it does not see it, from
// the provider or from the engine.
func TestComparisonCountsFeature_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ComparisonCountsFeature)
	d, _, _ := countingExchange(protocol.ModelComparison_OBJECTS, 0)
	listed := provider.NegotiatedDescription(d, []string{provider.ComparisonCountsFeature})
	c.Assert(listed.Features, qt.Contains, provider.ComparisonCountsFeature)
	c.Assert(listed.Engines[0].Features, qt.Contains, provider.ComparisonCountsFeature)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Features, qt.Not(qt.Contains), provider.ComparisonCountsFeature)
	c.Assert(unlisted.Engines[0].Features, qt.Not(qt.Contains), provider.ComparisonCountsFeature)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
}

func TestComparisonCountsFeature_FailurePath(t *testing.T) {
	t.Run("a provider without model comparison", func(t *testing.T) {
		c := qt.New(t)
		d := description()
		d.Features = []string{provider.NormalizeValuesFeature, provider.ComparisonCountsFeature}
		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*models\.compare-counts requires models\.compare`)
	})
	t.Run("an engine whose provider does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := countingExchange(protocol.ModelComparison_OBJECTS, 0)
		d.Features = withoutCounts(d.Features)
		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*engine models\.compare-counts requires the provider's`)
	})
}
