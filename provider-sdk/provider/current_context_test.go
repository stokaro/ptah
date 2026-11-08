package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// currentTargets are the target features of a request that carries a current
// context of facets: the comparison's own, the declared facets it reads and
// the current context. objectTargetsWithContext are those of a request that
// carries a current context of objects.
var (
	currentTargets           = []string{"retention", provider.DeclaredFacetsFeature, provider.CurrentContextFeature}
	objectTargetsWithContext = []string{"retention", provider.DeclaredObjectsFeature, provider.CurrentContextFeature}
)

// withCurrentContext makes d list the current context for the provider and
// its engine.
func withCurrentContext(d *protocol.Description) {
	d.Features = append(d.Features, provider.CurrentContextFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.CurrentContextFeature)
}

// observedModel is the engine's observed model of kind.
func observedModel(d *protocol.Description, kind string) *protocol.ValueModel {
	index := slices.IndexFunc(d.Engines[0].ValueModels, func(model *protocol.ValueModel) bool {
		return model.GetKind() == kind && model.GetRepresentation() == protocol.ValueInput_OBSERVED
	})
	return proto.CloneOf(d.Engines[0].ValueModels[index])
}

// currentFacetsExchange is [declaredFacetsExchange] whose request carries the
// current retention facet of one table for each table name given, under a
// complete coverage of the kind.
func currentFacetsExchange(targetFeatures []string, tables ...string) (*protocol.Description, *protocol.CompareModelsRequest) {
	d, request := declaredFacetsExchange(targetFeatures, "orders")
	withCurrentContext(d)
	_, base, _ := comparisonExchange()
	facet := base.GetCurrent().GetRecords()[0]
	request.CurrentContext = &protocol.ComparisonState{Representation: protocol.ValueInput_OBSERVED, Coverage: &protocol.ComparisonCoverage{
		Kinds: []*protocol.ComparisonKindCoverage{{Model: observedModel(d, retentionKind),
			Sources: []*protocol.ComparisonKnowledgeSource{{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}},
	}}
	for _, table := range tables {
		held := proto.CloneOf(facet)
		held.Subject.Name = &protocol.ComparisonPart{Source: table, Normalized: table}
		held.Value.Id = facet.GetValue().GetId() + "-current-" + table
		request.CurrentContext.Records = append(request.CurrentContext.Records, held)
	}
	return d, request
}

// currentObjectsExchange is [declaredObjectsExchange] whose request carries
// the current topic of each name given, and a topic queue the read recorded
// without describing it.
func currentObjectsExchange(targetFeatures []string, names ...string) (*protocol.Description, *protocol.CompareModelsRequest) {
	d, request := declaredObjectsExchange(targetFeatures)
	withCurrentContext(d)
	value := request.GetCurrent().GetRecords()[0].GetValue()
	topic := func(name string) *protocol.ComparisonIdentity {
		return &protocol.ComparisonIdentity{Kind: topicKind, Name: &protocol.ComparisonPart{Source: name, Normalized: name}, Schema: &protocol.ComparisonPart{Source: "app", Normalized: "app"}}
	}
	request.CurrentContext = &protocol.ComparisonState{Representation: protocol.ValueInput_OBSERVED, Coverage: &protocol.ComparisonCoverage{
		Kinds: []*protocol.ComparisonKindCoverage{{Model: observedModel(d, topicKind),
			Sources: []*protocol.ComparisonKnowledgeSource{{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}},
		Subjects: []*protocol.ComparisonSubjectCoverage{{Kind: topicKind, Subject: topic("queue"),
			Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "a persistent queue group, which the read does not describe"}}},
	}}
	for _, name := range names {
		held := proto.CloneOf(value)
		held.Id = value.GetId() + "-current-" + name
		held.Value.Kind = topicKind
		request.CurrentContext.Records = append(request.CurrentContext.Records, &protocol.ComparisonRecord{Kind: topicKind, Subject: topic(name), Value: held})
	}
	return d, request
}

// withoutCurrentContext lists every two-way feature but the current context.
func withoutCurrentContext() []string {
	return slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool { return feature == provider.CurrentContextFeature })
}

func TestCurrentContext_HappyPath(t *testing.T) {
	t.Run("current facets of a declared facet kind", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets, "orders", "users")

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
	})
	t.Run("current objects of a declared kind and one the read recorded without describing it", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentObjectsExchange(objectTargetsWithContext, "events", "orders")

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
	})
	t.Run("a coverage without records", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets)

		c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
	})
	t.Run("no current context for a target that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(facetTargets(true))
		request.CurrentContext = nil

		c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
	})
}

// A host that does not list the feature sees an engine without it, and the
// description it receives is still valid.
func TestCurrentContextNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, _ := currentFacetsExchange(currentTargets)

	negotiated := provider.NegotiatedDescription(d, withoutCurrentContext())

	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.CurrentContextFeature)
	c.Assert(negotiated.GetFeatures(), qt.Not(qt.Contains), provider.CurrentContextFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(provider.NegotiatedDescription(d, provider.TwoWayFeatures()).GetEngines()[0].GetFeatures(), qt.Contains, provider.CurrentContextFeature)
}

// A current context goes only where both sides negotiated the feature, and
// each of its facets is of a kind the comparison reads, owned by an object of
// a kind the facet's comparison owns, with a value, and listed once.
func TestCurrentContext_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		targets   []string
		kind      string
		ownerKind string
		tables    []string
		want      string
	}{
		{"a target that does not list the feature", facetTargets(true), retentionKind, "table", []string{"orders"},
			`.*a current context needs models\.current-context on both sides`},
		{"a kind the comparison does not read", currentTargets, topicKind, "table", []string{"orders"},
			`.*a current context record of kind "example\.org/fixture/topic" is not one comparison "topics" reads`},
		{"an owner of a kind the facet comparison does not own", currentTargets, retentionKind, "view", []string{"orders"},
			`.*a current context facet of kind "example\.org/fixture/retention" has an owner of kind "view"`},
		{"a facet listed twice", currentTargets, retentionKind, "table", []string{"orders", "orders"}, `current context: .*duplicate comparison record`},
		{"an owner without a name", currentTargets, retentionKind, "table", []string{""}, `current context: .*comparison identity requires a kind and name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := currentFacetsExchange(test.targets, test.tables...)
			for _, record := range request.CurrentContext.Records {
				record.Kind, record.Subject.Kind = test.kind, test.ownerKind
			}

			err := provider.ValidateComparisonRequest(d, request)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("a facet without a value", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets, "orders")
		request.CurrentContext.Records[0].Value = nil

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `current context: .*missing value is not an explicitly excluded facet`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("the desired representation", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets, "orders")
		request.CurrentContext.Representation = protocol.ValueInput_DESIRED

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `current context: .*comparison state has the wrong representation`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("no coverage", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets, "orders")
		request.CurrentContext.Coverage = nil

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `current context: .*comparison coverage must be explicit`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("a coverage of a kind the comparison does not read", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets)
		request.CurrentContext.Coverage.Kinds[0].Model = observedModel(d, topicKind)

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `current context: .*unknown, changed, or duplicate comparison coverage model`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("a recorded subject of a kind the coverage does not enroll", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentObjectsExchange(objectTargetsWithContext)
		request.CurrentContext.Coverage.Kinds = nil

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `current context: .*comparison subject coverage has no enrolled model`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("a comparison that names no declared kind", func(t *testing.T) {
		c := qt.New(t)
		d, request := currentFacetsExchange(currentTargets, "orders")
		request.DeclaredFacets = nil
		d.Engines[0].Comparisons[1].DeclaredFacetKinds = nil

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `.*comparison "topics" names no declared kind or declared facet kind, so it receives no current context`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("a current context sent to a facet comparison", func(t *testing.T) {
		c := qt.New(t)
		d, base, _ := comparisonExchange()
		withCurrentContext(d)
		base.TargetFeatures = []string{"retention", provider.CurrentContextFeature}
		base.CurrentContext = proto.CloneOf(base.GetCurrent())

		err := provider.ValidateComparisonRequest(d, base)

		c.Assert(err, qt.ErrorMatches, `.*facet comparison contains native object context`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
}

// A host that does not list the feature sees an engine without it, and a
// target without it, so a current context it is sent is refused.
func TestCurrentContextNegotiation_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := currentFacetsExchange(facetTargets(true), "orders")

	err := provider.ValidateComparisonRequest(provider.NegotiatedDescription(d, withoutCurrentContext()), request)

	c.Assert(err, qt.ErrorMatches, `.*a current context needs models\.current-context on both sides`)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}

// The engine's feature needs the provider's and a model comparison.
func TestCurrentContextDescription_FailurePath(t *testing.T) {
	t.Run("an engine feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		d, _ := currentFacetsExchange(currentTargets)
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.CurrentContextFeature })

		err := provider.ValidateDescription(d, nil)

		c.Assert(err, qt.ErrorMatches, `.*a current context requires the provider's feature and a model comparison`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("an engine without a model comparison", func(t *testing.T) {
		c := qt.New(t)
		d, _ := currentFacetsExchange(currentTargets)
		d.Engines[0].Comparisons = nil
		d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.DeclaredFacetsFeature })

		err := provider.ValidateDescription(d, nil)

		c.Assert(err, qt.ErrorMatches, `.*a current context requires the provider's feature and a model comparison`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
}
