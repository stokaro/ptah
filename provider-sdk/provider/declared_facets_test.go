package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// retentionKind is the facet model the comparison exchange's "retention"
// comparison owns on tables.
const retentionKind = "example.org/fixture/retention"

// declaredFacetsExchange is an object comparison "topics" that reads the
// declared retention facets of the engine's facet comparison "retention", for
// a target whose features are targetFeatures. The request names the
// retention facet of one table for each table name given.
func declaredFacetsExchange(targetFeatures []string, tables ...string) (*protocol.Description, *protocol.CompareModelsRequest) {
	d, base, _ := comparisonExchange()
	engine := d.Engines[0]
	kinds := map[protocol.ValueInput_Representation]string{protocol.ValueInput_DESIRED: topicKind, protocol.ValueInput_OBSERVED: topicKind,
		protocol.ValueInput_CHANGE: topicKind + "-change"}
	models := make(map[protocol.ValueInput_Representation]*protocol.ValueModel)
	for _, model := range engine.ValueModels {
		topic := proto.CloneOf(model)
		topic.Kind = kinds[model.Representation]
		models[model.Representation] = topic
		engine.ValueModels = append(engine.ValueModels, topic)
	}
	engine.Comparisons = append(engine.Comparisons, &protocol.ModelComparison{Id: "topics", Placement: protocol.ModelComparison_OBJECTS,
		Kinds: []string{topicKind}, ChangeKinds: []string{topicKind + "-change"}, DeclaredFacetKinds: []string{retentionKind}})
	d.Features = append(d.Features, provider.DeclaredFacetsFeature)
	engine.Features = append(engine.Features, provider.DeclaredFacetsFeature)
	state := func(representation protocol.ValueInput_Representation) *protocol.ComparisonState {
		coverage := proto.CloneOf(base.GetDesired().GetCoverage())
		coverage.Kinds[0].Model = proto.CloneOf(models[representation])
		return &protocol.ComparisonState{Representation: representation, Coverage: coverage}
	}
	request := &protocol.CompareModelsRequest{Engine: base.GetEngine(), ComparisonId: "topics", TargetFeatures: targetFeatures,
		Identifiers: proto.CloneOf(base.GetIdentifiers()), Kinds: []string{topicKind},
		Desired: state(protocol.ValueInput_DESIRED), Current: state(protocol.ValueInput_OBSERVED)}
	facet := base.GetDesired().GetRecords()[0]
	for _, table := range tables {
		declared := proto.CloneOf(facet)
		declared.Subject.Name = &protocol.ComparisonPart{Source: table, Normalized: table}
		declared.Value.Id = facet.GetValue().GetId() + "-" + table
		request.DeclaredFacets = append(request.DeclaredFacets, declared)
	}
	return d, request
}

// facetTargets are the target features of the exchange's request: the
// comparison's own, and declared facets as well for a listed target.
func facetTargets(listed bool) []string {
	return map[bool][]string{false: {"retention"}, true: {"retention", provider.DeclaredFacetsFeature}}[listed]
}

// withoutDeclaredFacets lists every two-way feature but declared facets.
func withoutDeclaredFacets() []string {
	return slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool { return feature == provider.DeclaredFacetsFeature })
}

func TestDeclaredFacets_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		listed bool
		tables []string
	}{
		{"declared facets of a kind the comparison reads", true, []string{"orders", "users"}},
		{"no declared facet", true, nil},
		{"no declared facet for a target that does not list the feature", false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := declaredFacetsExchange(facetTargets(test.listed), test.tables...)

			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
		})
	}
}

// A host that does not list the feature receives no declared facet kinds, and
// the description it receives is still valid.
func TestDeclaredFacetsNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, _ := declaredFacetsExchange(facetTargets(true))

	negotiated := provider.NegotiatedDescription(d, withoutDeclaredFacets())

	c.Assert(negotiated.GetEngines()[0].GetComparisons()[1].GetDeclaredFacetKinds(), qt.HasLen, 0)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.DeclaredFacetsFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(provider.NegotiatedDescription(d, provider.TwoWayFeatures()).GetEngines()[0].GetComparisons()[1].GetDeclaredFacetKinds(),
		qt.DeepEquals, []string{retentionKind})
}

// A request names declared facets only where both sides negotiated the
// feature, each of a kind the comparison reads, owned by an object of a kind
// the facet's comparison owns, with a value of its kind, and listed once.
func TestDeclaredFacets_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		listed    bool
		kind      string
		ownerKind string
		tables    []string
		want      string
	}{
		{"a target that does not list the feature", false, retentionKind, "table", []string{"orders"}, `.*declared facets need models\.declared-facets on both sides`},
		{"a kind the comparison does not read", true, topicKind, "table", []string{"orders"},
			`.*a declared facet of kind "example\.org/fixture/topic" is not one comparison "topics" reads`},
		{"an owner of a kind the facet comparison does not own", true, retentionKind, "view", []string{"orders"},
			`.*a declared facet of kind "example\.org/fixture/retention" has an owner of kind "view"`},
		{"a facet listed twice", true, retentionKind, "table", []string{"orders", "orders"}, `.*a declared facet is listed twice`},
		{"an owner without a name", true, retentionKind, "table", []string{""}, `.*comparison identity requires a kind and name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := declaredFacetsExchange(facetTargets(test.listed), test.tables...)
			for _, facet := range request.DeclaredFacets {
				facet.Kind, facet.Subject.Kind = test.kind, test.ownerKind
			}

			err := provider.ValidateComparisonRequest(d, request)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("a facet without a value", func(t *testing.T) {
		c := qt.New(t)
		d, request := declaredFacetsExchange(facetTargets(true), "orders")
		request.DeclaredFacets[0].Value = nil

		err := provider.ValidateComparisonRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `.*a declared facet of kind "example\.org/fixture/retention" carries no value of its kind`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("declared facets sent to a facet comparison", func(t *testing.T) {
		c := qt.New(t)
		d, base, _ := comparisonExchange()
		d.Features = append(d.Features, provider.DeclaredFacetsFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.DeclaredFacetsFeature)
		base.TargetFeatures = facetTargets(true)
		base.DeclaredFacets = []*protocol.ComparisonRecord{proto.CloneOf(base.GetDesired().GetRecords()[0])}

		err := provider.ValidateComparisonRequest(d, base)

		c.Assert(err, qt.ErrorMatches, `.*facet comparison contains native object context`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
}

// A host that does not list the feature sees an engine without it, and a
// target without it, so a declared facet it is sent is refused.
func TestDeclaredFacetsNegotiation_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := declaredFacetsExchange(facetTargets(false), "orders")

	err := provider.ValidateComparisonRequest(provider.NegotiatedDescription(d, withoutDeclaredFacets()), request)

	c.Assert(err, qt.ErrorMatches, `.*declared facets need models\.declared-facets on both sides`)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}

// The engine's feature needs the provider's and a model comparison, and only
// an object comparison names declared facet kinds, each one a facet
// comparison of the engine owns.
func TestDeclaredFacetsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name          string
		providerLists bool
		engineLists   bool
		placement     protocol.ModelComparison_Placement
		ownerKinds    []string
		declared      []string
		want          string
	}{
		{"an engine feature without the provider's", false, true, protocol.ModelComparison_OBJECTS, nil, []string{retentionKind},
			`.*declared facets require the provider's feature and a model comparison`},
		{"declared facet kinds of an engine that does not list the feature", true, false, protocol.ModelComparison_OBJECTS, nil, []string{retentionKind},
			`.*comparison "topics" names declared facet kinds, which need models\.declared-facets`},
		{"declared facet kinds of a facet comparison", true, true, protocol.ModelComparison_FACETS, []string{"table"}, []string{retentionKind},
			`.*facet comparison "topics" names declared facet kinds, and only an object comparison reads them`},
		{"a kind named twice", true, true, protocol.ModelComparison_OBJECTS, nil, []string{retentionKind, retentionKind}, `.*missing or duplicate comparison name`},
		{"a blank kind", true, true, protocol.ModelComparison_OBJECTS, nil, []string{" "}, `.*missing or duplicate comparison name`},
		{"an object model", true, true, protocol.ModelComparison_OBJECTS, nil, []string{topicKind},
			`.*comparison "topics" names declared facet kind "example\.org/fixture/topic", which no facet comparison of the engine owns`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _ := declaredFacetsExchange(facetTargets(true))
			topics := d.Engines[0].Comparisons[1]
			topics.DeclaredFacetKinds, topics.Placement, topics.OwnerKinds = test.declared, test.placement, test.ownerKinds
			d.Features = slices.DeleteFunc(d.Features, func(feature string) bool {
				return feature == provider.DeclaredFacetsFeature && !test.providerLists
			})
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool {
				return feature == provider.DeclaredFacetsFeature && !test.engineLists
			})

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("an engine without a model comparison", func(t *testing.T) {
		c := qt.New(t)
		d, _ := declaredFacetsExchange(facetTargets(true))
		d.Engines[0].Comparisons = nil

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*declared facets require the provider's feature and a model comparison`)
	})
}
