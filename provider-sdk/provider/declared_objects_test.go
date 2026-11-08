package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// declaredObjectsExchange is an object comparison "retention" that reads the
// declared objects of the engine's "topics" comparison, which owns topicKind, for a target whose
// features are targetFeatures. The request names one declared topic for each
// name given.
func declaredObjectsExchange(targetFeatures []string, names ...string) (*protocol.Description, *protocol.CompareModelsRequest) {
	d, request, _ := comparisonExchange()
	engine := d.Engines[0]
	retention := engine.Comparisons[0]
	retention.Placement, retention.OwnerKinds, retention.DeclaredKinds = protocol.ModelComparison_OBJECTS, nil, []string{topicKind}
	kinds := map[protocol.ValueInput_Representation]string{protocol.ValueInput_DESIRED: topicKind, protocol.ValueInput_OBSERVED: topicKind,
		protocol.ValueInput_CHANGE: topicKind + "-change"}
	var topicModels []*protocol.ValueModel
	for _, model := range engine.ValueModels {
		topic := proto.CloneOf(model)
		topic.Kind = kinds[model.Representation]
		topicModels = append(topicModels, topic)
	}
	engine.ValueModels = append(engine.ValueModels, topicModels...)
	engine.Comparisons = append(engine.Comparisons, &protocol.ModelComparison{Id: "topics", Placement: protocol.ModelComparison_OBJECTS,
		Kinds: []string{topicKind}, ChangeKinds: []string{topicKind + "-change"}})
	d.Features = append(d.Features, provider.DeclaredObjectsFeature)
	engine.Features = append(engine.Features, provider.DeclaredObjectsFeature)
	request.TargetFeatures = targetFeatures
	for _, name := range names {
		request.DeclaredObjects = append(request.DeclaredObjects, &protocol.ComparisonIdentity{Kind: topicKind,
			Name: &protocol.ComparisonPart{Source: name, Normalized: name}, Schema: &protocol.ComparisonPart{Source: "app", Normalized: "app"}})
	}
	return d, request
}

// objectTargets are the target features of the exchange's request: the
// comparison's own, and declared objects as well for a listed target.
func objectTargets(listed bool) []string {
	return map[bool][]string{false: {"retention"}, true: {"retention", provider.DeclaredObjectsFeature}}[listed]
}

// withoutDeclaredObjects lists every two-way feature but declared objects.
func withoutDeclaredObjects() []string {
	return slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool { return feature == provider.DeclaredObjectsFeature })
}

func TestDeclaredObjects_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		listed bool
		names  []string
	}{
		{"declared objects of a kind the comparison reads", true, []string{"events", "orders"}},
		{"no declared object", true, nil},
		{"no declared object for a target that does not list the feature", false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := declaredObjectsExchange(objectTargets(test.listed), test.names...)

			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateComparisonRequest(d, request), qt.IsNil)
		})
	}
}

// A host that does not list the feature receives no declared kinds, and the
// description it receives is still valid.
func TestDeclaredObjectsNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, _ := declaredObjectsExchange(objectTargets(true))

	negotiated := provider.NegotiatedDescription(d, withoutDeclaredObjects())

	c.Assert(negotiated.GetEngines()[0].GetComparisons()[0].GetDeclaredKinds(), qt.HasLen, 0)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.DeclaredObjectsFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(provider.NegotiatedDescription(d, provider.TwoWayFeatures()).GetEngines()[0].GetComparisons()[0].GetDeclaredKinds(), qt.DeepEquals, []string{topicKind})
}

// A request names declared objects only where both sides negotiated the
// feature, each once and of a kind the comparison reads.
func TestDeclaredObjects_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		listed  bool
		kind    string
		objects []string
		want    string
	}{
		{"a target that does not list the feature", false, topicKind, []string{"events"}, `.*declared objects need models\.declared-objects on both sides`},
		{"a kind the comparison does not read", true, "example.org/fixture/retention", []string{"events"},
			`.*a declared object of kind "example\.org/fixture/retention" is not one comparison "retention" reads`},
		{"a relation's kind", true, "table", []string{"events"}, `.*a declared object of kind "table" is not one comparison "retention" reads`},
		{"an object named twice", true, topicKind, []string{"events", "events"}, `.*duplicate comparison identity`},
		{"an object without a name", true, topicKind, []string{""}, `.*comparison identity requires a kind and name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := declaredObjectsExchange(objectTargets(test.listed), test.objects...)
			for _, object := range request.DeclaredObjects {
				object.Kind = test.kind
			}

			err := provider.ValidateComparisonRequest(d, request)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that does not list the feature sees an engine without it, and a
// target without it, so a declared object it is sent is refused.
func TestDeclaredObjectsNegotiation_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := declaredObjectsExchange(objectTargets(false), "events")

	err := provider.ValidateComparisonRequest(provider.NegotiatedDescription(d, withoutDeclaredObjects()), request)

	c.Assert(err, qt.ErrorMatches, `.*declared objects need models\.declared-objects on both sides`)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}

// The engine's feature needs the provider's and a model comparison, and a
// comparison names only kinds another object comparison of the engine owns.
func TestDeclaredObjectsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name          string
		providerLists bool
		engineLists   bool
		placement     protocol.ModelComparison_Placement
		ownerKinds    []string
		declared      []string
		want          string
	}{
		{"an engine feature without the provider's", false, true, protocol.ModelComparison_OBJECTS, nil, []string{topicKind},
			`.*declared objects require the provider's feature and a model comparison`},
		{"declared kinds of an engine that does not list the feature", true, false, protocol.ModelComparison_OBJECTS, nil, []string{topicKind},
			`.*comparison "retention" names declared kinds, which need models\.declared-objects`},
		{"declared kinds of a facet comparison", true, true, protocol.ModelComparison_FACETS, []string{"table"}, []string{topicKind},
			`.*facet comparison "retention" names declared kinds, and a facet comparison receives no object context`},
		{"a kind named twice", true, true, protocol.ModelComparison_OBJECTS, nil, []string{topicKind, topicKind}, `.*missing or duplicate comparison name`},
		{"a blank kind", true, true, protocol.ModelComparison_OBJECTS, nil, []string{" "}, `.*missing or duplicate comparison name`},
		{"the comparison's own kind", true, true, protocol.ModelComparison_OBJECTS, nil, []string{"example.org/fixture/retention"},
			`.*comparison "retention" names its own model "example\.org/fixture/retention" among its declared kinds`},
		{"a kind no comparison owns", true, true, protocol.ModelComparison_OBJECTS, nil, []string{"example.org/fixture/stream"},
			`.*comparison "retention" names declared kind "example\.org/fixture/stream", which no object comparison of the engine owns`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _ := declaredObjectsExchange(objectTargets(true))
			retention := d.Engines[0].Comparisons[0]
			retention.DeclaredKinds, retention.Placement, retention.OwnerKinds = test.declared, test.placement, test.ownerKinds
			d.Features = slices.DeleteFunc(d.Features, func(feature string) bool {
				return feature == provider.DeclaredObjectsFeature && !test.providerLists
			})
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool {
				return feature == provider.DeclaredObjectsFeature && !test.engineLists
			})

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("an engine without a model comparison", func(t *testing.T) {
		c := qt.New(t)
		d, _ := declaredObjectsExchange(objectTargets(true))
		d.Engines[0].Comparisons = nil

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*declared objects require the provider's feature and a model comparison`)
	})
}
