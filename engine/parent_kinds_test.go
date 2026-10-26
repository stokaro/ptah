package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// The runtime is the vocabulary a rollback asks which captured knowledge to
// keep local to a recreated table.
var _ featureplan.ParentVocabulary = (*engine.Runtime)(nil)

func TestParentKinds_HappyPath(t *testing.T) {
	service := planningFunc(plannedParents)
	split := parentPlanningProvider(service)
	split.Planning = []engine.Planning{
		{Target: "custom", ParentKinds: []schemaext.Kind{conversionSecond}, Service: service},
		{Target: "custom", ParentKinds: []schemaext.Kind{conversionFirst}, Service: service},
	}
	unplanned := parentPlanningProvider(service)
	unplanned.Planning = nil
	both := []schemaext.Kind{conversionFirst, conversionSecond}
	for _, test := range []struct {
		name     string
		provider engine.Provider
		target   string
		want     []schemaext.Kind
	}{
		{name: "one service", provider: parentPlanningProvider(service), target: "custom", want: both},
		{name: "services joined in kind order", provider: split, target: "custom", want: both},
		{name: "an alias in another spelling", provider: parentPlanningProvider(service), target: " ALTERNATE ", want: both},
		{name: "a target without parent planning", provider: unplanned, target: "custom", want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			kinds, err := mustRuntime(c, test.provider).ParentKinds(test.target)
			c.Assert(err, qt.IsNil)
			c.Assert(kinds, qt.DeepEquals, test.want)
		})
	}
}

func TestParentKinds_ReturnsAnIndependentList(t *testing.T) {
	c := qt.New(t)
	provider := parentPlanningProvider(planningFunc(plannedParents))
	runtime := mustRuntime(c, provider)
	provider.Planning[0].ParentKinds[0] = "example.org/replaced"
	kinds, err := runtime.ParentKinds("custom")
	c.Assert(err, qt.IsNil)
	kinds[0] = "example.org/changed"
	again, err := runtime.ParentKinds("custom")
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
}

func TestParentKinds_NilRuntimeKnowsNoKinds(t *testing.T) {
	c := qt.New(t)
	var runtime *engine.Runtime
	kinds, err := runtime.ParentKinds("custom")
	c.Assert(err, qt.IsNil)
	c.Assert(kinds, qt.IsNil)
}

func TestParentKinds_FailurePath(t *testing.T) {
	for _, target := range []string{"unknown", "", "two words"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, parentPlanningProvider(planningFunc(plannedParents)))
			kinds, err := runtime.ParentKinds(target)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(kinds, qt.IsNil)
		})
	}
}
