package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/atlasfilter"
)

func topicNames(objects schemaext.Objects) []string {
	var names []string
	for _, ref := range objects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbtopic.Kind) }).Refs() {
		names = append(names, ydbtopic.Display(ref.Schema.Source, ref.Name.Source))
	}
	return names
}

// TestScope_Topics selects a YDB topic on its own name, in the directory
// that holds it, on both sides of a comparison: an include, an exclusion by
// name and an excluded directory each keep the same topics in the
// declaration and in the read.
func TestScope_Topics(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "include", scope: atlasfilter.Scope{Include: []string{"events[type=topic]"}}, want: []string{"events"}},
		{name: "include in a directory", scope: atlasfilter.Scope{Include: []string{"ext.queue[type=topic]"}}, want: []string{"ext/queue"}},
		{name: "exclude by name", scope: atlasfilter.Scope{Exclude: []string{"events[type=topic]"}}, want: []string{"ext/queue"}},
		{name: "schema", scope: atlasfilter.Scope{Schemas: []string{"ext"}}, want: []string{"ext/queue"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{}), ydbtopic.DesiredObject("ext", "queue", "", ydbtopic.Spec{})))}
			held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbtopic.ObservedObject("", "events", ydbtopic.Spec{}), ydbtopic.ObservedObject("ext", "queue", ydbtopic.Spec{})))}

			generated, err := atlasfilter.ScopeGenerated(declared, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(held, test.scope)
			c.Assert(err, qt.IsNil)

			c.Assert(topicNames(generated.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(topicNames(live.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(declared.FeatureObjects.Len(), qt.Equals, 2)
			c.Assert(held.FeatureObjects.Len(), qt.Equals, 2)
		})
	}
}

// TestExclude_Topics drops an excluded topic from the declaration as well as
// from the read, so a topic the database holds is neither dropped nor created
// again.
func TestExclude_Topics(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{}), ydbtopic.DesiredObject("ext", "queue", "", ydbtopic.Spec{})))}
	held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbtopic.ObservedObject("", "events", ydbtopic.Spec{}), ydbtopic.ObservedObject("ext", "queue", ydbtopic.Spec{})))}

	generated, err := atlasfilter.ExcludeGenerated(declared, []string{"ext.*[type=topic]"})
	c.Assert(err, qt.IsNil)
	live, err := atlasfilter.ExcludeDatabase(held, []string{"ext.*[type=topic]"})
	c.Assert(err, qt.IsNil)

	c.Assert(topicNames(generated.FeatureObjects), qt.DeepEquals, []string{"events"})
	c.Assert(topicNames(live.FeatureObjects), qt.DeepEquals, []string{"events"})
}
