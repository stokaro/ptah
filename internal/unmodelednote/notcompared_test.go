package unmodelednote_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/unmodelednote"
)

// readPublications is the publications names as a read reports them.
func readPublications(names ...string) schemaext.Objects {
	var objects []schemaext.Object
	for _, name := range names {
		objects = append(objects, must.Must(pgpublication.ObservedObject(pgpublication.Ref(name),
			pgpublication.ObservedPublication{Publish: pgpublication.PublishOperations()})))
	}
	return must.Must(schemaext.NewObjects(objects...))
}

// declaredPublications is the publications names as a desired schema declares
// them.
func declaredPublications(names ...string) schemaext.Objects {
	var objects []schemaext.Object
	for _, name := range names {
		objects = append(objects, must.Must(pgpublication.DesiredObject(pgpublication.Ref(name), pgpublication.DesiredPublication{})))
	}
	return must.Must(schemaext.NewObjects(objects...))
}

// TestNotCompared_HappyPath pins what a comparison leaves out: what the read
// recorded as unmodeled, and the publications the read found while the
// desired schema declares none, after them.
func TestNotCompared_HappyPath(t *testing.T) {
	event := catalog.UnmodeledObject{Kind: "event", Name: "nightly", Reason: notModeled}
	unmanaged := func(name string) catalog.UnmodeledObject {
		return catalog.UnmodeledObject{Kind: "publication", Name: name,
			Reason: "the desired schema declares no publication, and Ptah compares publications only once it declares one"}
	}
	current := &catalog.Database{UnmodeledObjects: []catalog.UnmodeledObject{event}, FeatureObjects: readPublications("dbz", "feed")}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
		want    []catalog.UnmodeledObject
	}{
		{name: "none declared", desired: &schemamodel.Database{}, current: current,
			want: []catalog.UnmodeledObject{event, unmanaged("dbz"), unmanaged("feed")}},
		{name: "one declared", desired: &schemamodel.Database{FeatureObjects: declaredPublications("feed")}, current: current,
			want: []catalog.UnmodeledObject{event}},
		{name: "no desired schema", current: current, want: []catalog.UnmodeledObject{event}},
		{name: "no read", desired: &schemamodel.Database{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(unmodelednote.NotCompared(test.desired, test.current), qt.DeepEquals, test.want)
			c.Assert(current.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{event})
		})
	}
}
