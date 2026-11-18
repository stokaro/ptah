package publicationcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationcompare"
)

func declared(names ...string) schemaext.ObjectState {
	objects := make([]schemaext.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, must.Must(pgpublication.DesiredObject(pgpublication.Ref(name), pgpublication.DesiredPublication{})))
	}
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(pgpublication.CompleteCoverage(schemaext.Desired))}
}

func held(knowledge schemaext.Knowledge, names ...string) schemaext.ObjectState {
	objects := make([]schemaext.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, must.Must(pgpublication.ObservedObject(pgpublication.Ref(name), pgpublication.ObservedPublication{
			Publish: pgpublication.PublishOperations()})))
	}
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(pgpublication.Coverage(schemaext.Observed, knowledge, nil))}
}

// TestCompareObjects_HappyPath pins which publications a comparison creates,
// drops and leaves undecided: one the schema leaves out is dropped only when
// the schema declares another, and one the read could not establish is
// neither created nor dropped.
func TestCompareObjects_HappyPath(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	partial := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the read covers some schemas"}
	tests := []struct {
		name      string
		desired   schemaext.ObjectState
		current   schemaext.ObjectState
		created   int
		dropped   int
		undecided int
	}{
		{name: "one created, one dropped", desired: declared("feed"), current: held(complete, "old"), created: 1, dropped: 1},
		{name: "none declared drops none", desired: declared(), current: held(complete, "dbz_publication", "supabase_realtime")},
		{name: "a partial read decides nothing", desired: declared("feed"), current: held(partial), undecided: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := publicationcompare.Service{}.CompareObjects(c.Context(), schemaext.ObjectComparisonRequest{Target: platform.Postgres,
				Kinds: []schemaext.Kind{pgpublication.PublicationKind}, Desired: test.desired, Current: test.current})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts[0].Added, qt.Equals, test.created)
			c.Assert(result.Counts[0].Removed, qt.Equals, test.dropped)
			c.Assert(result.Changes, qt.HasLen, test.created+test.dropped)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
		})
	}
}
