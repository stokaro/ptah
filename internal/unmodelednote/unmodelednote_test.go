package unmodelednote_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/unmodelednote"
)

const notModeled = "nothing compares it"

func named(names ...string) []catalog.UnmodeledObject {
	objects := make([]catalog.UnmodeledObject, 0, len(names))
	for _, name := range names {
		objects = append(objects, catalog.UnmodeledObject{Kind: "event", Name: name, Reason: notModeled})
	}
	return objects
}

// wholeKind is what a read records when it could not list the kind.
var wholeKind = catalog.UnmodeledObject{Kind: "event", Reason: "the account cannot list them"}

func TestSummary(t *testing.T) {
	tests := []struct {
		name    string
		objects []catalog.UnmodeledObject
		want    string
	}{
		{name: "nothing left out"},
		{name: "one object", objects: named("e_a"), want: "1 event was not compared"},
		{name: "several objects", objects: named("e_a", "e_b"), want: "2 events were not compared"},
		{name: "a kind the read could not list", objects: []catalog.UnmodeledObject{wholeKind}, want: "events were not compared"},
		{
			name:    "a whole kind is the larger statement",
			objects: append(named("e_a"), wholeKind),
			want:    "events were not compared",
		},
		{
			name: "two kinds",
			objects: append(named("e_a", "e_b"),
				catalog.UnmodeledObject{Kind: "change_stream", Name: "s", Reason: notModeled}),
			want: "2 events and 1 change_stream were not compared",
		},
		{
			name: "two kinds, one object of each, counts as more than one",
			objects: append(named("e_a"),
				catalog.UnmodeledObject{Kind: "change_stream", Name: "s", Reason: notModeled}),
			want: "1 event and 1 change_stream were not compared",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(unmodelednote.Summary(test.objects), qt.Equals, test.want)
		})
	}
}

func TestEntries(t *testing.T) {
	tests := []struct {
		name   string
		object catalog.UnmodeledObject
		want   string
	}{
		{name: "an object of the database read", object: catalog.UnmodeledObject{Kind: "event", Name: "e_a"}, want: `event "e_a"`},
		{name: "an object of a server read", object: catalog.UnmodeledObject{Kind: "event", Schema: "r1", Name: "e_a"}, want: `event "r1.e_a"`},
		{name: "a kind in the database read", object: catalog.UnmodeledObject{Kind: "event"}, want: "events"},
		{name: "a kind in one database of a server read", object: catalog.UnmodeledObject{Kind: "event", Schema: "r1"}, want: `events in "r1"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(unmodelednote.Entries([]catalog.UnmodeledObject{test.object}), qt.DeepEquals,
				[]unmodelednote.Entry{{Subject: test.want}})
		})
	}
}

// TestReport_WithoutARun is a library caller, which gets each object as a line.
func TestReport_WithoutARun(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	unmodelednote.Report(c.Context(), &out, "--from", &catalog.Database{
		UnmodeledObjects: append(named("e_a"), wholeKind),
	})

	c.Assert(out.String(), qt.Equals, `warning: event "e_a" in --from is left out: nothing compares it
warning: events in --from are left out: the account cannot list them
`)
}

// TestReport_WithinARun counts several objects as one warning naming each, and
// one object raised twice as one, since a run that plans and then applies computes
// its plan twice.
func TestReport_WithinARun(t *testing.T) {
	tests := []struct {
		name    string
		objects []catalog.UnmodeledObject
		want    string
	}{
		{
			name:    "one object, raised twice",
			objects: named("e_a"),
			want:    "warning: event \"e_a\" in the database is left out: nothing compares it\n",
		},
		{
			name:    "several objects",
			objects: named("e_a", "e_b"),
			want: "warning: 2 objects were left out: event \"e_a\" in the database, event \"e_b\" in the database (nothing compares it); " +
				string(rundiag.ListedAtDebugLevel) + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer
			report := rundiag.New(rundiag.Options{Stream: &out})
			ctx := rundiag.With(c.Context(), report)
			schema := &catalog.Database{UnmodeledObjects: test.objects}

			unmodelednote.Report(ctx, &out, "the database", schema)
			unmodelednote.Report(ctx, &out, "the database", schema)
			report.Flush()

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReport_SaysNothingWhenThereIsNothingToSay covers every reason for silence.
func TestReport_SaysNothingWhenThereIsNothingToSay(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	unmodelednote.Report(c.Context(), &out, "the database", nil)
	unmodelednote.Report(c.Context(), &out, "the database", &catalog.Database{})
	unmodelednote.Report(c.Context(), nil, "the database", &catalog.Database{UnmodeledObjects: named("e_a")})

	c.Assert(out.String(), qt.Equals, "")
}
