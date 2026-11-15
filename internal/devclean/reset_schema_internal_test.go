package devclean

// White-box testing required: what a reset of a pinned schema keeps is set
// only by Claim, which needs a live server, and the second half of the reset,
// the drop of the schemas a run added beside the pinned one, is observable only
// on a server whose claim found other schemas. These ask each baseline the
// claim can record against a writer that records what the reset asked of it.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/pgsnapshot"
)

// recordingWriter records each reset call, with the schemas the baseline
// kept. The embedded writer is nil: the reset of a pinned schema calls
// nothing else on it.
type recordingWriter struct {
	catalog.SchemaWriter
	calls []string
	kept  [][]string
	err   error
}

func (w *recordingWriter) DropAllTablesKeeping(_ context.Context, kept dbreset.Kept) error {
	w.calls = append(w.calls, "empty the pinned schema")
	w.kept = append(w.kept, kept.Schemas)
	return w.err
}

func (w *recordingWriter) DropSchemasAddedSince(_ context.Context, kept dbreset.Kept) error {
	w.calls = append(w.calls, "drop the schemas added since")
	w.kept = append(w.kept, kept.Schemas)
	return nil
}

func TestBaseline_ResetSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		baseline  Baseline
		wantCalls []string
		wantKept  [][]string
	}{
		{
			name:      "a claim that found other schemas drops the ones added since",
			baseline:  Baseline{schemas: []string{"kept"}, schemasRecorded: true},
			wantCalls: []string{"empty the pinned schema", "drop the schemas added since"},
			wantKept:  [][]string{{"kept"}, {"kept"}},
		},
		{
			name:      "a claim that found none drops every other schema",
			baseline:  Baseline{schemasRecorded: true},
			wantCalls: []string{"empty the pinned schema", "drop the schemas added since"},
			wantKept:  [][]string{nil, nil},
		},
		{
			name:      "a claim that did not list the schemas drops none",
			baseline:  Baseline{},
			wantCalls: []string{"empty the pinned schema"},
			wantKept:  [][]string{nil},
		},
		{
			name:      "a database that returns to a starting point is restored whole",
			baseline:  Baseline{schemas: []string{"kept"}, schemasRecorded: true, startingPoint: &pgsnapshot.Snapshot{}},
			wantCalls: []string{"empty the pinned schema"},
			wantKept:  [][]string{{"kept"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			writer := &recordingWriter{}

			err := test.baseline.resetSchema(c.Context(), writer)

			c.Assert(err, qt.IsNil)
			c.Assert(writer.calls, qt.DeepEquals, test.wantCalls)
			c.Assert(writer.kept, qt.DeepEquals, test.wantKept)
		})
	}
}

// TestBaseline_ResetSchema_FailurePath is a pinned schema the writer could
// not empty: the reset stops there and drops no other schema.
func TestBaseline_ResetSchema_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("cannot drop table notes because other objects depend on it")
	writer := &recordingWriter{err: refused}

	err := Baseline{schemas: []string{"kept"}, schemasRecorded: true}.resetSchema(c.Context(), writer)

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(writer.calls, qt.DeepEquals, []string{"empty the pinned schema"})
}

func TestBaseline_SchemasLeftAsFound(t *testing.T) {
	tests := []struct {
		name     string
		baseline Baseline
		want     []string
	}{
		{name: "a pinned schema", baseline: Baseline{schemas: []string{"auth", "kept"}, schemasRecorded: true}, want: []string{"auth", "kept"}},
		{name: "a realm cleanup", baseline: Baseline{realm: true}},
		{
			name:     "a database that returns to a starting point",
			baseline: Baseline{schemas: []string{"auth"}, schemasRecorded: true, startingPoint: &pgsnapshot.Snapshot{}},
		},
		{name: "the zero baseline", baseline: Baseline{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.baseline.SchemasLeftAsFound(), qt.DeepEquals, test.want)
		})
	}
}
