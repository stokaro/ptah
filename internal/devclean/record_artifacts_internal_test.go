package devclean

// White-box testing required: what a claim records of the database-scoped
// objects is set only by Claim, which needs a live server, and a server that
// cannot list them is not one CI runs. These hand the recording a writer that
// answers both ways and ask the baseline what its resets do with publications.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/devclean/replay"
)

// listingWriter lists artifacts and says whether its server answers the list.
// The embedded writer is nil: the recording calls nothing else on it.
type listingWriter struct {
	catalog.SchemaWriter
	artifacts []dbreset.Object
	reads     bool
}

func (w listingWriter) DatabaseScopedArtifacts(context.Context) ([]dbreset.Object, error) {
	return w.artifacts, nil
}

func (w listingWriter) ReadsDatabaseScopedArtifacts(context.Context) (bool, error) {
	return w.reads, nil
}

func TestBaseline_RecordArtifacts_HappyPath(t *testing.T) {
	held := []dbreset.Object{
		{Kind: "event trigger", Name: "watch_ddl"},
		{Kind: "publication", Name: "supabase_realtime"},
	}
	tests := []struct {
		name   string
		writer catalog.SchemaWriter
		want   replay.PublicationScope
	}{
		{
			name:   "a server that answers the list",
			writer: listingWriter{artifacts: held, reads: true},
			want:   replay.PublicationScope{Dropped: true, Kept: []string{"supabase_realtime"}},
		},
		{
			name:   "a server that cannot answer it",
			writer: listingWriter{reads: false},
			want:   replay.PublicationScope{},
		},
		{
			name:   "a writer that lists nothing",
			writer: &recordingWriter{},
			want:   replay.PublicationScope{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var baseline Baseline

			err := baseline.recordArtifacts(c.Context(), test.writer)

			c.Assert(err, qt.IsNil)
			c.Assert(baseline.Publications(), qt.DeepEquals, test.want)
		})
	}
}
