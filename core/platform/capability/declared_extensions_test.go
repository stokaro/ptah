package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// TestWithDeclaredExtensions is the offline half of the hypertable capability,
// and it exists because a script that installs an extension and then refuses to
// use it is worse than either answer alone.
//
// `ptah schema render --dialect postgres` over a document declaring the
// timescaledb extension and a hypertable emitted
// `CREATE EXTENSION "timescaledb";` followed by
// `-- POSTGRES: hypertable public.readings is not supported by this target; skipped.`
// The render has no connection to ask, and the declaration in front of it is
// the evidence (stokaro/ptah#1026).
func TestWithDeclaredExtensions(t *testing.T) {
	tests := []struct {
		name       string
		base       capability.Capabilities
		extensions []string
		want       bool
	}{
		{
			name:       "the extension is declared",
			base:       capability.Postgres17(),
			extensions: []string{"timescaledb"},
			want:       true,
		},
		{
			name:       "spelled the way a catalog might",
			base:       capability.Postgres17(),
			extensions: []string{"TimescaleDB"},
			want:       true,
		},
		{
			name:       "another extension",
			base:       capability.Postgres17(),
			extensions: []string{"pgcrypto", "citext"},
			want:       false,
		},
		{
			name:       "no extension at all",
			base:       capability.Postgres17(),
			extensions: nil,
			want:       false,
		},
		{
			// It only ever turns the key on: a connection that already reported
			// the extension keeps its answer.
			name:       "already on",
			base:       capability.Postgres17().With(capability.Hypertables, true),
			extensions: nil,
			want:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			refined := capability.WithDeclaredExtensions(test.base, test.extensions)

			c.Assert(refined.Has(capability.Hypertables), qt.Equals, test.want)
			// Nothing else moves: the rule names one key, and a refinement that
			// rebuilt the set would be a preset change nobody reviewed.
			c.Assert(refined.Has(capability.Sequences), qt.Equals, test.base.Has(capability.Sequences))
			c.Assert(refined, qt.HasLen, len(test.base))
		})
	}
}
