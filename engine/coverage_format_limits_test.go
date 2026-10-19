package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/engine"
)

// TestRuntime_CoverageFormatLimits_HappyPath hands a provider's statement
// that a format cannot spell its kinds to the runtime's vocabulary, which
// every document loader of the run reads. A runtime of a provider that
// states nothing knows no limit.
func TestRuntime_CoverageFormatLimits_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{
		ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed", "pulse"},
		CoverageFormatLimits: []coverage.FormatLimit{
			{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"pulse", "feed"}},
			{Format: coverage.FormatDBML, Kinds: []coverage.Kind{"feed"}},
		},
	}))
	silent := must.Must(engine.New(engine.Provider{ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed", "pulse"}}))

	c.Assert(runtime.CoverageVocabulary().FormatLimits(coverage.FormatHCL), qt.DeepEquals, []coverage.Kind{"feed", "pulse"})
	c.Assert(runtime.CoverageVocabulary().FormatLimits(coverage.FormatDBML), qt.DeepEquals, []coverage.Kind{"feed"})
	c.Assert(runtime.CoverageVocabulary().FormatLimits(coverage.FormatYAML), qt.IsNil)
	c.Assert(silent.CoverageVocabulary().FormatLimits(coverage.FormatHCL), qt.IsNil)
}

// TestRuntime_CoverageFormatLimits_FailurePath refuses a provider's limit on
// a kind it does not register itself, another provider's kind included, and
// a limit the vocabulary refuses, such as one for an unknown format.
func TestRuntime_CoverageFormatLimits_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		providers []engine.Provider
		want      string
	}{
		{
			name: "a kind the provider does not register",
			providers: []engine.Provider{{ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed"},
				CoverageFormatLimits: []coverage.FormatLimit{{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"pulse"}}}}},
			want: `invalid provider registration: provider "example.org/feeds" states a hcl format limit on coverage kind "pulse", which it does not register`,
		},
		{
			name: "another provider's kind",
			providers: []engine.Provider{
				{ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed"}},
				{ID: "example.org/pulses", CoverageKinds: []coverage.Kind{"pulse"},
					CoverageFormatLimits: []coverage.FormatLimit{{Format: coverage.FormatDBML, Kinds: []coverage.Kind{"feed"}}}},
			},
			want: `invalid provider registration: provider "example.org/pulses" states a dbml format limit on coverage kind "feed", which it does not register`,
		},
		{
			name: "a common kind",
			providers: []engine.Provider{{ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed"},
				CoverageFormatLimits: []coverage.FormatLimit{{Format: coverage.FormatHCL, Kinds: []coverage.Kind{coverage.Sequence}}}}},
			want: `invalid provider registration: provider "example.org/feeds" states a hcl format limit on coverage kind "sequence", which it does not register`,
		},
		{
			name: "an unknown format",
			providers: []engine.Provider{{ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed"},
				CoverageFormatLimits: []coverage.FormatLimit{{Format: "prisma", Kinds: []coverage.Kind{"feed"}}}}},
			want: `invalid provider registration: unknown document format "prisma": valid formats are dbml, hcl, sql, yaml`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.providers...)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(runtime, qt.IsNil)
		})
	}
}
