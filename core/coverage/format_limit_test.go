package coverage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
)

// TestVocabulary_WithFormatLimits_HappyPath answers, per format, the owners'
// kinds the format cannot spell, sorted, and nothing for a format no owner
// states a limit for.
func TestVocabulary_WithFormatLimits_HappyPath(t *testing.T) {
	c := qt.New(t)
	vocabulary := must.Must(must.Must(coverage.NewVocabulary("gauge", "meter", "tide")).WithFormatLimits(
		coverage.FormatLimit{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"meter", "gauge"}},
		coverage.FormatLimit{Format: coverage.FormatDBML, Kinds: []coverage.Kind{"gauge"}},
		coverage.FormatLimit{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"tide"}},
	))

	c.Assert(vocabulary.FormatLimits(coverage.FormatHCL), qt.DeepEquals, []coverage.Kind{"gauge", "meter", "tide"})
	c.Assert(vocabulary.FormatLimits(coverage.FormatDBML), qt.DeepEquals, []coverage.Kind{"gauge"})
	c.Assert(vocabulary.FormatLimits(coverage.FormatYAML), qt.IsNil)
	c.Assert(vocabulary.FormatLimits(coverage.FormatSQL), qt.IsNil)
	c.Assert(coverage.Vocabulary{}.FormatLimits(coverage.FormatHCL), qt.IsNil)
}

// TestVocabulary_FormatLimits_ReturnsACopy hands each caller a slice of its
// own, so a caller that changes one does not change what the vocabulary
// answers.
func TestVocabulary_FormatLimits_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	vocabulary := must.Must(must.Must(coverage.NewVocabulary("gauge")).WithFormatLimits(
		coverage.FormatLimit{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"gauge"}}))

	vocabulary.FormatLimits(coverage.FormatHCL)[0] = "changed"

	c.Assert(vocabulary.FormatLimits(coverage.FormatHCL), qt.DeepEquals, []coverage.Kind{"gauge"})
}

// TestVocabulary_FormatLimits_SurviveWithProcess keeps the limits when the
// vocabulary learns its in-process kinds after them, and a vocabulary that
// adds limits leaves the one it was built from as it was.
func TestVocabulary_FormatLimits_SurviveWithProcess(t *testing.T) {
	c := qt.New(t)
	base := must.Must(coverage.NewVocabulary("gauge"))
	limited := must.Must(base.WithFormatLimits(coverage.FormatLimit{Format: coverage.FormatSQL, Kinds: []coverage.Kind{"gauge"}}))

	process := must.Must(limited.WithProcess("replica"))

	c.Assert(process.FormatLimits(coverage.FormatSQL), qt.DeepEquals, []coverage.Kind{"gauge"})
	c.Assert(base.FormatLimits(coverage.FormatSQL), qt.IsNil)
}

// TestVocabulary_WithFormatLimits_FailurePath refuses a format this package
// does not name, a common kind, an in-process kind, a kind the vocabulary
// does not hold, and a kind stated twice for one format.
func TestVocabulary_WithFormatLimits_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		limits []coverage.FormatLimit
		want   string
	}{
		{
			name:   "an unknown format",
			limits: []coverage.FormatLimit{{Format: "prisma", Kinds: []coverage.Kind{"gauge"}}},
			want:   `unknown document format "prisma": valid formats are dbml, hcl, sql, yaml`,
		},
		{
			name:   "a common kind",
			limits: []coverage.FormatLimit{{Format: coverage.FormatHCL, Kinds: []coverage.Kind{coverage.Sequence}}},
			want:   `format hcl cannot limit coverage kind "sequence": it is a common kind, not an owner's`,
		},
		{
			name:   "an in-process kind",
			limits: []coverage.FormatLimit{{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"replica"}}},
			want:   `format hcl cannot limit coverage kind "replica": the vocabulary does not accept it`,
		},
		{
			name:   "a kind the vocabulary does not hold",
			limits: []coverage.FormatLimit{{Format: coverage.FormatDBML, Kinds: []coverage.Kind{"meter"}}},
			want:   `format dbml cannot limit coverage kind "meter": the vocabulary does not accept it`,
		},
		{
			name: "twice for one format",
			limits: []coverage.FormatLimit{
				{Format: coverage.FormatYAML, Kinds: []coverage.Kind{"gauge"}},
				{Format: coverage.FormatYAML, Kinds: []coverage.Kind{"gauge"}},
			},
			want: `format yaml limits coverage kind "gauge" twice`,
		},
	}
	base := must.Must(must.Must(coverage.NewVocabulary("gauge")).WithProcess("replica"))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			vocabulary, err := base.WithFormatLimits(test.limits...)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(vocabulary.Kinds(), qt.DeepEquals, coverage.Vocabulary{}.Kinds())
			c.Assert(vocabulary.FormatLimits(coverage.FormatHCL), qt.IsNil)
		})
	}
}

// TestVocabulary_WithFormatLimits_SameKindInTwoFormats accepts one kind
// stated for two formats: the rule against a repeat holds per format.
func TestVocabulary_WithFormatLimits_SameKindInTwoFormats(t *testing.T) {
	c := qt.New(t)

	vocabulary, err := must.Must(coverage.NewVocabulary("gauge")).WithFormatLimits(
		coverage.FormatLimit{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"gauge"}},
		coverage.FormatLimit{Format: coverage.FormatDBML, Kinds: []coverage.Kind{"gauge"}},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(vocabulary.FormatLimits(coverage.FormatHCL), qt.DeepEquals, []coverage.Kind{"gauge"})
	c.Assert(vocabulary.FormatLimits(coverage.FormatDBML), qt.DeepEquals, []coverage.Kind{"gauge"})
}
