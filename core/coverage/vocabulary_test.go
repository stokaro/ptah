package coverage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
)

// TestNewVocabulary_HappyPath accepts an owner's kind beside the common ones,
// and a header decoded with the vocabulary keeps the owner's record in the
// common set.
func TestNewVocabulary_HappyPath(t *testing.T) {
	c := qt.New(t)
	vocabulary := must.Must(coverage.NewVocabulary("gauge"))

	c.Assert(vocabulary.Kinds(), qt.Contains, coverage.Kind("gauge"))
	c.Assert(vocabulary.Kinds(), qt.Contains, coverage.Sequence)
	set, err := coverage.DecodeHeader("-- ptah:not-described gauge \"meter\"\nCREATE TABLE t (id INT);\n", vocabulary, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(set.Directives(), qt.DeepEquals, []string{`ptah:not-described gauge "meter"`})
	c.Assert(vocabulary.Validate(set), qt.IsNil)
}

// TestNewVocabulary_FailurePath refuses a kind that is not a lower-case name,
// a common kind, and one registered twice.
func TestNewVocabulary_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		kinds []coverage.Kind
		want  string
	}{
		{name: "upper case", kinds: []coverage.Kind{"Gauge"}, want: `coverage kind "Gauge" is not a lower-case name`},
		{name: "empty", kinds: []coverage.Kind{""}, want: `coverage kind "" is not a lower-case name`},
		{name: "a common kind", kinds: []coverage.Kind{coverage.Role}, want: `coverage kind "role" is a common kind and cannot be an owner's`},
		{name: "twice", kinds: []coverage.Kind{"gauge", "gauge"}, want: `coverage kind "gauge" is registered twice`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			vocabulary, err := coverage.NewVocabulary(test.kinds...)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(vocabulary.Kinds(), qt.DeepEquals, coverage.Vocabulary{}.Kinds())
		})
	}
}

// TestVocabulary_RefusesAnUnregisteredKind refuses an owner's kind in the zero
// vocabulary, by name, both when parsed and when a header names it.
func TestVocabulary_RefusesAnUnregisteredKind(t *testing.T) {
	c := qt.New(t)

	kind, err := coverage.Vocabulary{}.ParseKind("gauge")
	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "gauge": valid kinds are .*`)
	c.Assert(kind, qt.Equals, coverage.Kind(""))

	set, err := coverage.DecodeHeader("-- ptah:not-described gauge\n", coverage.Vocabulary{}, nil)
	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "gauge": valid kinds are .*`)
	c.Assert(set.IsZero(), qt.IsTrue)
}
