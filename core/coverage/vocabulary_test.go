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

// TestVocabulary_WithProcess_HappyPath knows an in-process kind beside the
// kinds documents name, and still refuses it by name, since no document
// names one. The vocabulary it was made from is unchanged.
func TestVocabulary_WithProcess_HappyPath(t *testing.T) {
	c := qt.New(t)
	named := must.Must(coverage.NewVocabulary("gauge"))

	vocabulary, err := named.WithProcess("shadow")

	c.Assert(err, qt.IsNil)
	c.Assert(vocabulary.InProcess("shadow"), qt.IsTrue)
	c.Assert(vocabulary.InProcess("gauge"), qt.IsFalse)
	c.Assert(vocabulary.Kinds(), qt.DeepEquals, named.Kinds())
	kind, err := vocabulary.ParseKind("shadow")
	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "shadow": valid kinds are .*`)
	c.Assert(kind, qt.Equals, coverage.Kind(""))
	c.Assert(named.InProcess("shadow"), qt.IsFalse)
	c.Assert(coverage.Vocabulary{}.InProcess("shadow"), qt.IsFalse)
}

// TestVocabulary_WithProcess_FailurePath refuses an in-process kind that is
// not a lower-case name, a kind documents name, common or owned, and one
// registered twice.
func TestVocabulary_WithProcess_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		kinds []coverage.Kind
		want  string
	}{
		{name: "upper case", kinds: []coverage.Kind{"Shadow"}, want: `in-process coverage kind "Shadow" is not a lower-case name`},
		{name: "empty", kinds: []coverage.Kind{""}, want: `in-process coverage kind "" is not a lower-case name`},
		{name: "a common kind", kinds: []coverage.Kind{coverage.Role}, want: `coverage kind "role" is a kind documents name and cannot be in-process`},
		{name: "an owned kind", kinds: []coverage.Kind{"gauge"}, want: `coverage kind "gauge" is a kind documents name and cannot be in-process`},
		{name: "twice", kinds: []coverage.Kind{"shadow", "shadow"}, want: `in-process coverage kind "shadow" is registered twice`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			vocabulary, err := must.Must(coverage.NewVocabulary("gauge")).WithProcess(test.kinds...)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(vocabulary.Kinds(), qt.DeepEquals, coverage.Vocabulary{}.Kinds())
			c.Assert(vocabulary.InProcess("shadow"), qt.IsFalse)
		})
	}
}

// TestVocabulary_WithProcess_KeepsEachVocabularysKinds keeps the in-process
// kinds of two vocabularies made from one apart: adding to one leaves the
// other, and the one both were made from, as they were.
func TestVocabulary_WithProcess_KeepsEachVocabularysKinds(t *testing.T) {
	c := qt.New(t)
	base := must.Must(coverage.Vocabulary{}.WithProcess("a", "b", "c"))

	first := must.Must(base.WithProcess("d"))
	second := must.Must(base.WithProcess("e"))

	c.Assert(first.InProcess("d"), qt.IsTrue)
	c.Assert(first.InProcess("e"), qt.IsFalse)
	c.Assert(second.InProcess("e"), qt.IsTrue)
	c.Assert(second.InProcess("d"), qt.IsFalse)
	c.Assert(base.InProcess("d") || base.InProcess("e"), qt.IsFalse)
}
