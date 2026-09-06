package embeddigest_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embeddigest"
)

// TestOf_ComponentBoundariesAreNotGuessable is the property the whole encoding
// exists for.
//
// Every joiner has this failure: pick any separator, and two lists whose values
// contain it collapse to the same bytes. The rows below are the same characters
// in the same order, split differently, and each pair must digest differently.
func TestOf_ComponentBoundariesAreNotGuessable(t *testing.T) {
	tests := []struct {
		name  string
		left  []string
		right []string
	}{
		{name: "a dot", left: []string{"a", "b.c"}, right: []string{"a.b", "c"}},
		{name: "a colon", left: []string{"a", "b:c"}, right: []string{"a:b", "c"}},
		{name: "a newline", left: []string{"a", "b\nc"}, right: []string{"a\nb", "c"}},
		{name: "a NUL", left: []string{"a", "b\x00c"}, right: []string{"a\x00b", "c"}},
		{name: "an empty component", left: []string{"", "ab"}, right: []string{"a", "b"}},
		{name: "the count", left: []string{"ab"}, right: []string{"a", "b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embeddigest.Of(test.left...), qt.Not(qt.Equals), embeddigest.Of(test.right...))
		})
	}
}

// TestOf_TheSameListDigestsTheSame is the other half: a content address that
// moved on its own would make every generation look new on every run.
func TestOf_TheSameListDigestsTheSame(t *testing.T) {
	c := qt.New(t)

	first := embeddigest.Of("source", "public.articles", "", "title")
	second := embeddigest.Of("source", "public.articles", "", "title")

	c.Assert(first, qt.Equals, second)
	c.Assert(first, qt.HasLen, 64)
	c.Assert(strings.Trim(first, "0123456789abcdef"), qt.Equals, "")
}

// TestOf_OrderIsPartOfTheContent keeps a reordered list from reading as the
// same one. Title-then-body is not body-then-title, and the two produce
// different text and different vectors.
func TestOf_OrderIsPartOfTheContent(t *testing.T) {
	c := qt.New(t)

	c.Assert(embeddigest.Of("title", "body"), qt.Not(qt.Equals), embeddigest.Of("body", "title"))
}

// TestOf_NoComponentsIsStillADigest pins the empty case rather than leaving it
// to whatever the builder happens to do.
func TestOf_NoComponentsIsStillADigest(t *testing.T) {
	c := qt.New(t)

	c.Assert(embeddigest.Of(), qt.HasLen, 64)
	c.Assert(embeddigest.Of(), qt.Not(qt.Equals), embeddigest.Of(""))
}

// TestShort_IsAPrefixAndNeverPanics covers the name a person reads.
func TestShort_IsAPrefixAndNeverPanics(t *testing.T) {
	tests := []struct {
		name   string
		digest string
		want   string
	}{
		{name: "a full digest", digest: strings.Repeat("a", 64), want: strings.Repeat("a", 12)},
		{name: "exactly twelve", digest: "0123456789ab", want: "0123456789ab"},
		{name: "shorter than twelve", digest: "0123", want: "0123"},
		{name: "empty", digest: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embeddigest.Short(test.digest), qt.Equals, test.want)
		})
	}
}
