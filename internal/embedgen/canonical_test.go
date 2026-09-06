package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
)

// row builds a source row from the values a test names.
func row(values ...*string) embedgen.Row {
	return embedgen.Row{Key: []string{"1"}, Fields: values}
}

// TestCanonicalize_TheOrderAndSeparatorAreTheText is the property everything
// downstream rests on.
//
// The same row and specification must produce the same bytes anywhere, and the
// bytes are decided by the specification alone -- which is why every input to
// them is part of the generation identity.
func TestCanonicalize_TheOrderAndSeparatorAreTheText(t *testing.T) {
	tests := []struct {
		name      string
		separator string
		prefix    string
		fields    []*string
		want      string
	}{
		{name: "two fields", separator: " | ", fields: []*string{new("Title"), new("Body")}, want: "Title | Body"},
		{
			name: "the order is the text", separator: " | ",
			fields: []*string{new("Body"), new("Title")}, want: "Body | Title",
		},
		{
			name: "a prefix some models expect", separator: " ", prefix: "passage: ",
			fields: []*string{new("Title"), new("Body")}, want: "passage: Title Body",
		},
		{name: "one field", separator: " | ", fields: []*string{new("Only")}, want: "Only"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Preprocessing.Separator = test.separator
			spec.Preprocessing.Prefix = test.prefix
			spec.Preprocessing.CollapseWhitespace = false
			spec.Source.InputFields = fieldNames(len(test.fields))

			set, err := spec.CanonicalInputs(row(test.fields...))

			c.Assert(err, qt.IsNil)
			c.Assert(set.Whole.Text, qt.Equals, test.want)
			c.Assert(set.Whole.Truncated, qt.IsFalse)
			c.Assert(set.Whole.Skipped, qt.IsFalse)
		})
	}
}

// TestCanonicalize_TheNullPolicyDecidesWhatANullContributes pins the three
// answers, because a NULL is not the empty string and the difference is
// visible in the text a provider sees.
func TestCanonicalize_TheNullPolicyDecidesWhatANullContributes(t *testing.T) {
	tests := []struct {
		name      string
		policy    embedgen.NullPolicy
		want      string
		wantError bool
	}{
		{name: "as empty keeps the field's place", policy: embedgen.NullAsEmpty, want: "Title||Body"},
		{name: "skip removes it and its separator", policy: embedgen.NullSkipField, want: "Title|Body"},
		{name: "refuse declines the row", policy: embedgen.NullRefuseRow, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Preprocessing.Separator = "|"
			spec.Preprocessing.NullPolicy = test.policy
			spec.Preprocessing.CollapseWhitespace = false
			spec.Source.InputFields = []string{"title", "middle", "body"}

			set, err := spec.CanonicalInputs(row(new("Title"), nil, new("Body")))

			c.Assert(err != nil, qt.Equals, test.wantError)
			c.Assert(set.Whole.Text, qt.Equals, test.want)
		})
	}
}

// TestCanonicalize_ARefusalIsItsOwnClass is what lets a caller tell "this row
// must not be embedded on these terms" from "the run broke".
func TestCanonicalize_ARefusalIsItsOwnClass(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()
	spec.Preprocessing.NullPolicy = embedgen.NullRefuseRow
	spec.Source.InputFields = []string{"title", "body"}

	_, err := spec.CanonicalInputs(row(new("Title"), nil))

	c.Assert(err, qt.ErrorIs, embedgen.ErrRefused)
}

// TestCanonicalize_TruncationIsNeverSilent is the epic's explicit rule.
//
// Either the row is refused, or it is cut AND the cut is reported, so a caller
// storing the result can tell a whole input from a truncated one.
func TestCanonicalize_TruncationIsNeverSilent(t *testing.T) {
	tests := []struct {
		name          string
		policy        embedgen.TruncatePolicy
		wantError     bool
		wantText      string
		wantTruncated bool
	}{
		{name: "refuse", policy: embedgen.TruncateRefuse, wantError: true},
		{name: "truncate, and say so", policy: embedgen.TruncateBytes, wantText: "abcde", wantTruncated: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Preprocessing.MaxInputBytes = 5
			spec.Preprocessing.Truncate = test.policy
			spec.Preprocessing.CollapseWhitespace = false
			spec.Source.InputFields = []string{"body"}

			set, err := spec.CanonicalInputs(row(new("abcdefghij")))

			c.Assert(err != nil, qt.Equals, test.wantError)
			c.Assert(set.Whole.Text, qt.Equals, test.wantText)
			c.Assert(set.Whole.Truncated, qt.Equals, test.wantTruncated)
		})
	}
}

// TestCanonicalize_TruncationDoesNotSplitARune is what stops the cut producing
// bytes no provider accepts.
//
// A multi-byte rune cut in half is not a shorter input; it is invalid UTF-8.
// The bound here falls inside the second rune, so the honest answer is the
// first one alone.
func TestCanonicalize_TruncationDoesNotSplitARune(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()
	spec.Preprocessing.MaxInputBytes = 4
	spec.Preprocessing.Truncate = embedgen.TruncateBytes
	spec.Preprocessing.CollapseWhitespace = false
	spec.Preprocessing.UnicodeNormalization = embedgen.UnicodeNone
	spec.Source.InputFields = []string{"body"}

	set, err := spec.CanonicalInputs(row(new("日本語")))

	c.Assert(err, qt.IsNil)
	c.Assert(set.Whole.Text, qt.Equals, "日")
	c.Assert(set.Whole.Truncated, qt.IsTrue)
}

// TestCanonicalize_AnEmptyInputIsAnswered pins the two policies. A model asked
// to embed nothing answers with a vector that means nothing, and a corpus
// carrying those retrieves them.
func TestCanonicalize_AnEmptyInputIsAnswered(t *testing.T) {
	tests := []struct {
		name        string
		policy      embedgen.EmptyPolicy
		wantError   bool
		wantSkipped bool
	}{
		{name: "refuse", policy: embedgen.EmptyRefuseRow, wantError: true},
		{name: "skip, and record why", policy: embedgen.EmptySkipRow, wantSkipped: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Preprocessing.EmptyPolicy = test.policy
			spec.Source.InputFields = []string{"body"}

			set, err := spec.CanonicalInputs(row(new("   ")))

			c.Assert(err != nil, qt.Equals, test.wantError)
			c.Assert(set.Whole.Skipped, qt.Equals, test.wantSkipped)
			c.Assert(set.Whole.SkipReason != "", qt.Equals, test.wantSkipped)
		})
	}
}

// TestCanonicalize_UnicodeAndWhitespaceFollowTheSpecification is why both are
// in the identity: they change the bytes a model sees.
func TestCanonicalize_UnicodeAndWhitespaceFollowTheSpecification(t *testing.T) {
	tests := []struct {
		name     string
		form     embedgen.UnicodeForm
		collapse bool
		input    string
		want     string
	}{
		// U+00E9 composed against e + U+0301 decomposed: one grapheme, two
		// encodings, and a model sees different bytes for each.
		{name: "NFC composes", form: embedgen.UnicodeNFC, input: "é", want: "é"},
		{name: "NFD decomposes", form: embedgen.UnicodeNFD, input: "é", want: "é"},
		{name: "none leaves the bytes alone", form: embedgen.UnicodeNone, input: "é", want: "é"},
		{
			name: "whitespace collapses", form: embedgen.UnicodeNone, collapse: true,
			input: "  a \n\t b  ", want: "a b",
		},
		{
			name: "and is left alone when the specification says so", form: embedgen.UnicodeNone,
			input: "  a  b  ", want: "  a  b  ",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Preprocessing.UnicodeNormalization = test.form
			spec.Preprocessing.CollapseWhitespace = test.collapse
			spec.Source.InputFields = []string{"body"}

			set, err := spec.CanonicalInputs(row(new(test.input)))

			c.Assert(err, qt.IsNil)
			c.Assert(set.Whole.Text, qt.Equals, test.want)
		})
	}
}

// TestCanonicalize_RefusesARowOfTheWrongShape is the guard against a caller
// and a specification disagreeing about how many fields a row has.
func TestCanonicalize_RefusesARowOfTheWrongShape(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()
	spec.Source.InputFields = []string{"title", "body"}

	_, err := spec.CanonicalInputs(row(new("only one")))

	c.Assert(err, qt.ErrorMatches, `canonicalize: the specification names 2 input fields and the row carries 1`)
}

// fieldNames makes a specification's input list the size a row needs.
func fieldNames(count int) []string {
	names := make([]string, 0, count)
	for index := range count {
		names = append(names, "f"+string(rune('a'+index)))
	}
	return names
}
