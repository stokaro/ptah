package embedgen

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Row is one source row's input fields, in the specification's order.
//
// A nil entry is a NULL. That distinction is the reason this is a slice of
// pointers rather than of strings: a NULL column and a column holding the empty
// string are different facts about the row, and [Preprocessing.NullPolicy]
// answers them differently.
type Row struct {
	// Key is the row's key values, in the specification's key order.
	Key []string
	// Fields are the input field values, in the specification's input order,
	// with nil for NULL.
	Fields []*string
}

// CanonicalInput is the exact text handed to a provider, and the facts about
// how it was derived.
type CanonicalInput struct {
	// Text is what the provider is asked to embed.
	Text string
	// Truncated reports whether the bound cut it. It is recorded rather than
	// implied: silent truncation is what makes a corpus quietly wrong, and a
	// caller storing this alongside the vector can tell a whole input from a
	// cut one.
	Truncated bool
	// Skipped reports a row the policy declines to embed rather than refuse,
	// which verification reads as an intentional gap rather than a miss.
	Skipped bool
	// SkipReason names why, and is empty when Skipped is false.
	SkipReason string
}

// ErrRefused is the class of every refusal a policy asks for.
//
// A refusal is not a failure of the run: it is the specification saying this
// row must not be embedded on the terms available. The caller decides whether
// that stops the migration or is recorded and skipped, and it cannot decide
// that if the two arrive as one error.
var ErrRefused = errors.New("row refused by the specification")

// Canonicalize turns one row into the exact text a provider is asked to embed.
//
// Determinism is the whole point: the same row and the same specification must
// produce the same bytes on any machine, in any order, at any time. Everything
// that could vary -- field order, NULL handling, Unicode form, whitespace, the
// size bound -- is named by the specification and part of its identity, so a
// change to any of them is a new generation rather than a quiet difference in
// what the corpus means (stokaro/ptah#2068).
func (s Spec) Canonicalize(row Row) (CanonicalInput, error) {
	if len(row.Fields) != len(s.Source.InputFields) {
		return CanonicalInput{}, fmt.Errorf(
			"canonicalize: the specification names %d input fields and the row carries %d",
			len(s.Source.InputFields), len(row.Fields))
	}

	parts, err := s.canonicalParts(row)
	if err != nil {
		return CanonicalInput{}, err
	}

	text := s.Preprocessing.Prefix + strings.Join(parts, s.Preprocessing.Separator)
	text = s.applyUnicodeForm(text)
	if s.Preprocessing.CollapseWhitespace {
		text = collapseWhitespace(text)
	}

	if strings.TrimSpace(text) == "" {
		return s.emptyInput()
	}
	return s.applyBound(text)
}

// canonicalParts renders each input field under the null policy.
func (s Spec) canonicalParts(row Row) ([]string, error) {
	parts := make([]string, 0, len(row.Fields))
	for index, value := range row.Fields {
		if value != nil {
			parts = append(parts, *value)
			continue
		}
		switch s.Preprocessing.NullPolicy {
		case NullAsEmpty:
			parts = append(parts, "")
		case NullSkipField:
			// The field and the separator that would follow it are both gone,
			// which is what "skip" has to mean: keeping the separator would
			// leave the shape of a field that is not there.
		case NullRefuseRow:
			return nil, fmt.Errorf("%w: field %q is NULL and the null policy is %q",
				ErrRefused, s.Source.InputFields[index], NullRefuseRow)
		default:
			return nil, fmt.Errorf("canonicalize: unknown null policy %q", s.Preprocessing.NullPolicy)
		}
	}
	return parts, nil
}

// emptyInput answers a canonical input that came out empty.
func (s Spec) emptyInput() (CanonicalInput, error) {
	switch s.Preprocessing.EmptyPolicy {
	case EmptySkipRow:
		return CanonicalInput{
			Skipped:    true,
			SkipReason: "the canonical input is empty and the empty policy is skip",
		}, nil
	case EmptyRefuseRow:
		return CanonicalInput{}, fmt.Errorf(
			"%w: the canonical input is empty and the empty policy is %q", ErrRefused, EmptyRefuseRow)
	default:
		return CanonicalInput{}, fmt.Errorf("canonicalize: unknown empty policy %q", s.Preprocessing.EmptyPolicy)
	}
}

// applyBound enforces the size bound, and never silently.
func (s Spec) applyBound(text string) (CanonicalInput, error) {
	bound := s.Preprocessing.MaxInputBytes
	if bound <= 0 || len(text) <= bound {
		return CanonicalInput{Text: text}, nil
	}
	switch s.Preprocessing.Truncate {
	case TruncateRefuse:
		return CanonicalInput{}, fmt.Errorf(
			"%w: the canonical input is %d bytes and the bound is %d, with truncation %q",
			ErrRefused, len(text), bound, TruncateRefuse)
	case TruncateBytes:
		return CanonicalInput{Text: truncateOnRune(text, bound), Truncated: true}, nil
	default:
		return CanonicalInput{}, fmt.Errorf("canonicalize: unknown truncation policy %q", s.Preprocessing.Truncate)
	}
}

// applyUnicodeForm normalizes the assembled text.
func (s Spec) applyUnicodeForm(text string) string {
	switch s.Preprocessing.UnicodeNormalization {
	case UnicodeNFC:
		return norm.NFC.String(text)
	case UnicodeNFD:
		return norm.NFD.String(text)
	case UnicodeNFKC:
		return norm.NFKC.String(text)
	case UnicodeNFKD:
		return norm.NFKD.String(text)
	default:
		return text
	}
}

// collapseWhitespace folds runs of whitespace to one space and trims the ends.
func collapseWhitespace(text string) string {
	return strings.Join(strings.FieldsFunc(text, unicode.IsSpace), " ")
}

// truncateOnRune cuts at or below the bound without splitting a rune.
//
// Cutting mid-rune would hand the provider invalid UTF-8, and the bytes a
// provider rejects are not a smaller version of the input -- they are a
// different one.
func truncateOnRune(text string, bound int) string {
	if len(text) <= bound {
		return text
	}
	cut := bound
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}
