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
	// Ordinal is this chunk's position in its source row's set, and is zero
	// for a specification that does not chunk.
	//
	// It orders; it does not identify. Text that gains a sentence moves every
	// boundary after it, so the chunk at ordinal 3 before an edit and the one
	// after it are not the same chunk in any sense a rule may rely on (ADR
	// 0017 section 3.2).
	Ordinal int
}

// ErrRefused is the class of every refusal a policy asks for.
//
// A refusal is not a failure of the run: it is the specification saying this
// row must not be embedded on the terms available. The caller decides whether
// that stops the migration or is recorded and skipped, and it cannot decide
// that if the two arrive as one error.
var ErrRefused = errors.New("row refused by the specification")

// InputSet is everything one source row produces.
//
// Chunks are what a provider is asked to embed; Whole is what they were split
// from, and it is what the row's input hash is taken over.
//
// The two are separate because hashing a chunk would be wrong twice. A set
// whose first chunk is unchanged and whose fourth is not would read as the same
// work arriving again and never be rewritten -- the resolution asks its
// question once per source key, against the first member. And a verification
// recomputing a row's hash has one row to compare against, not a set of hashes
// it would have to reassemble in the right order first.
type InputSet struct {
	Chunks []CanonicalInput
	Whole  CanonicalInput
}

// Chunks reports whether this specification splits a source row into a set.
//
// A predicate rather than a comparison at each site, because "does this produce
// one row or a set" is asked by the canonicalizer, the write path, the target
// schema and the verification walk, and four spellings of one question is how
// three of them come to agree and the fourth does not.
func (s Spec) Chunks() bool {
	return s.Preprocessing.Truncate == TruncateChunk
}

// CanonicalInputs turns one row into the set of texts a provider is asked to
// embed.
//
// A specification that does not chunk answers with exactly one, which is what
// every specification means today. One that does answers with its row's chunk
// set, in order, each within MaxInputBytes.
//
// A skipped or refused row has no chunks. Skipping answers with one input
// carrying the reason rather than with none, because the caller records a skip
// against a row and a set of zero would leave it nothing to record.
//
// Determinism is the whole point: the same row and the same specification must
// produce the same bytes on any machine, in any order, at any time. Everything
// that could vary -- field order, NULL handling, Unicode form, whitespace, the
// size bound, the overlap -- is named by the specification and part of its
// identity, so a change to any of them is a new generation rather than a quiet
// difference in what the corpus means (stokaro/ptah#2068).
func (s Spec) CanonicalInputs(row Row) (InputSet, error) {
	if len(row.Fields) != len(s.Source.InputFields) {
		return InputSet{}, fmt.Errorf(
			"canonicalize: the specification names %d input fields and the row carries %d",
			len(s.Source.InputFields), len(row.Fields))
	}

	parts, err := s.canonicalParts(row)
	if err != nil {
		return InputSet{}, err
	}

	text := s.Preprocessing.Prefix + strings.Join(parts, s.Preprocessing.Separator)
	text = s.applyUnicodeForm(text)
	if s.Preprocessing.CollapseWhitespace {
		text = collapseWhitespace(text)
	}

	if strings.TrimSpace(text) == "" {
		empty, emptyErr := s.emptyInput()
		if emptyErr != nil {
			return InputSet{}, emptyErr
		}
		return InputSet{Chunks: []CanonicalInput{empty}, Whole: empty}, nil
	}
	if s.Chunks() {
		chunks, splitErr := s.splitInput(text)
		if splitErr != nil {
			return InputSet{}, splitErr
		}
		return InputSet{Chunks: chunks, Whole: CanonicalInput{Text: text}}, nil
	}
	bounded, err := s.applyBound(text)
	if err != nil {
		return InputSet{}, err
	}
	return InputSet{Chunks: []CanonicalInput{bounded}, Whole: bounded}, nil
}

// splitInput cuts the canonical text into the set this specification declares.
//
// The bound and the overlap are validated here rather than trusted, because a
// step of zero or less does not produce a bad corpus, it produces a loop: the
// split never advances and the row is embedded forever. A specification is
// checked before a run starts, and this is the second place, on the path that
// would hang.
func (s Spec) splitInput(text string) ([]CanonicalInput, error) {
	if err := s.validateChunking(); err != nil {
		return nil, err
	}
	pieces := splitOnRuneBoundaries(
		text, s.Preprocessing.MaxInputBytes, s.Preprocessing.OverlapBytes)
	inputs := make([]CanonicalInput, 0, len(pieces))
	for ordinal, piece := range pieces {
		inputs = append(inputs, CanonicalInput{Text: piece, Ordinal: ordinal})
	}
	return inputs, nil
}

// MinimumChunkBytes is the smallest bound a chunking specification may carry.
//
// Four bytes is the longest a single UTF-8 rune can be, so a smaller bound
// could not hold one and the split would have to choose between exceeding the
// bound and making no progress. The floor is well above that because a bound
// near it produces a corpus of fragments rather than of passages, which is a
// configuration error rather than a corpus.
const MinimumChunkBytes = 16

// validateChunking refuses the two settings that make a split impossible.
func (s Spec) validateChunking() error {
	bound := s.Preprocessing.MaxInputBytes
	overlap := s.Preprocessing.OverlapBytes
	switch {
	case bound < MinimumChunkBytes:
		return fmt.Errorf(
			"%w: chunking needs a max_input_bytes of at least %d and the specification has %d",
			ErrRefused, MinimumChunkBytes, bound)
	case overlap < 0:
		return fmt.Errorf("%w: overlap_bytes is %d and cannot be negative", ErrRefused, overlap)
	case overlap >= bound:
		return fmt.Errorf(
			"%w: overlap_bytes is %d and max_input_bytes is %d, so each chunk would repeat "+
				"everything the last one held and the split would never reach the end of the input",
			ErrRefused, overlap, bound)
	default:
		return nil
	}
}

// splitOnRuneBoundaries cuts text into pieces of at most bound bytes, each
// repeating overlap bytes of the one before it.
//
// Both ends land on a rune boundary: a chunk cut through a multi-byte rune
// would be handed to the provider as invalid UTF-8, and the next chunk would
// begin with the other half. Cutting BACK at the end and FORWARD at the start
// is what keeps every piece valid without the two adjustments fighting.
//
// The caller has already refused a bound below [MinimumChunkBytes] and an
// overlap at or above it, so the step is positive and every rune fits.
func splitOnRuneBoundaries(text string, bound, overlap int) []string {
	pieces := make([]string, 0, len(text)/(bound-overlap)+1)
	for start := 0; start < len(text); {
		if start+bound >= len(text) {
			pieces = append(pieces, text[start:])
			break
		}
		end := start + bound
		for end > start && !utf8.RuneStart(text[end]) {
			end--
		}
		pieces = append(pieces, text[start:end])
		next := end - overlap
		for next < len(text) && !utf8.RuneStart(text[next]) {
			next++
		}
		if next <= start {
			// The overlap swallowed the step. It cannot happen for a validated
			// specification, and if it ever does the answer is to stop making
			// chunks rather than to make them forever.
			next = end
		}
		start = next
	}
	return pieces
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
