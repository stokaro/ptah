package embedgen_test

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
)

// chunkingSpec is baseSpec split into chunks of the given size and overlap.
//
// The separator is empty and the prefix unset, so the canonical text is the
// input field verbatim and a test asserting on chunk boundaries is asserting
// about the split rather than about the assembly.
func chunkingSpec(bound, overlap int) embedgen.Spec {
	spec := baseSpec()
	spec.Preprocessing.Separator = ""
	spec.Preprocessing.Prefix = ""
	spec.Preprocessing.CollapseWhitespace = false
	spec.Preprocessing.UnicodeNormalization = embedgen.UnicodeNone
	spec.Preprocessing.Truncate = embedgen.TruncateChunk
	spec.Preprocessing.MaxInputBytes = bound
	spec.Preprocessing.OverlapBytes = overlap
	spec.Source.InputFields = []string{"body"}
	return spec
}

// textsOf is what a set of inputs says, in order.
func textsOf(inputs []embedgen.CanonicalInput) []string {
	texts := make([]string, 0, len(inputs))
	for _, input := range inputs {
		texts = append(texts, input.Text)
	}
	return texts
}

// TestCanonicalInputs_AnUnchunkedSpecificationAnswersWithOne is the control
// every row below rests on.
//
// A specification that does not chunk must produce exactly one input, and it
// must be the one Canonicalize has always produced. Without this, a split that
// fired unconditionally would satisfy every chunking assertion here while
// quietly turning every existing corpus into a set.
func TestCanonicalInputs_AnUnchunkedSpecificationAnswersWithOne(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()
	spec.Source.InputFields = []string{"body"}
	long := new(strings.Repeat("word ", 500))

	set, err := spec.CanonicalInputs(embedgen.Row{Key: []string{"1"}, Fields: []*string{long}})

	c.Assert(err, qt.IsNil)
	c.Assert(set.Chunks, qt.HasLen, 1)
	c.Assert(set.Chunks[0].Ordinal, qt.Equals, 0)
	c.Assert(spec.Chunks(), qt.IsFalse)
}

// TestCanonicalInputs_TheSplitIsDeterministicAndOrdered pins the boundaries.
//
// The rows carry the exact pieces rather than a count, because a count passes
// for a split that cuts in the wrong places -- and where a chunk begins is the
// whole of what the overlap is for.
func TestCanonicalInputs_TheSplitIsDeterministicAndOrdered(t *testing.T) {
	tests := []struct {
		name    string
		bound   int
		overlap int
		text    string
		want    []string
	}{
		{
			name:  "shorter than the bound is one chunk",
			bound: 16, overlap: 0, text: "short",
			want: []string{"short"},
		},
		{
			name:  "exactly the bound is one chunk",
			bound: 16, overlap: 0, text: "0123456789abcdef",
			want: []string{"0123456789abcdef"},
		},
		{
			name:  "no overlap cuts end to end",
			bound: 16, overlap: 0, text: "0123456789abcdefGHIJKLMNOPQRSTUV!",
			want: []string{"0123456789abcdef", "GHIJKLMNOPQRSTUV", "!"},
		},
		{
			name:  "an overlap repeats the tail of the one before",
			bound: 16, overlap: 4, text: "0123456789abcdefGHIJKLMN",
			want: []string{"0123456789abcdef", "cdefGHIJKLMN"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := chunkingSpec(test.bound, test.overlap)

			set, err := spec.CanonicalInputs(
				embedgen.Row{Key: []string{"1"}, Fields: []*string{new(test.text)}})

			c.Assert(err, qt.IsNil)
			c.Assert(textsOf(set.Chunks), qt.DeepEquals, test.want)
		})
	}
}

// TestCanonicalInputs_TheOrdinalIsThePositionInTheSet is what the storage keys
// a chunk by.
func TestCanonicalInputs_TheOrdinalIsThePositionInTheSet(t *testing.T) {
	c := qt.New(t)
	spec := chunkingSpec(16, 0)

	set, err := spec.CanonicalInputs(embedgen.Row{
		Key: []string{"1"}, Fields: []*string{new(strings.Repeat("x", 40))}})

	c.Assert(err, qt.IsNil)
	c.Assert(set.Chunks, qt.HasLen, 3)
	c.Assert(set.Chunks[0].Ordinal, qt.Equals, 0)
	c.Assert(set.Chunks[1].Ordinal, qt.Equals, 1)
	c.Assert(set.Chunks[2].Ordinal, qt.Equals, 2)
}

// TestCanonicalInputs_EveryChunkIsValidUTF8AndWithinTheBound is the property a
// provider depends on.
//
// A split that cut through a multi-byte rune would hand the provider invalid
// UTF-8 and begin the next chunk with the other half. The text here is chosen
// so a byte-counted cut lands inside a rune: every character is three bytes and
// the bound is not a multiple of three.
//
// The bound assertion is the pair: a splitter that avoided the rune problem by
// never cutting would satisfy the UTF-8 half and produce one oversized chunk.
func TestCanonicalInputs_EveryChunkIsValidUTF8AndWithinTheBound(t *testing.T) {
	c := qt.New(t)
	// U+4E2D, three bytes each, and a bound of 20 which is not a multiple of 3.
	text := strings.Repeat("中", 40)
	spec := chunkingSpec(20, 0)

	set, err := spec.CanonicalInputs(
		embedgen.Row{Key: []string{"1"}, Fields: []*string{new(text)}})

	c.Assert(err, qt.IsNil)
	c.Assert(len(set.Chunks) > 1, qt.IsTrue)
	for _, input := range set.Chunks {
		c.Assert(utf8.ValidString(input.Text), qt.IsTrue,
			qt.Commentf("chunk %d is not valid UTF-8: %q", input.Ordinal, input.Text))
		c.Assert(len(input.Text) <= 20, qt.IsTrue,
			qt.Commentf("chunk %d is %d bytes and the bound is 20", input.Ordinal, len(input.Text)))
	}
}

// TestCanonicalInputs_TheChunksConcatenateToTheInput is coverage stated
// exactly, and it needs no overlap to say it.
//
// With no overlap the chunks are back to back, so concatenation IS the text --
// no search, no reconstruction, nothing that can agree with a defect. A
// splitter that dropped a rune at every boundary reddens here, and that is the
// failure no boundary assertion above catches: the pieces would still be
// valid, ordered and within the bound.
func TestCanonicalInputs_TheChunksConcatenateToTheInput(t *testing.T) {
	tests := []struct {
		name  string
		bound int
		text  string
	}{
		{name: "ascii", bound: 16, text: distinctASCII(200)},
		{name: "ascii, a bound that lands mid-token", bound: 23, text: distinctASCII(200)},
		{name: "three-byte runes", bound: 20, text: distinctRunes(60)},
		{name: "a bound one byte off a rune multiple", bound: 19, text: distinctRunes(60)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := chunkingSpec(test.bound, 0)

			set, err := spec.CanonicalInputs(
				embedgen.Row{Key: []string{"1"}, Fields: []*string{new(test.text)}})

			c.Assert(err, qt.IsNil)
			c.Assert(len(set.Chunks) > 1, qt.IsTrue)
			c.Assert(strings.Join(textsOf(set.Chunks), ""), qt.Equals, test.text)
		})
	}
}

// TestCanonicalInputs_AnOverlappingSetStillCoversTheInput says the same thing
// where concatenation cannot.
//
// With an overlap the chunks repeat each other, so the statement is positional:
// each chunk occurs in the text, the occurrences advance, and no chunk begins
// after the previous one ended. A dropped rune shows up as the gap that last
// condition forbids.
//
// The first chunk starting at the beginning and the last ending at the end are
// the two ends the middle conditions cannot supply: a splitter that lost the
// head or the tail satisfies every adjacency and covers less than the input.
func TestCanonicalInputs_AnOverlappingSetStillCoversTheInput(t *testing.T) {
	tests := []struct {
		name    string
		bound   int
		overlap int
		text    string
	}{
		{name: "ascii", bound: 32, overlap: 8, text: distinctASCII(200)},
		{name: "three-byte runes", bound: 20, overlap: 5, text: distinctRunes(60)},
		{name: "an overlap one byte off a rune multiple", bound: 22, overlap: 7, text: distinctRunes(60)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := chunkingSpec(test.bound, test.overlap)

			set, err := spec.CanonicalInputs(
				embedgen.Row{Key: []string{"1"}, Fields: []*string{new(test.text)}})

			c.Assert(err, qt.IsNil)
			c.Assert(len(set.Chunks) > 1, qt.IsTrue)

			offsets, reach := chunkOffsets(test.text, textsOf(set.Chunks))
			c.Assert(offsets[0], qt.Equals, 0, qt.Commentf("the set does not start at the input"))
			c.Assert(reach, qt.Equals, len(test.text), qt.Commentf("the set does not reach the end"))
			c.Assert(sort.IntsAreSorted(offsets), qt.IsTrue,
				qt.Commentf("the chunks do not advance through the input: %v", offsets))
			c.Assert(gapsIn(offsets, textsOf(set.Chunks)), qt.HasLen, 0)
		})
	}
}

// distinctRunes builds text whose every position is distinguishable, out of
// runes that are three bytes each.
//
// Repeated text cannot measure coverage, and finding that out cost two rounds.
// With `strings.Repeat("\u4e2d", 60)` every suffix of what had been rebuilt was
// also a prefix of the next piece, so a reconstruction that searched for the
// overlap matched all of it and appended nothing -- the test failed against a
// splitter that was correct. Text where no substring repeats is what makes a
// positional statement about the chunks mean anything.
func distinctRunes(count int) string {
	var text strings.Builder
	for index := range count {
		text.WriteRune(rune(0x4E00 + index))
	}
	return text.String()
}

// distinctASCII builds single-byte text with the same property.
//
// The index itself, so the tokens strictly increase and no substring of the
// result repeats. A cycling alphabet would look distinct and repeat every
// period, which is the same false result in a different disguise.
func distinctASCII(count int) string {
	var text strings.Builder
	for index := range count {
		text.WriteString(strconv.Itoa(index))
		text.WriteByte(',')
	}
	return text.String()
}

// chunkOffsets locates each chunk in the text, searching forward from where
// the previous one began, and reports how far the last one reaches.
//
// Forward from the previous START rather than from its end, because chunks
// overlap. Searching from zero would find an earlier occurrence of a repeated
// piece and report an order the split does not have.
func chunkOffsets(text string, chunks []string) ([]int, int) {
	offsets := make([]int, 0, len(chunks))
	from := 0
	for _, chunk := range chunks {
		at := strings.Index(text[from:], chunk)
		offsets = append(offsets, from+at)
		from += at
	}
	return offsets, from + len(chunks[len(chunks)-1])
}

// gapsIn reports every place a chunk begins after the previous one ended.
func gapsIn(offsets []int, chunks []string) []string {
	gaps := make([]string, 0)
	for index := 1; index < len(offsets); index++ {
		end := offsets[index-1] + len(chunks[index-1])
		if offsets[index] <= end {
			continue
		}
		gaps = append(gaps, fmt.Sprintf(
			"chunk %d ends at %d and chunk %d begins at %d", index-1, end, index, offsets[index]))
	}
	return gaps
}

// TestCanonicalInputs_ChunkingFailurePath is the pair of settings that make a
// split impossible.
//
// The overlap row is the one that matters: an overlap at or above the bound is
// not a wide overlap, it is a step of zero, and a splitter that clamped it
// would loop over one row forever rather than answer.
func TestCanonicalInputs_ChunkingFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		bound   int
		overlap int
		wantErr string
	}{
		{
			name: "a bound too small to hold a passage", bound: 8, overlap: 0,
			wantErr: `.*chunking needs a max_input_bytes of at least 16 and the specification has 8`,
		},
		{
			name: "no bound at all", bound: 0, overlap: 0,
			wantErr: `.*chunking needs a max_input_bytes of at least 16 and the specification has 0`,
		},
		{
			name: "a negative overlap", bound: 32, overlap: -1,
			wantErr: `.*overlap_bytes is -1 and cannot be negative`,
		},
		{
			name: "an overlap the size of the bound", bound: 32, overlap: 32,
			wantErr: `.*overlap_bytes is 32 and max_input_bytes is 32, so each chunk would repeat.*`,
		},
		{
			name: "an overlap larger than the bound", bound: 32, overlap: 33,
			wantErr: `.*overlap_bytes is 33 and max_input_bytes is 32, so each chunk would repeat.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := chunkingSpec(test.bound, test.overlap)

			set, err := spec.CanonicalInputs(
				embedgen.Row{Key: []string{"1"}, Fields: []*string{new(strings.Repeat("x", 200))}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, embedgen.ErrRefused)
			c.Assert(set.Chunks, qt.IsNil)
		})
	}
}

// TestTargetObjects_ChunkingAndTheLayoutMustAgree is the pair of refusals that
// keep a chunking specification from meaning less than it says.
//
// The layout row is the one with rows at stake: the columns beside a source row
// hold one vector, so a set stored there would be written over itself and the
// corpus would hold one piece of each row while reporting itself covered.
//
// The overlap row is the quieter one. A number an author wrote that nothing
// reads is the failure this repository refuses everywhere else, and it would be
// outside the identity as well -- so two specifications differing only in it
// would be one generation.
func TestTargetObjects_ChunkingAndTheLayoutMustAgree(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*embedgen.Spec)
		wantErr string
	}{
		{
			name: "chunking into the source row",
			change: func(s *embedgen.Spec) {
				s.Preprocessing.Truncate = embedgen.TruncateChunk
				s.Preprocessing.MaxInputBytes = 64
			},
			wantErr: `target objects: the specification splits a row into a set of chunks, and layout "" stores one vector per source row.*`,
		},
		{
			name: "an overlap nothing splits",
			change: func(s *embedgen.Spec) {
				s.Preprocessing.Truncate = embedgen.TruncateRefuse
				s.Preprocessing.OverlapBytes = 200
			},
			wantErr: `target objects: overlap_bytes is 200 and the truncation policy is "refuse", which does not split anything.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			test.change(&spec)

			objects, err := spec.TargetObjects()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects, qt.DeepEquals, embedgen.TargetObjects{})
		})
	}
}

// TestTargetObjects_ChunkingIntoARelationOfItsOwnIsAccepted is the control the
// refusals need.
//
// Without it, a validation that refused every chunking specification would
// satisfy both rows above and the feature would not exist.
func TestTargetObjects_ChunkingIntoARelationOfItsOwnIsAccepted(t *testing.T) {
	c := qt.New(t)
	spec := chunkingSpec(64, 16)
	spec.Target.Table = "article_chunks"
	spec.Target.Layout = embedgen.LayoutOwnTable

	objects, err := spec.TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.OwnsTable, qt.IsTrue)
}

// TestChunks_ReportsWhatTheSpecificationDoes pins the predicate four call sites
// read.
func TestChunks_ReportsWhatTheSpecificationDoes(t *testing.T) {
	c := qt.New(t)

	c.Assert(chunkingSpec(32, 4).Chunks(), qt.IsTrue)
	c.Assert(baseSpec().Chunks(), qt.IsFalse)

	bytesPolicy := baseSpec()
	bytesPolicy.Preprocessing.Truncate = embedgen.TruncateBytes
	c.Assert(bytesPolicy.Chunks(), qt.IsFalse)
}
