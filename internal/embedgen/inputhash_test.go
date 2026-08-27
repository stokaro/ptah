package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
)

// canonical is the input a hash is taken over.
func canonical(value string) embedgen.CanonicalInput {
	return embedgen.CanonicalInput{Text: value}
}

// TestSourceInputHash_MovesWithTheTextAndTheGeneration is what makes the hash
// usable for freshness.
//
// It has to move when the source text moves -- otherwise a changed row reads as
// fresh -- and it has to move when the GENERATION moves, because the same text
// under two generations produces two different vectors. A hash that ignored the
// generation would call a row fresh while it held the previous generation's
// answer (stokaro/ptah#2068).
func TestSourceInputHash_MovesWithTheTextAndTheGeneration(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()
	other := baseSpec()
	other.Model.Identifier = "e5-large"

	base := spec.SourceInputHash(canonical("hello"))

	c.Assert(base, qt.HasLen, 64)
	c.Assert(spec.SourceInputHash(canonical("hello")), qt.Equals, base)
	c.Assert(spec.SourceInputHash(canonical("hello!")), qt.Not(qt.Equals), base)
	c.Assert(other.SourceInputHash(canonical("hello")), qt.Not(qt.Equals), base)
}

// TestSourceInputHash_SeparatesATruncatedInputFromTheWholeOne is the case a
// digest over the text alone gets wrong.
//
// A truncated input and the whole one share a prefix, and a target row computed
// from one must not read as fresh against the other.
func TestSourceInputHash_SeparatesATruncatedInputFromTheWholeOne(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()

	whole := spec.SourceInputHash(embedgen.CanonicalInput{Text: "abcde"})
	cut := spec.SourceInputHash(embedgen.CanonicalInput{Text: "abcde", Truncated: true})

	c.Assert(whole, qt.Not(qt.Equals), cut)
}

// TestTargetRow_StaleNamesWhichQuestionFailed pins the three separate reasons a
// stored vector stops matching its source.
//
// One boolean would let a caller act; the reason is what lets a diagnostic
// explain, and the three are genuinely different situations: a row from another
// generation, a row whose text moved, and a row the source has advanced past.
func TestTargetRow_StaleNamesWhichQuestionFailed(t *testing.T) {
	const generation = "gen-1"
	const inputHash = "hash-1"

	tests := []struct {
		name       string
		row        embedgen.TargetRow
		generation string
		inputHash  string
		version    string
		wantStale  bool
		wantReason string
	}{
		{
			name:       "fresh",
			row:        embedgen.TargetRow{GenerationIdentity: generation, SourceInputHash: inputHash, SourceVersion: "7"},
			generation: generation, inputHash: inputHash, version: "7",
		},
		{
			name:       "another generation",
			row:        embedgen.TargetRow{GenerationIdentity: "gen-0", SourceInputHash: inputHash},
			generation: generation, inputHash: inputHash,
			wantStale: true, wantReason: "the row belongs to a different generation",
		},
		{
			name:       "the text moved",
			row:        embedgen.TargetRow{GenerationIdentity: generation, SourceInputHash: "hash-0"},
			generation: generation, inputHash: inputHash,
			wantStale: true, wantReason: "the source input has changed since the vector was computed",
		},
		{
			name:       "the source advanced",
			row:        embedgen.TargetRow{GenerationIdentity: generation, SourceInputHash: inputHash, SourceVersion: "7"},
			generation: generation, inputHash: inputHash, version: "8",
			wantStale: true, wantReason: "the source has advanced past the version the vector was computed at",
		},
		{
			// A strategy that establishes no version leaves the question
			// unanswerable, and answering it anyway would report every row
			// stale under the input-hash strategy.
			name:       "no version on either side",
			row:        embedgen.TargetRow{GenerationIdentity: generation, SourceInputHash: inputHash},
			generation: generation, inputHash: inputHash,
		},
		{
			// The row was written without a version and the caller has one
			// now. That is a strategy change, not evidence the source moved:
			// there is no earlier version to have moved FROM, and reporting
			// stale here would recompute a corpus that is fresh.
			name:       "the row carries no version and the caller does",
			row:        embedgen.TargetRow{GenerationIdentity: generation, SourceInputHash: inputHash},
			generation: generation, inputHash: inputHash, version: "8",
		},
		{
			// And the reverse: the caller cannot establish a version now, so
			// it cannot say the row's is behind.
			name:       "the caller has no version and the row does",
			row:        embedgen.TargetRow{GenerationIdentity: generation, SourceInputHash: inputHash, SourceVersion: "7"},
			generation: generation, inputHash: inputHash,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stale, reason := test.row.Stale(test.generation, test.inputHash, test.version)

			c.Assert(stale, qt.Equals, test.wantStale)
			c.Assert(reason, qt.Equals, test.wantReason)
		})
	}
}
