package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
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
