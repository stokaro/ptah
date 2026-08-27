package embedgen

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// SourceInputHash is the digest of the exact canonical text a provider was
// given, bound to the generation that gave it.
//
// It answers "does this target row still match its source" without reading the
// vector, which is what makes four things possible: refusing a result computed
// from source text that has since changed, verifying freshness across a whole
// corpus, skipping rows whose input did not move, and recomputing selectively.
//
// It is bound to the generation identity on purpose. The same text under two
// generations produces two different vectors, so a hash that ignored the
// generation would call a row fresh while it held the previous generation's
// answer (stokaro/ptah#2068).
//
// It is not a secrecy mechanism and must not be presented as one: it digests
// text an attacker with the source can reproduce.
func (s Spec) SourceInputHash(input CanonicalInput) string {
	var b strings.Builder
	writeComponent(&b, "source-input-hash")
	writeComponent(&b, s.Identity().Digest)
	writeComponent(&b, input.Text)
	// Truncation is part of the hash because a truncated input and the whole
	// one are different text with the same prefix, and a target row carrying
	// one must not read as fresh against the other.
	mark := "whole"
	if input.Truncated {
		mark = "truncated"
	}
	writeComponent(&b, mark)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// TargetRow is what a generation records beside each vector.
//
// The epic names these as the row's retained facts, and each one answers a
// question the migration has to ask later: which source row this is, which
// generation produced it, whether the source has moved since, which version was
// read, and when.
type TargetRow struct {
	// Key is the source key, in the specification's key order.
	Key []string
	// GenerationIdentity is the digest of the specification that produced the
	// vector.
	GenerationIdentity string
	// SourceInputHash is [Spec.SourceInputHash] of the input embedded.
	SourceInputHash string
	// SourceVersion is the version read when the input was taken, under the
	// specification's version strategy.
	SourceVersion string
	// EmbeddedAt is when the vector was written, as an RFC 3339 timestamp.
	//
	// It is recorded and is NOT part of any identity or hash: two runs of one
	// specification over one row produce the same vector at different times.
	EmbeddedAt string
	// Truncated reports that the embedded input was cut.
	Truncated bool
}

// Stale reports whether a recorded target row no longer matches the source it
// was computed from.
//
// A row is stale when the input text has moved, when the generation it belongs
// to is not the one being asked about, or when the source has advanced past the
// version the row was computed at. The three are separate questions and the
// answer is deliberately one boolean plus a reason, so a caller can act and a
// diagnostic can explain.
func (t TargetRow) Stale(generation, currentInputHash, currentVersion string) (bool, string) {
	switch {
	case t.GenerationIdentity != generation:
		return true, "the row belongs to a different generation"
	case t.SourceInputHash != currentInputHash:
		return true, "the source input has changed since the vector was computed"
	case currentVersion != "" && t.SourceVersion != "" && currentVersion != t.SourceVersion:
		return true, "the source has advanced past the version the vector was computed at"
	default:
		return false, ""
	}
}
