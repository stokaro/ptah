// Package embeddigest encodes a list of components into one content address.
//
// Everything in the inference lifecycle that has to be compared for exact
// equality -- a generation identity, a cutover plan an approval is bound to, a
// verification report a plan cites -- is a digest over an ordered list of
// strings. One encoder rather than one per package is not tidiness: two
// encoders that disagree by a separator produce two digests for one thing, and
// the disagreement shows up as an approval that will not bind or a generation
// that looks new every run.
package embeddigest

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// Of is the hex SHA-256 of the components, encoded length-prefixed.
//
// Length-prefixed, so no separator a value could contain decides a boundary:
// two components "a" and "b.c" and two components "a.b" and "c" are different
// lists and must produce different digests. A joiner -- any joiner -- makes
// them the same bytes, and the failure it causes is an approval binding to a
// plan that is not the plan.
func Of(components ...string) string {
	sum := sha256.Sum256([]byte(Encode(components...)))
	return hex.EncodeToString(sum[:])
}

// Encode is the length-prefixed encoding [Of] hashes, before it hashes it.
//
// Exported because a caller sometimes needs the encoding WITHOUT the hash: an
// identity that has to be compared for equality and then shown to a person
// cannot be a digest, and a caller that wrote its own joiner to keep it
// readable reintroduces exactly the boundary this encoding removes. It did:
// the verification walk joined a composite key on U+001F, so a tenant holding
// that byte could forge another row's identity (stokaro/ptah#2744).
//
// Each component is its byte length, a colon, then the component. Nothing
// separates one from the next, because nothing needs to -- the length says
// where each ends, which is what makes the encoding unambiguous for ANY
// component value including one holding a colon, a digit, or the encoding of
// another list.
//
// Decoded by [Decode].
func Encode(components ...string) string {
	var b strings.Builder
	for _, component := range components {
		b.WriteString(strconv.Itoa(len(component)))
		b.WriteByte(':')
		b.WriteString(component)
	}
	return b.String()
}

// Decode reverses [Encode], and reports whether the input was one of its
// answers.
//
// A caller that shows an identity to a person needs the components back, and
// asking it to keep them beside the identity is a second copy that can disagree
// with the first. False for anything Encode could not have produced, so a
// value from somewhere else is refused rather than rendered as a truncated
// guess.
func Decode(encoded string) ([]string, bool) {
	var components []string
	for rest := encoded; rest != ""; {
		colon := strings.IndexByte(rest, ':')
		if colon <= 0 {
			return nil, false
		}
		width, err := strconv.Atoi(rest[:colon])
		if err != nil || width < 0 {
			return nil, false
		}
		rest = rest[colon+1:]
		if len(rest) < width {
			return nil, false
		}
		components = append(components, rest[:width])
		rest = rest[width:]
	}
	return components, true
}

// ShortLength is how much of a digest a name carries.
const ShortLength = 12

// Short is a digest's leading twelve hex characters, for a name a person reads
// and a column suffix a database accepts.
func Short(digest string) string {
	if len(digest) < ShortLength {
		return digest
	}
	return digest[:ShortLength]
}
