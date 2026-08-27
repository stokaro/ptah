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
	var b strings.Builder
	for _, component := range components {
		b.WriteString(strconv.Itoa(len(component)))
		b.WriteByte(':')
		b.WriteString(component)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
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
