package spannerschema

import "ptah.run/core/coverage"

// CoverageChangeStream is a change stream (CREATE CHANGE STREAM): a database
// object with its own lifecycle that publishes row changes to a reader outside
// the schema.
//
// Ptah does not model one. The kind exists so that saying so is possible: a
// Spanner database's description carries none of its change streams, and that
// silence is not a statement that it has none. Without the record the silence
// read as authoritative, which is how an unmodeled construct becomes a DROP
// (stokaro/ptah#2236).
//
// It is recorded whenever the target could have them rather than when a read
// found some: recording only what was found would assert that the absence of
// every other one is authoritative.
const CoverageChangeStream coverage.Kind = "change_stream"

// CoverageKinds returns the Spanner coverage kinds a document may name, in a
// new slice.
func CoverageKinds() []coverage.Kind {
	return []coverage.Kind{CoverageChangeStream}
}
