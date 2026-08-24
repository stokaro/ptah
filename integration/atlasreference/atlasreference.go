// Package atlasreference names the pinned Atlas community binary the
// conformance tests measure against, and the one place its address is spelled.
//
// "Reference implementation" is what the binary is to these tests, and the name
// has to say so rather than reach for the testing sense of "oracle" -- a trusted
// answer to compare against. Ptah ships an Oracle DATABASE dialect, so one
// spelling would serve two unrelated senses in adjacent trees, and a grep for
// either would return the other: the term would stop narrowing anything
// (stokaro/ptah#1887).
//
// The dialect keeps the name, because it is the engine's.
package atlasreference

import "os"

// EnvVar names the environment variable holding the path to the pinned
// binary.
//
// One constant rather than a const in each test package. Fourteen copies of a
// spelling are fourteen places for a rename to miss one.
const EnvVar = "PTAH_ATLAS_REFERENCE"

// Version is the only build the conformance runs trust. A different build may
// have changed the very rules under test, so comparing against it would report
// divergences that are really version drift.
const Version = "atlas community version v1.3.0"

// Binary returns the configured path and whether one was configured at all.
//
// One spelling, not two. The variable was renamed in stokaro/ptah#1938 and the
// older name was read alongside it for a moment; Ptah is pre-general-
// availability and owes no compatibility to what it itself wrote earlier, so
// carrying a second name would only be somewhere for the two to disagree.
func Binary() (string, bool) {
	path, ok := os.LookupEnv(EnvVar)
	return path, ok && path != ""
}
