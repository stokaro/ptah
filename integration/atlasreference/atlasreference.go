// Package atlasreference names the pinned Atlas community binary the
// conformance tests measure against, and the one place its address is spelled.
//
// The word this package replaces was "oracle", in the testing sense of a
// trusted answer to compare against. That was the right word while it was the
// only one: Ptah now ships an Oracle DATABASE dialect, and the two senses sat
// in adjacent trees under one spelling -- `oracle_version_test.go` about Oracle
// release lines beside `revision_identity_oracle_test.go` about a pinned Atlas
// build. A grep for either returned the other, which is the specific cost: the
// term stopped narrowing anything (stokaro/ptah#1887).
//
// The dialect keeps the name, because it is the engine's. This side is renamed
// because "reference implementation" is what the thing actually is.
package atlasreference

import "os"

// EnvVar names the environment variable holding the path to the pinned
// binary.
//
// One constant rather than a const in each test package, which is how the old
// spelling came to be written out fourteen times: a rename then has fourteen
// places to miss one.
const EnvVar = "PTAH_ATLAS_REFERENCE"

// LegacyEnvVar is the spelling this variable had before the dialect arrived.
//
// Honored so a checkout configured before the rename keeps working, and named
// so nobody adds a third. It is read only when EnvVar is unset, so a shell that
// carries both gets the current one.
const LegacyEnvVar = "PTAH_ATLAS_ORACLE"

// Version is the only build the conformance runs trust. A different build may
// have changed the very rules under test, so comparing against it would report
// divergences that are really version drift.
const Version = "atlas community version v1.3.0"

// Binary returns the configured path and whether one was configured at all.
func Binary() (string, bool) {
	if path, ok := os.LookupEnv(EnvVar); ok && path != "" {
		return path, true
	}
	path, ok := os.LookupEnv(LegacyEnvVar)
	return path, ok && path != ""
}
