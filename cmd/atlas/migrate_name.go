package atlas

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// errAtlasMigrationNameNotAnElement reports a migration name that cannot become
// one file name in the migration directory.
//
// The sentence is deliberately the one
// [go.5x5.cz/ptah/internal/atlasmigrateimport.ErrSkeletonNameNotAnElement]
// already prints for the same mistake on a foreign directory layout. One rule
// with two spellings is how an operator learns that the two paths are different
// rules, which they are not.
var errAtlasMigrationNameNotAnElement = errors.New(
	"migration name must be a single file name element, without a path separator",
)

// checkAtlasMigrationName refuses a migration name that cannot become a file in
// the migration directory, for the verbs that write into an Atlas-layout one.
//
// The community binary composes `<version>_<name>.sql` from the name verbatim
// and opens it, so a name carrying a path separator names a file in a directory
// that does not exist and the run fails, having created nothing. Measured on
// the pinned v1.3.0:
//
//	migrate new "sub/dir_name"   exit 1, open …/20260808055159_sub/dir_name.sql:
//	                             no such file or directory, no file written
//	migrate diff "sub/name"      exit 1, same shape, on a directory with changes
//	migrate new "a b"            exit 0, writes `<version>_a b.sql`
//	migrate new 'a\b'            exit 0, writes `<version>_a\b.sql`
//	migrate new ".."             exit 0, writes `<version>_...sql`
//
// Ptah stripped the separator instead and wrote `<version>_subdir_name.sql` at
// exit 0 (stokaro/ptah#1231 case 6): a file the author did not ask for, under a
// name that no longer says what they typed, in the directory the next
// `migrate apply` will run.
//
// The refusal names the rule rather than reproducing that binary's message. The
// message there is the raw `open …: no such file or directory` of a write it
// did not expect to fail -- it leaks an internal path and never says what to
// change -- so reproducing it would import a defect along with the exit code.
// AGENTS.md's `file()` sandbox precedent is the same trade: same direction,
// better reason.
//
// Only the path separator is refused here, and that too is measured rather than
// assumed: a space, a backslash on this platform, and `..` are all accepted
// there, so refusing them would refuse what it accepts. The foreign-layout path
// draws its own rule wider on purpose, in the strict direction, and that
// divergence predates this one.
func checkAtlasMigrationName(verb, name string) error {
	if !strings.ContainsRune(name, '/') && !strings.ContainsRune(name, os.PathSeparator) {
		return nil
	}
	return fmt.Errorf("atlas migrate %s %q: %w", verb, name, errAtlasMigrationNameNotAnElement)
}
