package atlasmigrate_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// exemptFromNamingTheEngine is the one migrator in this package that may not
// name a revision engine, with the reason it may not.
//
// set.go builds its migrator for SetRevision, which refuses ClickHouse before
// it initializes anything -- migration/migrator/revisions.go turns both revision
// formats down because revision history cannot be updated atomically there, and
// integration/migrator/atlas_integration_test.go pins that refusal against a
// live server. ClickHouse is the only dialect where the engine decides whether
// the CREATE is legal, so an engine here could not reach a statement. Threading
// one anyway would put an option on the surface that changes nothing, which is
// the shape this guard exists to catch.
var exemptFromNamingTheEngine = []string{"set.go"}

// TestEveryMigratorInThisPackageNamesItsRevisionEngine guards the compatibility
// surface against losing an option the native one has.
//
// This package builds its own migrators rather than going through the native
// commands, so every option the native path applies has to be threaded here or
// it is dropped in silence -- which is how --skip-checks came to be accepted and
// ignored on the down path (stokaro/ptah#1621), and how PTAH_MIGRATIONS_ENGINE
// was unreachable from `ptah-compat migrate apply | status | down` while the
// native verbs honored it (stokaro/ptah#2234). On a replicated ClickHouse
// deployment that difference is a migration history on one node while every
// replica reports itself consistent.
//
// The walk reads the package from disk, so an entry point added later is
// measured without anyone updating a list.
func TestEveryMigratorInThisPackageNamesItsRevisionEngine(t *testing.T) {
	c := qt.New(t)

	building := sourcesMentioning(c, "migrator.NewFSMigrator")
	naming := sourcesMentioning(c, "WithMigrationsEngine")

	c.Assert(len(building) > 0, qt.IsTrue,
		qt.Commentf("the walk found no migrator in this package, so it measured nothing"))

	var silent []string
	for _, file := range building {
		silent = appendUnlessNamedOrExempt(silent, file, naming)
	}

	c.Assert(silent, qt.HasLen, 0,
		qt.Commentf("these build a migrator without naming its revision engine: %v", silent))
}

// TestTheRevisionEngineWalkSeesTheKnownEntryPoints is the control on the walk.
//
// A walk that stopped reading files would compare two empty sets and pass. This
// names the three entry points that must be in the building set, and asserts
// the exemption still names a file that exists.
func TestTheRevisionEngineWalkSeesTheKnownEntryPoints(t *testing.T) {
	c := qt.New(t)

	building := sourcesMentioning(c, "migrator.NewFSMigrator")

	for _, file := range []string{"apply.go", "status.go", "down.go", "set.go"} {
		c.Assert(building, qt.Contains, file,
			qt.Commentf("%s builds a migrator but the walk did not see it", file))
	}
}

// sourcesMentioning returns the package's non-test Go files containing needle.
func sourcesMentioning(c *qt.C, needle string) []string {
	c.Helper()

	entries, err := os.ReadDir(".")
	c.Assert(err, qt.IsNil)

	var found []string
	for _, entry := range entries {
		name := entry.Name()
		if !isPackageSource(name) {
			continue
		}
		body, readErr := os.ReadFile(filepath.Clean(name))
		c.Assert(readErr, qt.IsNil)
		if strings.Contains(string(body), needle) {
			found = append(found, name)
		}
	}
	slices.Sort(found)
	return found
}

// isPackageSource reports whether a directory entry is a non-test Go source.
func isPackageSource(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

// appendUnlessNamedOrExempt records a file that neither names the engine nor
// carries a written-down reason not to.
func appendUnlessNamedOrExempt(silent []string, file string, naming []string) []string {
	if slices.Contains(naming, file) || slices.Contains(exemptFromNamingTheEngine, file) {
		return silent
	}
	return append(silent, file)
}
