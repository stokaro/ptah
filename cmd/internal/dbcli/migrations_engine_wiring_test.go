package dbcli_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// commandTree is the directory the guard walks. It is walked rather than
// listed, so a verb added later is measured without anyone updating a list.
const commandTree = "../.."

// TestEveryPackageRegisteringTheEngineFlagAlsoAppliesIt is the guard on a flag
// that is accepted and discarded.
//
// A registered flag that nothing reads is worse than an absent one: `--help`
// advertises it, an operator sets it, and the command does the other thing in
// silence. It shipped that way in cmd/internal/migratemaint, where
// `migrations edit | rm | rebase` offered --migrations-engine while the guard
// they run first created the revision table with the dialect default -- and
// because that DDL is CREATE TABLE IF NOT EXISTS, no later verb that does honor
// the flag can repair the table it left behind (stokaro/ptah#2234).
//
// The rule is one a package can satisfy two ways, because both shapes exist in
// the tree: call the migrator option, or set the field an options struct
// carries to something that does.
func TestEveryPackageRegisteringTheEngineFlagAlsoAppliesIt(t *testing.T) {
	c := qt.New(t)

	registering := packagesMentioning(c, "RegisterMigrationsEngineFlag")
	applying := packagesMentioning(c, "WithMigrationsEngine", "MigrationsEngine:")
	// The package that declares the registrar is not a caller of it. Found by
	// what it declares rather than by name, so moving the registrar moves the
	// exemption with it.
	declaring := packagesMentioning(c, "func RegisterMigrationsEngineFlag")

	c.Assert(len(registering) > 0, qt.IsTrue,
		qt.Commentf("the walk found no package registering the flag, so it measured nothing"))
	c.Assert(declaring, qt.HasLen, 1,
		qt.Commentf("the registrar is declared in %v, so the exemption no longer names one place", declaring))

	var unapplied []string
	for _, pkg := range registering {
		unapplied = appendIfMissing(unapplied, pkg, append(applying, declaring...))
	}

	c.Assert(unapplied, qt.HasLen, 0,
		qt.Commentf("these packages register --migrations-engine and never apply it: %v", unapplied))
}

// TestTheEngineFlagGuardSeesTheVerbsThatCarryIt is the control on the walk.
//
// A walk that stopped finding files would compare two empty sets and pass,
// which is how a source-derived guard quietly stops guarding. This names verbs
// that must be in the registering set, so a walk reading nothing reddens.
func TestTheEngineFlagGuardSeesTheVerbsThatCarryIt(t *testing.T) {
	c := qt.New(t)

	registering := packagesMentioning(c, "RegisterMigrationsEngineFlag")

	for _, verb := range []string{"migrateup", "migratedown", "migratestatus", "internal/migratemaint"} {
		c.Assert(registering, qt.Contains, verb,
			qt.Commentf("%s registers the flag but the walk did not see it", verb))
	}
}

// packagesMentioning returns the command packages whose non-test Go sources
// contain any of the given strings, named relative to the command tree.
func packagesMentioning(c *qt.C, needles ...string) []string {
	c.Helper()

	seen := make(map[string]bool)
	for _, path := range commandSources(c) {
		body, err := os.ReadFile(filepath.Clean(path))
		c.Assert(err, qt.IsNil)
		if containsAny(string(body), needles) {
			seen[packageOf(path)] = true
		}
	}

	packages := make([]string, 0, len(seen))
	for pkg := range seen {
		packages = append(packages, pkg)
	}
	slices.Sort(packages)
	return packages
}

// commandSources lists every non-test Go source under the command tree.
//
// The walk collects paths and the read happens after it, rather than inside the
// callback: a filesystem operation on a path a walk is still producing is the
// symlink race gosec's G122 names.
func commandSources(c *qt.C) []string {
	c.Helper()

	var paths []string
	err := filepath.WalkDir(commandTree, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !isCommandSource(entry, path) {
			return err
		}
		paths = append(paths, path)
		return nil
	})
	c.Assert(err, qt.IsNil)
	slices.Sort(paths)
	return paths
}

// isCommandSource reports whether a walked entry is a non-test Go source file.
func isCommandSource(entry fs.DirEntry, path string) bool {
	return entry != nil &&
		!entry.IsDir() &&
		strings.HasSuffix(path, ".go") &&
		!strings.HasSuffix(path, "_test.go")
}

// containsAny reports whether the body holds any of the needles.
func containsAny(body string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(body, needle) {
			return true
		}
	}
	return false
}

// packageOf names a file's package the way this guard reports it: the directory
// under cmd/, with the tree prefix removed.
func packageOf(path string) string {
	dir := filepath.ToSlash(filepath.Dir(path))
	return strings.TrimPrefix(strings.TrimPrefix(dir, filepath.ToSlash(commandTree)), "/")
}

// appendIfMissing adds the package to the list when it is not in applying.
func appendIfMissing(unapplied []string, pkg string, applying []string) []string {
	if slices.Contains(applying, pkg) {
		return unapplied
	}
	return append(unapplied, pkg)
}
