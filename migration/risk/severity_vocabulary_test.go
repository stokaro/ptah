package risk_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// canonicalPackage is the scale itself, which necessarily declares the type.
const canonicalPackage = "migration/risk"

// separateVocabularies are the packages that declare a type named Severity
// which is deliberately not this scale, each with the reason it is separate.
//
// A package added here is a decision someone made on purpose. A package that
// declares one and is absent is the thing this file exists to catch: the scale
// called itself shared by every producer while five packages had quietly grown
// their own, and nothing said so (stokaro/ptah#2395).
var separateVocabularies = map[string]string{
	"internal/adoptpreflight": "scores a preflight outcome (ok, action, refuse), not a level",
	"internal/embedverify":    "scores a gate outcome (blocking, advisory), not a level",
}

// TestSeverity_EveryOtherVocabularyIsRecorded makes the doc comment enforceable.
//
// The claim is that findings share one scale. Prose is read by whoever is
// already thinking about the vocabulary, which is never the person adding a
// type in a hurry, and a `type Severity string` in a new package costs nothing
// to write and shows up in published JSON.
func TestSeverity_EveryOtherVocabularyIsRecorded(t *testing.T) {
	c := qt.New(t)

	own := packagesDeclaringTheirOwnSeverity(c)

	c.Assert(own, qt.Contains, canonicalPackage,
		qt.Commentf("the walk did not find the scale itself, so it proves nothing"))
	for _, pkg := range withoutCanonical(own) {
		c.Assert(separateVocabularies[pkg], qt.Not(qt.Equals), "",
			qt.Commentf(
				"%s declares its own `type Severity string`.\n"+
					"Alias migration/risk.Severity, or record it in separateVocabularies "+
					"with the reason it is a different vocabulary.", pkg))
	}
}

// TestSeverity_TheRecordedExceptionsStillDeclareOne is the control.
//
// A recorded package that stopped declaring its own type leaves an exception
// standing for nothing, and the next reader takes the list as current.
func TestSeverity_TheRecordedExceptionsStillDeclareOne(t *testing.T) {
	c := qt.New(t)

	own := packagesDeclaringTheirOwnSeverity(c)

	for pkg := range separateVocabularies {
		c.Assert(own, qt.Contains, pkg,
			qt.Commentf("%s is recorded as a separate vocabulary and no longer declares one", pkg))
	}
}

// packagesDeclaringTheirOwnSeverity walks the repository and returns the
// directories holding a `type Severity` that is not an alias.
//
// It reads the tree rather than a list of packages: a list would be the
// hand-written inventory this test exists to replace.
func packagesDeclaringTheirOwnSeverity(c *qt.C) []string {
	c.Helper()
	root := repositoryRoot(c)
	var found []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return skipUninterestingDir(root, path, entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if declaresOwnSeverity(c, path) {
			rel, relErr := filepath.Rel(root, filepath.Dir(path))
			if relErr != nil {
				return relErr
			}
			found = appendUnique(found, filepath.ToSlash(rel))
		}
		return nil
	})
	c.Assert(err, qt.IsNil)
	return found
}

// skipUninterestingDir keeps the walk inside this module's own source.
func skipUninterestingDir(root, path string, entry fs.DirEntry) error {
	name := entry.Name()
	if path == root {
		return nil
	}
	if strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "testdata" {
		return filepath.SkipDir
	}
	return nil
}

// declaresOwnSeverity reports whether a file declares `type Severity` as its
// own definition rather than an alias.
func declaresOwnSeverity(c *qt.C, path string) bool {
	c.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return false
	}
	declared := false
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok {
			return true
		}
		if spec.Name.Name == "Severity" && !spec.Assign.IsValid() {
			declared = true
		}
		return true
	})
	return declared
}

// repositoryRoot walks up from this package to the directory holding go.mod.
func repositoryRoot(c *qt.C) string {
	c.Helper()
	dir, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	for range 8 {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	c.Fatalf("no go.mod above %s", dir)
	return ""
}

// appendUnique adds a value the slice does not already carry.
func appendUnique(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

// withoutCanonical drops the scale's own package from a walk result.
func withoutCanonical(packages []string) []string {
	kept := make([]string, 0, len(packages))
	for _, pkg := range packages {
		if pkg != canonicalPackage {
			kept = append(kept, pkg)
		}
	}
	return kept
}
