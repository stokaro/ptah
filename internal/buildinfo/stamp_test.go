package buildinfo_test

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/buildinfo"
)

// stampedFiles are the two places that stamp version metadata into a binary.
var stampedFiles = []string{"../../Makefile", "../../.goreleaser.yaml"}

// stampPattern finds the linker target of a -X assignment aimed at this
// package, whatever import path it names.
var stampPattern = regexp.MustCompile(`-X\s+(\S*buildinfo)\.(\w+)=`)

// TestStampTargetsNameThisPackage is what makes moving this package safe.
//
// The build stamps Version, Commit and Date through the linker, and the linker
// takes the import path as a string. A package moved without its stamps keeps
// compiling, keeps passing every other test, and quietly reports "dev" forever:
// the failure appears only in a released binary, as a version nobody can trace.
// Reading the real import path back out of the package rather than writing it
// down again means this test cannot drift from the thing it guards.
func TestStampTargetsNameThisPackage(t *testing.T) {
	c := qt.New(t)
	importPath := reflect.TypeFor[buildinfo.Info]().PkgPath()
	c.Assert(importPath, qt.Not(qt.Equals), "")

	found := 0
	for _, file := range stampedFiles {
		t.Run(strings.TrimPrefix(file, "../../"), func(t *testing.T) {
			c := qt.New(t)
			contents, err := os.ReadFile(file)
			c.Assert(err, qt.IsNil)

			matches := stampPattern.FindAllStringSubmatch(string(contents), -1)
			c.Assert(len(matches) > 0, qt.IsTrue,
				qt.Commentf("%s stamps nothing into this package, so a released binary reports dev", file))
			for _, match := range matches {
				c.Assert(match[1], qt.Equals, importPath,
					qt.Commentf("%s stamps %s.%s, which no longer exists", file, match[1], match[2]))
			}
			found += len(matches)
		})
	}
	c.Assert(found > 0, qt.IsTrue)
}

// TestStampedVariablesExist keeps the linker targets pointing at variables the
// package actually declares. A -X assignment to a name that is gone is accepted
// by the linker in silence.
func TestStampedVariablesExist(t *testing.T) {
	c := qt.New(t)
	declared := map[string]bool{"Version": true, "Commit": true, "Date": true}

	for _, file := range stampedFiles {
		contents, err := os.ReadFile(file)
		c.Assert(err, qt.IsNil)
		for _, match := range stampPattern.FindAllStringSubmatch(string(contents), -1) {
			c.Assert(declared[match[2]], qt.IsTrue,
				qt.Commentf("%s stamps %s, which this package does not declare", file, match[2]))
		}
	}
}
