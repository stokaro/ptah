package postgres

// White-box testing required: the set the default constructor builds is a
// package-local literal, and the capabilities this package consults are read
// from its own source. Both are unexported, and the property is about the
// relationship between them.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// TestDefaultWriterCapabilitiesAnswerEveryKeyThisPackageConsults is the gate the
// target-facts invariant was missing (stokaro/ptah#2315).
//
// A capability set answers a key or it does not, and [capability.Capabilities.Has]
// reports false for both. So a set that lists the keys its code happens to
// consult is a claim that was true when it was written, and the code drifts:
// stokaro/ptah#1811 is the recorded instance, where a key added to the query's
// gating was not added to this set and the cleanup silently dropped its
// extension-owned-routine filter for every caller of the default constructor.
//
// The fix is not to enumerate the keys again here -- that is a second copy of
// the same claim. It is to read the keys this package CONSULTS out of its own
// source and require the set to have an answer for each. A key whose absence is
// deliberate is written `false`, which is a decision; leaving it out is silence,
// which reads as the same thing and is not.
func TestDefaultWriterCapabilitiesAnswerEveryKeyThisPackageConsults(t *testing.T) {
	c := qt.New(t)

	consulted := capabilitiesConsultedBy(c, ".")
	c.Assert(len(consulted) > 0, qt.IsTrue,
		qt.Commentf("a scan that finds nothing would pass while measuring nothing"))

	caps := NewPostgreSQLWriterForRunner(nil, "public").caps

	unanswered := keysWithoutAnAnswer(caps, consulted)

	c.Assert(unanswered, qt.HasLen, 0,
		qt.Commentf("these keys are consulted here and unanswered by the default set, "+
			"so Has reports false for them without anyone having decided that: %v", unanswered))
}

// capabilitiesConsultedBy reads every capability.X the WRITER's code names.
//
// It reads identifiers rather than taking a list, because a list is the thing
// this test exists to make unnecessary.
//
// The package holds two independent consumers with two different sets: the
// reader, whose default constructor takes a whole preset, and the writer, whose
// default constructor builds the set this test is about. Attribution is by
// receiver -- a function that mentions `r.caps` is reading the reader's set, so
// its keys are not the writer's obligation. That is a proxy rather than data
// flow, and a writer helper that took a set from somewhere else would escape
// it; what it does catch is the drift stokaro/ptah#1811 recorded, where the
// query's gating grew a key and the set beside it did not.
//
// Keys named inside the set literal itself are included deliberately: they are
// answered by construction, so they cost nothing, and excluding them would mean
// deciding which literal is the set -- a rule that would need updating the next
// time the file moves.
func capabilitiesConsultedBy(c *qt.C, dir string) []capability.Capability {
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)

	known := make(map[string]capability.Capability, len(capability.All()))
	for _, key := range capability.All() {
		known[capabilityConstantName(key)] = key
	}

	seen := make(map[capability.Capability]struct{})
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		c.Assert(err, qt.IsNil, qt.Commentf("parsing %s", name))
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if readsTheReadersSet(function) {
				continue
			}
			collectCapabilityIdents(function, known, seen)
		}
	}

	consulted := slices.Collect(maps.Keys(seen))
	slices.Sort(consulted)
	return consulted
}

// keysWithoutAnAnswer names the keys a set neither affirms nor denies.
func keysWithoutAnAnswer(caps capability.Capabilities, keys []capability.Capability) []string {
	var unanswered []string
	for _, key := range keys {
		if caps.Established(key) {
			continue
		}
		unanswered = append(unanswered, string(key))
	}
	return unanswered
}

// readsTheReadersSet reports whether a function reads `r.caps`, which is the
// reader's set and not the writer's.
func readsTheReadersSet(function *ast.FuncDecl) bool {
	found := false
	ast.Inspect(function, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "caps" {
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if ok && receiver.Name == "r" {
			found = true
		}
		return true
	})
	return found
}

// collectCapabilityIdents records every capability.X a function names.
func collectCapabilityIdents(
	function *ast.FuncDecl,
	known map[string]capability.Capability,
	seen map[capability.Capability]struct{},
) {
	ast.Inspect(function, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "capability" {
			return true
		}
		if key, ok := known[selector.Sel.Name]; ok {
			seen[key] = struct{}{}
		}
		return true
	})
}

// capabilityConstantName is the Go constant name for a capability's value.
//
// The registry answers values (`drop_constraint_if_exists`) and the source
// names constants (`DropConstraintIfExists`), so one has to be derived from the
// other. Deriving the name from the value keeps this in step with a key added
// to the registry, which is the direction that matters.
func capabilityConstantName(key capability.Capability) string {
	parts := strings.Split(string(key), "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, "")
}
