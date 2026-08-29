package atlas

// White-box testing required: this file reads the flag declarations of this
// package's own source, which is where the mark it is about is written. There
// is no exported surface that reports whether a registered flag opted out of
// its PTAH_<FLAG> environment twin -- registerAtlasFlags turns an
// atlasargs.Flag into a cobra flag, and cobra has nowhere to carry it.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// envMarkedFlags names the flags on this surface that opt out of their
// PTAH_<FLAG> environment twin, with the collision that makes the opt-out
// correct.
//
// It is empty, and that is the whole state of the tree: the one flag that
// carried the mark was `migrate down --skip-checks`, and it stopped needing it
// when the rollback capability became real rather than refused. Both verbs now
// mean "skip the pre-migration checks", so one PTAH_SKIP_CHECKS serving both is
// the right answer rather than a collision.
//
// A reason rather than a bare list, for the reason appendEnvArgs states: the
// mark is not the default for a waiver. Setting PTAH_TO_TAG is a request for a
// capability Ptah lacks and the loud refusal is correct; only a name another
// verb has repurposed for a different capability earns the opt-out.
var envMarkedFlags = make(map[string]string)

// TestNoFlagCarriesTheEnvironmentMarkUnexplained keeps the paragraph above
// appendEnvArgs from becoming a count nobody rechecks.
//
// It read "on this surface exactly one does" while zero did, which is the shape
// stokaro/ptah#2496 was filed about: a comment stating a fact about the tree
// that the tree stopped having. Whoever marks a flag has to name it here and
// say which name it collides with, and the constructors keep their reasoning
// for the case rather than being deleted for having no instance today.
func TestNoFlagCarriesTheEnvironmentMarkUnexplained(t *testing.T) {
	c := qt.New(t)

	marked, err := environmentMarkedFlagNames()
	c.Assert(err, qt.IsNil)

	for _, name := range marked {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				envMarkedFlags[name],
				qt.Not(qt.Equals),
				"",
				qt.Commentf("--%s opts out of PTAH_%s; record here which verb repurposed the name, "+
					"and say so in the comment above appendEnvArgs", name, name),
			)
		})
	}
	c.Assert(marked, qt.HasLen, len(envMarkedFlags),
		qt.Commentf("marked in source: %v; recorded here: %v", marked, envMarkedFlags))
}

// environmentMarkedFlagNames returns the flag name every Explicit... constructor
// in this package is called with.
//
// It reads the source rather than the built tree because the mark does not
// survive registration: cobra carries no field for it. The two constructors are
// the only way to set it -- atlasargs.Flag.EnvDisabled is assigned nowhere else
// -- so naming them is naming the mark.
func environmentMarkedFlagNames() ([]string, error) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		return nil, err
	}
	constructors := map[string]struct{}{
		"ExplicitNativeBool": {}, "ExplicitUnsupportedBoolReason": {},
	}
	var names []string
	fset := token.NewFileSet()
	for _, path := range paths {
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil, parseErr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall || len(call.Args) == 0 {
				return true
			}
			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			if _, marks := constructors[selector.Sel.Name]; !marks {
				return true
			}
			literal, isLiteral := call.Args[0].(*ast.BasicLit)
			if !isLiteral {
				return true
			}
			names = append(names, literal.Value[1:len(literal.Value)-1])
			return true
		})
	}
	return names, nil
}
