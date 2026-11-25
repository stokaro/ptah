package secretmask_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// differsOnPurpose are the declarations this package and its copy in
// provider-sdk/internal/secretmask both hold with different source: the SDK's
// copy matches the letters of a secret in any ASCII case and takes no pieces of
// a password, so the functions that find and replace a secret differ, and so
// does the one that spells a secret.
var differsOnPurpose = []string{
	"Mask",
	"maskedWidth",
	"spelled",
}

// sharedDeclarationsFloor is the fewest declarations the two copies must share.
// A parse that stopped finding declarations would otherwise compare none and
// pass.
const sharedDeclarationsFloor = 19

// The provider SDK is a module of its own and cannot import this package, so
// it carries a copy. The shared corpus tests what both copies do with its
// rows; this test reads both files and holds every top-level declaration they
// both declare to the same source, but those [differsOnPurpose] names, so a
// change to one copy alone fails here even where no row of the corpus reaches
// it, and so does a new helper both copies declare with different bodies.
func TestTheProviderSDKCopyHoldsTheSameRule(t *testing.T) {
	c := qt.New(t)
	root := sourceDeclarations(c, "secretmask.go")
	sdk := sourceDeclarations(c, filepath.Join("..", "..", "provider-sdk", "internal", "secretmask", "secretmask.go"))
	shared := sharedNames(root, sdk)
	c.Assert(len(shared) >= sharedDeclarationsFloor, qt.IsTrue, qt.Commentf("%d shared declarations: %v", len(shared), shared))

	for _, name := range shared {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sdk[name] == root[name], qt.Equals, !slices.Contains(differsOnPurpose, name),
				qt.Commentf("root:\n%s\nsdk:\n%s", root[name], sdk[name]))
		})
	}
	for _, name := range differsOnPurpose {
		t.Run("declared in both: "+name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(shared, qt.Contains, name)
		})
	}
}

// sharedNames returns the names both declare, sorted.
func sharedNames(root, sdk map[string]string) []string {
	var shared []string
	for name := range root {
		if _, both := sdk[name]; both {
			shared = append(shared, name)
		}
	}
	slices.Sort(shared)
	return shared
}

// sourceDeclarations returns the source of each top-level declaration of the
// Go file at path, printed without its comments, by its name: a method by its
// receiver's type and its name, and each name of a value or a type by itself.
func sourceDeclarations(c *qt.C, path string) map[string]string {
	c.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
	c.Assert(err, qt.IsNil)
	declarations := make(map[string]string)
	render := func(node any) string {
		var out strings.Builder
		c.Assert(printer.Fprint(&out, files, node), qt.IsNil)
		return out.String()
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			declarations[receiverName(decl)+decl.Name.Name] = render(decl)
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				switch spec := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						declarations[name.Name] = render(spec)
					}
				case *ast.TypeSpec:
					declarations[spec.Name.Name] = render(spec)
				}
			}
		}
	}
	return declarations
}

// receiverName returns the type of the receiver of decl, rendered with a
// trailing ".", or "" for a function.
func receiverName(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return ""
	}
	typ := decl.Recv.List[0].Type
	if star, isPointer := typ.(*ast.StarExpr); isPointer {
		typ = star.X
	}
	if ident, isIdent := typ.(*ast.Ident); isIdent {
		return ident.Name + "."
	}
	return "?."
}
