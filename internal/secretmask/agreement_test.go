package secretmask_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// agreedDeclarations are the declarations that make the rule, which this
// package and its copy in provider-sdk/internal/secretmask must both hold: the
// floors, the patterns of a credential's place, the characters that end a
// value, the names of a secret setting, and the functions that read them. The
// copies differ elsewhere on purpose: the SDK's matches the letters of a
// secret in any case, and takes no pieces of a password.
var agreedDeclarations = []string{
	"shortestFreeTextSecret",
	"longestSetting",
	"valueEnds",
	"settingBefore",
	"credentialBefore",
	"secretParameterWords",
	"unescapes",
	"standsAsCredential",
	"followsUser",
	"wordEnds",
	"wordRune",
	"IsSecretParameter",
	"comparableName",
}

// The provider SDK is a module of its own and cannot import this package, so
// it carries a copy. The shared corpus tests what both copies do with its
// rows; this test reads both files and holds each declaration of the rule to
// the same source, so a change to one copy alone fails here even where no row
// of the corpus reaches it.
func TestTheProviderSDKCopyHoldsTheSameRule(t *testing.T) {
	c := qt.New(t)
	root := sourceDeclarations(c, "secretmask.go")
	sdk := sourceDeclarations(c, filepath.Join("..", "..", "provider-sdk", "internal", "secretmask", "secretmask.go"))

	for _, name := range agreedDeclarations {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(root[name], qt.Not(qt.Equals), "", qt.Commentf("%s is not declared in this package", name))
			c.Assert(sdk[name], qt.Equals, root[name])
		})
	}
}

// sourceDeclarations returns the source of each top-level declaration of the
// Go file at path, by name, printed without its comments.
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
			declarations[decl.Name.Name] = render(decl)
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				if value, isValue := spec.(*ast.ValueSpec); isValue {
					for _, name := range value.Names {
						declarations[name.Name] = render(value)
					}
				}
			}
		}
	}
	return declarations
}
