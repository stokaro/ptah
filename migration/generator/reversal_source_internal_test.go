package generator

// White-box testing required: source traversal follows reversal helpers and shared capture
// constructors. Type information attributes nested assignments to their model.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"golang.org/x/tools/go/packages"
)

type reversalSource struct {
	function *ast.FuncDecl
	info     *types.Info
}

func fieldsNamedByReversalBuilders(c *qt.C) map[string]bool {
	c.Helper()
	root, err := exec.CommandContext(c.Context(), "git", "rev-parse", "--show-toplevel").Output()
	c.Assert(err, qt.IsNil)
	repository := strings.TrimSpace(string(root))
	command := exec.CommandContext(c.Context(), "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "migration/generator", "migration/schemadiff/difftypes")
	command.Dir = repository
	listing, err := command.Output()
	c.Assert(err, qt.IsNil)
	selected := make(map[string]bool)
	for path := range strings.SplitSeq(string(listing), "\x00") {
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			selected[filepath.Join(repository, filepath.FromSlash(path))] = true
		}
	}
	c.Assert(len(selected) > 20, qt.IsTrue)
	loaded, err := packages.Load(&packages.Config{Context: c.Context(), Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, ".", "ptah.run/migration/schemadiff/difftypes")
	c.Assert(err, qt.IsNil)
	functions := make(map[*types.Func]reversalSource)
	var roots []*types.Func
	for _, pkg := range loaded {
		c.Assert(pkg.Errors, qt.HasLen, 0)
		for _, file := range pkg.Syntax {
			if !selected[pkg.Fset.Position(file.Pos()).Filename] {
				continue
			}
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok {
					continue
				}
				object, ok := pkg.TypesInfo.Defs[function.Name].(*types.Func)
				if !ok {
					continue
				}
				functions[object] = reversalSource{function: function, info: pkg.TypesInfo}
				if pkg.PkgPath == "ptah.run/migration/generator" && (strings.HasPrefix(function.Name.Name, "reverse") || strings.HasPrefix(function.Name.Name, "prior")) {
					roots = append(roots, object)
				}
			}
		}
	}
	c.Assert(len(roots) > 20, qt.IsTrue)
	return reversalNamedFields(functions, roots)
}

func reversalNamedFields(functions map[*types.Func]reversalSource, roots []*types.Func) map[string]bool {
	named := make(map[string]bool)
	visited := make(map[*types.Func]bool)
	var walk func(*types.Func)
	walk = func(object *types.Func) {
		source, found := functions[object]
		if !found || visited[object] {
			return
		}
		visited[object] = true
		collectReversalFields(source, named, walk)
	}
	for _, root := range roots {
		walk(root)
	}
	return named
}

func collectReversalFields(source reversalSource, named map[string]bool, walk func(*types.Func)) {
	ast.Inspect(source.function.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.CompositeLit:
			element := reversalTypeName(source.info.TypeOf(node))
			if element == "" || len(node.Elts) == 0 {
				return true
			}
			named["type:"+element] = true
			for _, item := range node.Elts {
				if pair, ok := item.(*ast.KeyValueExpr); ok {
					if key, ok := pair.Key.(*ast.Ident); ok {
						named[element+"."+key.Name] = true
					}
				}
			}
		case *ast.SelectorExpr:
			selection := source.info.Selections[node]
			if selection != nil && selection.Kind() == types.FieldVal {
				element := reversalTypeName(selection.Recv())
				if element != "" {
					named[element+"."+node.Sel.Name] = true
				}
			}
		case *ast.Ident:
			if object, ok := source.info.Uses[node].(*types.Func); ok {
				walk(object)
			}
		}
		return true
	})
}

func reversalTypeName(value types.Type) string {
	if value == nil {
		return ""
	}
	value = types.Unalias(value)
	if pointer, ok := value.(*types.Pointer); ok {
		value = types.Unalias(pointer.Elem())
	}
	if named, ok := value.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "ptah.run/migration/schemadiff/difftypes" {
		return named.Obj().Name()
	}
	return ""
}

func TestReversalSourceCensusFollowsOnlyReachableTypedFields(t *testing.T) {
	for _, test := range []struct {
		name    string
		source  string
		field   string
		present bool
	}{
		{name: "nested assignment", source: `func reverse(v *SchemaDiff) { v.TablesModified[0].Current = 1 }`, field: "TableDiff.Current", present: true},
		{name: "omitted assignment", source: `func reverse(v *SchemaDiff) {}`, field: "TableDiff.Current"},
		{name: "same field on another type", source: `func reverse(v *Unrelated) { v.Current = 1 }`, field: "TableDiff.Current"},
		{name: "shared constructor", source: `func reverse() TableCreation { return capture() }; func capture() TableCreation { return TableCreation{OwnedObjects:7} }`, field: "TableCreation.OwnedObjects", present: true},
		{name: "unused constructor", source: `func reverse() {}; func capture() TableCreation { return TableCreation{OwnedObjects:7} }`, field: "TableCreation.OwnedObjects"},
		{name: "omitted constructor field", source: `func reverse() TableCreation { return capture() }; func capture() TableCreation { return TableCreation{Name:"table"} }`, field: "TableCreation.OwnedObjects"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			functions, roots := reversalSourceFixture(c, test.source)
			c.Assert(reversalNamedFields(functions, roots)[test.field], qt.Equals, test.present)
		})
	}
}

func reversalSourceFixture(c *qt.C, body string) (map[*types.Func]reversalSource, []*types.Func) {
	c.Helper()
	source := `package difftypes
 type SchemaDiff struct { TablesModified []TableDiff }
 type TableDiff struct { Current int }
 type Unrelated struct { Current int }
 type TableCreation struct { Name string; OwnedObjects int }
 ` + body
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, 0)
	c.Assert(err, qt.IsNil)
	info := &types.Info{Types: make(map[ast.Expr]types.TypeAndValue), Defs: make(map[*ast.Ident]types.Object), Uses: make(map[*ast.Ident]types.Object), Selections: make(map[*ast.SelectorExpr]*types.Selection)}
	configuration := &types.Config{}
	_, err = configuration.Check("ptah.run/migration/schemadiff/difftypes", fset, []*ast.File{file}, info)
	c.Assert(err, qt.IsNil)
	functions := make(map[*types.Func]reversalSource)
	var roots []*types.Func
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			object := info.Defs[function.Name].(*types.Func)
			functions[object] = reversalSource{function: function, info: info}
			if function.Name.Name == "reverse" {
				roots = append(roots, object)
			}
		}
	}
	return functions, roots
}
