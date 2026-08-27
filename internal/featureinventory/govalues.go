package featureinventory

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// goPackage is one Go package's declared string values, read from its source.
//
// The lists this package compares the inventory against are read out of the
// declarations that own them rather than out of the help text beside them. That
// is not a preference. `ptah schema inspect --format` writes its four accepted
// values three times -- in the flag's usage, in the `case` list that accepts
// them and in the refusal that rejects everything else -- and nothing joins the
// three, so a check that read the usage string would agree with a usage string
// that had drifted from the switch. `internal/schemaload.supportedExtensions`
// carries the same lesson in its own comment: the set was spelled by hand in
// five places, and adding `.dbml` reached three of them.
//
// Reading the source is parsing, not searching. A doc comment showing a caller
// how to build such a list looks exactly like the list, which is the false
// positive docs/architecture_boundaries.md records having measured twice.
type goPackage struct {
	dir    string
	consts map[string]string
	// constType records the named type each constant was declared with, so a
	// set declared as `const ( FormatAtlas Format = "atlas" ... )` can be read
	// back as a set rather than by guessing at a name prefix.
	constType map[string]string
	// constOrder preserves declaration order, which is the order a generated
	// document should render.
	constOrder []string
	vars       map[string]*ast.ValueSpec
}

// loadGoPackage parses one package's tracked, non-test Go files.
func loadGoPackage(repoRoot, pkgDir string) (*goPackage, error) {
	tracked, err := trackedFiles(repoRoot, pkgDir+"/*.go")
	if err != nil {
		return nil, err
	}
	pkg := &goPackage{
		dir:       pkgDir,
		consts:    make(map[string]string),
		constType: make(map[string]string),
		vars:      make(map[string]*ast.ValueSpec),
	}
	fset := token.NewFileSet()
	parsed := 0
	for _, rel := range tracked {
		// Only this directory, never a subpackage: `git ls-files cmd/schema/*.go`
		// is already one level, but a pathspec is not a promise.
		if path.Dir(rel) != pkgDir || strings.HasSuffix(rel, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(repoRoot, filepath.FromSlash(rel)), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("featureinventory: parsing %s: %w", rel, err)
		}
		parsed++
		pkg.collect(file)
	}
	if parsed == 0 {
		return nil, fmt.Errorf("featureinventory: %s has no tracked non-test Go files; a list read from it would be empty", pkgDir)
	}
	return pkg, nil
}

// collect records every string constant and every variable declaration.
func (p *goPackage) collect(file *ast.File) {
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		p.collectDecl(gen)
	}
}

// collectDecl records one declaration group.
func (p *goPackage) collectDecl(gen *ast.GenDecl) {
	// A const block's type carries to the specs below it, which is how
	// `const ( A Format = "a" \n B = "b" )` is written.
	typeName := ""
	for _, spec := range gen.Specs {
		value, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		if gen.Tok == token.VAR {
			for _, name := range value.Names {
				p.vars[name.Name] = value
			}
			continue
		}
		if gen.Tok != token.CONST {
			continue
		}
		if ident, ok := typeIdent(value); ok {
			typeName = ident
		}
		p.collectConsts(value, typeName)
	}
}

// collectConsts records the string constants of one spec.
func (p *goPackage) collectConsts(value *ast.ValueSpec, typeName string) {
	for index, name := range value.Names {
		if index >= len(value.Values) {
			continue
		}
		literal, ok := stringLiteral(value.Values[index])
		if !ok {
			continue
		}
		p.consts[name.Name] = literal
		p.constType[name.Name] = typeName
		p.constOrder = append(p.constOrder, name.Name)
	}
}

// typeIdent reads a spec's declared type name, when it has one.
func typeIdent(value *ast.ValueSpec) (string, bool) {
	if value.Type == nil {
		return "", false
	}
	ident, ok := value.Type.(*ast.Ident)
	if !ok {
		return "", false
	}
	return ident.Name, true
}

// stringLiteral reads a basic string literal.
func stringLiteral(expr ast.Expr) (string, bool) {
	basic, ok := expr.(*ast.BasicLit)
	if !ok || basic.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(basic.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

// SliceVar reads a `[]string`-shaped variable, resolving identifier elements
// through the package's own constants.
func (p *goPackage) SliceVar(name string) ([]string, error) {
	spec, ok := p.vars[name]
	if !ok || len(spec.Values) == 0 {
		return nil, fmt.Errorf("featureinventory: %s declares no variable %q; the list this check reads has moved or been renamed", p.dir, name)
	}
	composite, ok := spec.Values[0].(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("featureinventory: %s's %q is not a composite literal", p.dir, name)
	}
	values := make([]string, 0, len(composite.Elts))
	for _, element := range composite.Elts {
		if literal, ok := stringLiteral(element); ok {
			values = append(values, literal)
			continue
		}
		ident, ok := element.(*ast.Ident)
		if !ok {
			return nil, fmt.Errorf("featureinventory: %s's %q holds an element this reader cannot resolve", p.dir, name)
		}
		literal, ok := p.consts[ident.Name]
		if !ok {
			return nil, fmt.Errorf("featureinventory: %s's %q names the constant %s, which this package does not declare as a string", p.dir, name, ident.Name)
		}
		values = append(values, literal)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("featureinventory: %s's %q resolved to no values at all", p.dir, name)
	}
	return values, nil
}

// ConstsOfType reads every string constant declared with a named type.
func (p *goPackage) ConstsOfType(typeName string) ([]string, error) {
	var values []string
	for _, name := range p.constOrder {
		if p.constType[name] == typeName {
			values = append(values, p.consts[name])
		}
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("featureinventory: %s declares no string constants of type %q; the set this check reads has moved", p.dir, typeName)
	}
	return values, nil
}

// ConstsNamed reads the named string constants, in declaration order.
func (p *goPackage) ConstsNamed(names ...string) ([]string, error) {
	var values []string
	for _, name := range names {
		value, ok := p.consts[name]
		if !ok {
			return nil, fmt.Errorf("featureinventory: %s declares no string constant %q", p.dir, name)
		}
		values = append(values, value)
	}
	return values, nil
}

// UntypedStringConsts reads every string constant declared without a named
// type, in declaration order.
func (p *goPackage) UntypedStringConsts() ([]string, error) {
	var values []string
	for _, name := range p.constOrder {
		if p.constType[name] == "" {
			values = append(values, p.consts[name])
		}
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("featureinventory: %s declares no untyped string constants", p.dir)
	}
	return values, nil
}

// sortedUnique folds a list to a sorted set.
func sortedUnique(values []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
