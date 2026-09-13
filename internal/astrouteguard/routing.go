package astrouteguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// RendererFloor is the smallest number of dialect renderers this guard accepts.
//
// A glob that stopped matching finds no renderers, and a gate that checked no
// renderer reports no unrouted kind -- which reads exactly like a tree where
// every renderer routes everything.
const RendererFloor = 8

// dialectsDirectory holds one package per renderer.
const dialectsDirectory = "core/renderer/internal/dialects"

// Renderer is one dialect renderer and the node kinds its dispatcher names.
type Renderer struct {
	// Package is the directory holding the renderer, relative to the module
	// root, spelled with forward slashes on every platform.
	Package string
	// Routed are the node kind names the VisitNode switch lists as cases.
	Routed []string
	// DelegatesTo is the package the default arm forwards to, empty when the
	// default arm does not delegate.
	//
	// It is the field's declared package rather than the field's name: a
	// forward is only an answer if the renderer it reaches has one, and a name
	// says nothing about which renderer that is.
	//
	// A renderer that forwards its default arm has made a decision about every
	// kind it does not name: the kind is the other renderer's to answer. The
	// guard follows the edge rather than counting the forward as an answer,
	// because a forward to a renderer that does not route the kind either is
	// two renderers agreeing to drop it.
	DelegatesTo string
}

// Renderers reads every dialect renderer's VisitNode dispatcher.
func Renderers(root string) ([]Renderer, error) {
	files, err := trackedFiles(root, dialectsDirectory+"/*/*.go")
	if err != nil {
		return nil, err
	}

	byPackage := make(map[string][]string)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		// A git path is slash-separated on every platform, so the directory
		// comes from path rather than filepath: filepath.Dir answers
		// `core\renderer\...` on Windows, and a Package spelled that way
		// matches nothing a caller compares it against.
		directory := path.Dir(file)
		byPackage[directory] = append(byPackage[directory], file)
	}

	var renderers []Renderer
	for directory, packageFiles := range byPackage {
		renderer, found, readErr := readDispatcher(root, directory, packageFiles)
		if readErr != nil {
			return nil, readErr
		}
		if !found {
			continue
		}
		renderers = append(renderers, renderer)
	}

	slices.SortFunc(renderers, func(a, b Renderer) int {
		return strings.Compare(a.Package, b.Package)
	})
	return renderers, nil
}

// readDispatcher finds the VisitNode method in one package and reads its switch.
func readDispatcher(root, directory string, files []string) (Renderer, bool, error) {
	fileSet := token.NewFileSet()
	for _, file := range files {
		parsed, err := parser.ParseFile(fileSet, filepath.Join(root, file), nil, 0)
		if err != nil {
			return Renderer{}, false, fmt.Errorf("astrouteguard: parsing %s: %w", file, err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Name.Name != "VisitNode" || function.Recv == nil {
				continue
			}
			routed, field := readSwitch(function)
			return Renderer{
				Package:     directory,
				Routed:      routed,
				DelegatesTo: delegationPackage(parsed, function, field),
			}, true, nil
		}
	}
	return Renderer{}, false, nil
}

// readSwitch collects the case type names and the default arm's delegation.
func readSwitch(function *ast.FuncDecl) (routed []string, delegatesTo string) {
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switchStatement, ok := node.(*ast.TypeSwitchStmt)
		if !ok {
			return true
		}
		for _, statement := range switchStatement.Body.List {
			clause, ok := statement.(*ast.CaseClause)
			if !ok {
				continue
			}
			if clause.List == nil {
				delegatesTo = delegationTarget(clause.Body)
				continue
			}
			for _, expression := range clause.List {
				if name, ok := caseTypeName(expression); ok {
					routed = append(routed, name)
				}
			}
		}
		return true
	})
	slices.Sort(routed)
	return slices.Compact(routed), delegatesTo
}

// caseTypeName is the node type a case names, without its package qualifier.
func caseTypeName(expression ast.Expr) (string, bool) {
	if star, ok := expression.(*ast.StarExpr); ok {
		expression = star.X
	}
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	return selector.Sel.Name, true
}

// delegationTarget reports the receiver field a default arm forwards through,
// as `field.VisitNode`, or the empty string when the arm answers for itself.
func delegationTarget(body []ast.Stmt) string {
	target := ""
	for _, statement := range body {
		ast.Inspect(statement, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "VisitNode" {
				return true
			}
			if inner, ok := selector.X.(*ast.SelectorExpr); ok {
				target = inner.Sel.Name
			}
			return true
		})
	}
	return target
}

// Unrouted reports the node kinds a renderer's dispatcher does not name.
//
// A renderer whose default arm delegates is asked only about the kinds it names
// itself; the kinds it forwards are the target's to answer, and the target is
// checked in its own right. What this cannot express is a forward to a renderer
// that does not route the kind either, so the caller compares every renderer
// against the same corpus rather than trusting one edge.
func Unrouted(kinds []NodeKind, renderer Renderer) []string {
	routed := make(map[string]bool, len(renderer.Routed))
	for _, name := range renderer.Routed {
		routed[name] = true
	}

	var missing []string
	for _, kind := range kinds {
		if !routed[kind.Name] {
			missing = append(missing, kind.Name)
		}
	}
	return missing
}

// delegationPackage resolves the receiver field a default arm forwards through
// to the package that declares its type.
//
// The renderer type is in the same file as its VisitNode in every dialect here,
// so the struct is found by name rather than by loading the package. A field
// whose type is not qualified by a package is a same-package value, which is
// not a delegation to another renderer and answers nothing.
func delegationPackage(file *ast.File, function *ast.FuncDecl, field string) string {
	if field == "" {
		return ""
	}
	receiver, ok := receiverTypeName(function.Recv.List[0].Type)
	if !ok {
		return ""
	}
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range generic.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != receiver {
				continue
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			if name := fieldPackage(structure, field); name != "" {
				return name
			}
		}
	}
	return ""
}

// fieldPackage is the package qualifier on the named field's type.
func fieldPackage(structure *ast.StructType, field string) string {
	for _, declared := range structure.Fields.List {
		for _, name := range declared.Names {
			if name.Name != field {
				continue
			}
			expression := declared.Type
			if star, ok := expression.(*ast.StarExpr); ok {
				expression = star.X
			}
			selector, ok := expression.(*ast.SelectorExpr)
			if !ok {
				return ""
			}
			if qualifier, ok := selector.X.(*ast.Ident); ok {
				return qualifier.Name
			}
		}
	}
	return ""
}
