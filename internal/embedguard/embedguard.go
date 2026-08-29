// Package embedguard reports a decision the inference vertical makes nowhere.
//
// Twice in one epic a function carried a rule, carried its own tests, and had
// no caller outside them. `embedrun.ResolveWrite` held the row-level write rules
// -- a write never crosses generations, a stale answer does not win, a tombstone
// survives a late update -- and the write path rendered an unconditional UPDATE
// (stokaro/ptah#2391). `embedrun.Run.Advance` held the phase machine, and
// `status` reported whatever `prepare` wrote, forever (stokaro/ptah#2441).
//
// Neither was caught by a linter. `unused` counts a test as a use, which is
// exactly what these had: full coverage of behavior that was not in effect, so
// the suite was green and the rule was absent.
//
// This finds the shape. An exported function or method declared under
// internal/embed... and called from no non-test file in the module is reported,
// unless it is named in Exempt with a reason.
package embedguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Finding is one declaration nothing outside a test calls.
type Finding struct {
	// Name is the function or method name, as a caller would write it.
	Name string
	// File is where it is declared, relative to the module root.
	File string
	// Line is the declaration's line.
	Line int
}

// Exempt names the declarations that may have no non-test caller, and why.
//
// A reason rather than a bare list: the whole failure this package exists for
// was a rule nobody was applying, and an exemption with no reason is the same
// thing one level up. Each entry says what makes it legitimate, or which issue
// tracks the gap it stands for.
var Exempt = map[string]string{
	"NewMemory": "an in-memory store that exists for tests to drive the engine without a " +
		"server; a production caller would be the defect",

	// The shape this package is named for, found by running it. Tracked in
	"Undetailed": "a self-check rather than a decision: it reports the facts a plan built " +
		"that owe an explanation and give none, and the only caller a self-check can have " +
		"is a test. embedreport's facts_internal_test.go runs it over the real assembly",
}

// Scan reports every exported declaration under root's internal/embed... that
// no non-test file in root calls.
func Scan(root string) ([]Finding, error) {
	declared, err := Declarations(root)
	if err != nil {
		return nil, err
	}
	called, err := calledNames(root)
	if err != nil {
		return nil, err
	}

	findings := make([]Finding, 0)
	for _, declaration := range declared {
		if called[declaration.Name] {
			continue
		}
		if _, exempt := Exempt[declaration.Name]; exempt {
			continue
		}
		findings = append(findings, declaration)
	}
	return findings, nil
}

// Declarations lists the exported functions and methods the inference packages
// declare.
//
// Exported so the gate can assert it reached a corpus. A scan that walked the
// wrong directory reports zero findings and reads exactly like a clean tree.
func Declarations(root string) ([]Finding, error) {
	var declared []Finding
	err := walkGoFiles(filepath.Join(root, "internal"), func(path string, file *ast.File, fset *token.FileSet) {
		if !strings.Contains(filepath.ToSlash(path), "/embed") {
			return
		}
		for _, decl := range file.Decls {
			function, isFunction := decl.(*ast.FuncDecl)
			if !isFunction || !function.Name.IsExported() {
				continue
			}
			relative, relErr := filepath.Rel(root, path)
			if relErr != nil {
				relative = path
			}
			declared = append(declared, Finding{
				Name: function.Name.Name,
				File: filepath.ToSlash(relative),
				Line: fset.Position(function.Name.Pos()).Line,
			})
		}
	})
	return declared, err
}

// calledNames collects every name a non-test file uses in a call or as a value.
//
// By name rather than by resolved symbol, which is coarse in one direction
// only: a name shared with something else reads as called and is not reported.
// That makes a false negative possible and a false positive impossible, which
// is the right way round for a gate somebody has to act on.
func calledNames(root string) (map[string]bool, error) {
	called := make(map[string]bool)
	err := walkGoFiles(root, func(path string, file *ast.File, _ *token.FileSet) {
		declaredHere := make(map[*ast.Ident]bool)
		for _, decl := range file.Decls {
			if function, isFunction := decl.(*ast.FuncDecl); isFunction {
				declaredHere[function.Name] = true
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				called[typed.Sel.Name] = true
			case *ast.Ident:
				if !declaredHere[typed] {
					called[typed.Name] = true
				}
			}
			return true
		})
	})
	return called, err
}

// walkGoFiles parses every non-test Go file under a directory.
//
// Vendored and generated trees are skipped by directory name rather than by
// content, because a caller in one of them is not a caller this repository
// maintains.
func walkGoFiles(root string, visit func(string, *ast.File, *token.FileSet)) error {
	fset := token.NewFileSet()
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor", "dist", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		visit(path, file, fset)
		return nil
	})
}
