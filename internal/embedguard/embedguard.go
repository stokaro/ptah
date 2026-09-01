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
	// Package is the import path that declares it.
	//
	// Carried because the name alone is not enough to tell a call from a
	// coincidence: a caller has to be able to REACH the declaration, and a file
	// that neither imports the package nor belongs to it cannot.
	Package string
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

	"Prune": "the outbox's only deleting statement, and nothing calls it: an outbox grows " +
		"for the whole life of a migration and is removed with the table at retirement. " +
		"That is a product gap rather than a dead declaration -- when to prune and whether " +
		"it is policy are decisions, and the consistency page describes a draining that does " +
		"not happen -- so it is tracked in stokaro/ptah#2690 rather than wired up here",

	// Surfaced the day the name check learned to ask whether a caller could
	// reach the declaration (stokaro/ptah#2682). `Check` is a common enough
	// method name that any `.Check(` in the module used to answer for it.
	"Check": "the policy half of the dual-write assessment, and it has no caller for the " +
		"reason embedcatchup/mode.go states at ModeDualWrite: DualWriteEvidence has no " +
		"producer in this build -- no verb, no table, no endpoint a writer could report " +
		"through -- so the mode is deliberately not selectable rather than selectable and " +
		"permanently unverifiable (stokaro/ptah#2632). The assessment is written and tested " +
		"and wants only a reporting surface",
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

	viaInterface, err := interfaceMethods(root)
	if err != nil {
		return nil, err
	}

	findings := make([]Finding, 0)
	for _, declaration := range declared {
		if called[declaration.Name][declaration.Package] {
			continue
		}
		// A name an interface declares is reachable from a package that never
		// imports the implementation -- that is what an interface is for. So
		// for those the reach question cannot be asked, and the older name-only
		// rule stands.
		//
		// Measured: `embedpg.Outbox.Since` is called by `embedengine`'s
		// catch-up loop through the `Changes` interface, and embedengine does
		// not import embedpg. Without this, the reach rule reported a method
		// the product calls on every catch-up -- a false positive, which is the
		// direction this package promises never to produce.
		if viaInterface[declaration.Name] && len(called[declaration.Name]) > 0 {
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
	module, err := modulePath(root)
	if err != nil {
		return nil, err
	}
	var declared []Finding
	err = walkGoFiles(filepath.Join(root, "internal"), func(path string, file *ast.File, fset *token.FileSet) {
		slashed := filepath.ToSlash(path)
		if !strings.Contains(slashed, "/embed") {
			return
		}
		// This package is not the vertical it guards. Its exported surface is
		// the gate's own entry points, whose only caller a gate can have is the
		// test that runs it, so scanning itself reports every one of them --
		// which the name-only check hid behind some other `.Scan(` in the
		// module until it learned to ask about reach (stokaro/ptah#2682).
		if strings.Contains(slashed, "/internal/embedguard/") {
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
			owner, pathErr := importPathOf(root, module, filepath.Dir(path))
			if pathErr != nil {
				continue
			}
			declared = append(declared, Finding{
				Name:    function.Name.Name,
				Package: owner,
				File:    filepath.ToSlash(relative),
				Line:    fset.Position(function.Name.Pos()).Line,
			})
		}
	})
	return declared, err
}

// calledNames collects, for every name a non-test file uses, the packages that
// file could have been naming.
//
// By name AND by reach, rather than by resolved symbol. The name alone was too
// coarse in one direction: any same-named identifier anywhere in the module
// counted as a use, so `embedplan.Plan.Runnable` read as called because
// cmd/atlas calls cobra's `Command.Runnable` -- an unrelated method on an
// unrelated type, in a package that has nothing to do with the inference
// vertical. The decision it carried was absent from the product and the guard
// was green (stokaro/ptah#2682, the cost measured in stokaro/ptah#2648).
//
// A caller has to be able to REACH what it names, so a use counts against a
// declaration only from a file that imports the declaring package or belongs to
// it. That is not type resolution and does not pretend to be: a same-named
// method on another type, called from a file that DOES import the package,
// still masks the declaration. The direction is unchanged -- a false negative
// stays possible and a false positive stays impossible -- and the set of
// coincidences that produce one is much smaller.
func calledNames(root string) (map[string]map[string]bool, error) {
	module, err := modulePath(root)
	if err != nil {
		return nil, err
	}
	called := make(map[string]map[string]bool)
	note := func(name string, reachable map[string]bool) {
		if called[name] == nil {
			called[name] = make(map[string]bool, len(reachable))
		}
		for path := range reachable {
			called[name][path] = true
		}
	}
	err = walkGoFiles(root, func(path string, file *ast.File, _ *token.FileSet) {
		reachable := reachableFrom(root, module, path, file)
		declaredHere := make(map[*ast.Ident]bool)
		for _, decl := range file.Decls {
			if function, isFunction := decl.(*ast.FuncDecl); isFunction {
				declaredHere[function.Name] = true
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				note(typed.Sel.Name, reachable)
			case *ast.Ident:
				if !declaredHere[typed] {
					note(typed.Name, reachable)
				}
			}
			return true
		})
	})
	return called, err
}

// reachableFrom is every package one file can name: the ones it imports, and
// its own.
//
// Its own is not an import and is the half that matters most here -- most calls
// to a package's declarations come from beside them.
func reachableFrom(root, module, path string, file *ast.File) map[string]bool {
	reachable := make(map[string]bool, len(file.Imports)+1)
	for _, spec := range file.Imports {
		reachable[strings.Trim(spec.Path.Value, `"`)] = true
	}
	if own, err := importPathOf(root, module, filepath.Dir(path)); err == nil {
		reachable[own] = true
	}
	return reachable
}

// importPathOf turns a directory into the import path the module gives it.
func importPathOf(root, module, dir string) (string, error) {
	relative, err := filepath.Rel(root, dir)
	if err != nil {
		return "", err
	}
	if relative == "." {
		return module, nil
	}
	return module + "/" + filepath.ToSlash(relative), nil
}

// modulePath reads the module's own import path out of go.mod.
//
// Read rather than assumed: the guard compares import paths, and a wrong prefix
// would make every declaration unreachable and every one of them a finding --
// which is the loud direction, but for a reason that has nothing to do with the
// product.
func modulePath(root string) (string, error) {
	source, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("read the module path: %w", err)
	}
	for line := range strings.SplitSeq(string(source), "\n") {
		if after, found := strings.CutPrefix(strings.TrimSpace(line), "module "); found {
			return strings.TrimSpace(after), nil
		}
	}
	return "", fmt.Errorf("no module directive in %s/go.mod", root)
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

// interfaceMethods is every method name an interface in this module declares.
//
// The module's own interfaces only. A dependency's interface is not scanned,
// and an interface here that embeds one is not followed, so a method
// implementing a third-party interface and named nowhere in this module is
// still reported. That is the loud direction, and the reason has to be written
// into Exempt rather than inferred.
func interfaceMethods(root string) (map[string]bool, error) {
	names := make(map[string]bool)
	err := walkGoFiles(root, func(_ string, file *ast.File, _ *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			declared, isInterface := node.(*ast.InterfaceType)
			if !isInterface {
				return true
			}
			for _, method := range declared.Methods.List {
				for _, name := range method.Names {
					names[name.Name] = true
				}
			}
			return true
		})
	})
	return names, err
}
