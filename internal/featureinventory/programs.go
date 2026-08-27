package featureinventory

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// Program is one runnable `main` package in this repository.
type Program struct {
	// Dir is the package directory relative to the repository root, so
	// `go run ./<Dir>` runs it.
	Dir string
	// Drives names the command tree the program dispatches against, empty when
	// it drives none of them.
	Drives Tree
}

// Programs discovers every `main` package in the repository and works out which
// of them dispatch against a censused command tree.
//
// The list is discovered rather than written down, and the reason is a finding
// rather than a principle. `cmd/main.go` is three lines -- `package main`,
// an import of cmd/root, and `root.Execute()` -- so `./cmd` is a fourth
// complete copy of the native CLI. It has been in the tree since the initial
// commit, it is in no release and no gate, and the Phase 1 audit, its critic
// and every previous inventory all missed it while listing the binaries by
// hand. DRY_RUN.md invokes it twelve times.
//
// Files come from `git ls-files` rather than a filesystem walk: this checkout's
// parent directory holds other worktrees of the same repository, and a walk
// descends into them.
func Programs(repoRoot string) ([]Program, error) {
	tracked, err := trackedFiles(repoRoot, "*.go")
	if err != nil {
		return nil, err
	}

	drives := make(map[string]Tree)
	dirs := make(map[string]bool)
	for _, rel := range tracked {
		if strings.HasSuffix(rel, "_test.go") {
			continue
		}
		pkg, tree, err := inspectMain(filepath.Join(repoRoot, filepath.FromSlash(rel)))
		if err != nil {
			return nil, err
		}
		if !pkg {
			continue
		}
		dir := path.Dir(rel)
		dirs[dir] = true
		if tree != "" {
			drives[dir] = tree
		}
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("featureinventory: no `package main` found in any tracked Go file; the program census measured nothing")
	}

	programs := make([]Program, 0, len(dirs))
	for dir := range dirs {
		programs = append(programs, Program{Dir: dir, Drives: drives[dir]})
	}
	sort.Slice(programs, func(i, j int) bool { return programs[i].Dir < programs[j].Dir })
	return programs, nil
}

// inspectMain reports whether one file is `package main`, and which command tree
// its `main` function dispatches against.
//
// The dispatch test is the call, not the import. internal/cmd/agentsurface
// imports cmd/root exactly as cmd/ptah does, and calls NewRootCommand to walk
// the tree rather than Execute to run it -- so an import-based rule would make
// `go run ./internal/cmd/agentsurface --database-safe` read as a native ptah
// invocation with two commands that do not exist.
//
// The file is parsed rather than searched. A comment showing a caller how to
// dispatch looks exactly like a caller dispatching, which is the false positive
// docs/architecture_boundaries.md records having been measured twice.
func inspectMain(absPath string) (isMain bool, drives Tree, err error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, absPath, nil, parser.SkipObjectResolution)
	if err != nil {
		return false, "", fmt.Errorf("featureinventory: parsing %s: %w", absPath, err)
	}
	if file.Name == nil || file.Name.Name != "main" {
		return false, "", nil
	}

	// The local name each root package was imported under, so a renamed import
	// is followed rather than missed.
	locals := make(map[string]Tree)
	for _, spec := range file.Imports {
		importPath := strings.Trim(spec.Path.Value, `"`)
		var tree Tree
		switch importPath {
		case "go.5x5.cz/ptah/cmd/root":
			tree = TreeNative
		case "go.5x5.cz/ptah/cmd/atlas":
			tree = TreeCompat
		default:
			continue
		}
		name := path.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		locals[name] = tree
	}
	if len(locals) == 0 {
		return true, "", nil
	}

	// dispatchers are the calls that hand the process's own arguments to a
	// tree. Constructing a tree is not dispatching against it.
	dispatchers := map[Tree]map[string]bool{
		TreeNative: {"Execute": true, "ExecuteCommand": true},
		TreeCompat: {"NewCompatCommand": true, "NewCompatCommandWithPolicy": true},
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		tree, ok := locals[ident.Name]
		if !ok {
			return true
		}
		if dispatchers[tree][sel.Sel.Name] {
			// A binary that builds the compat tree and hands it to
			// root.ExecuteCommand drives the compat tree; the compat answer
			// wins because it is the more specific of the two.
			if drives == "" || tree == TreeCompat {
				drives = tree
			}
		}
		return true
	})
	return true, drives, nil
}

// Launcher is one spelling documentation uses to start a program.
type Launcher struct {
	// Prefix is the literal words a documented line begins with.
	Prefix string
	// Tree is the command tree the rest of the line addresses, empty when the
	// program drives none.
	Tree Tree
	// Program is the package directory the spelling runs.
	Program string
}

// Launchers returns every documented spelling of every program, longest first.
//
// Longest first is load-bearing rather than tidy. `go run ./cmd` is a prefix of
// `go run ./cmd/integration-test`, and matching the short one first attributes
// an integration-suite invocation to the native tree, where `--scenarios` is a
// flag that does not exist. Three lines were misread that way while this was
// being measured.
func Launchers(programs []Program) []Launcher {
	var out []Launcher
	for _, program := range programs {
		base := path.Base(program.Dir)
		spellings := []string{"go run ./" + program.Dir, "go run " + program.Dir}
		// A program that drives a tree is also invoked by its installed name.
		// `cmd` is deliberately excluded from this half: its base name is not a
		// program name anybody types, and accepting it would read every
		// documented `cmd ...` line as an invocation.
		if program.Drives != "" && base != "cmd" {
			spellings = append(spellings,
				base, "./"+base, "bin/"+base, "./bin/"+base, "$(BIN)/"+base)
		}
		for _, spelling := range spellings {
			out = append(out, Launcher{Prefix: spelling, Tree: program.Drives, Program: program.Dir})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].Prefix) != len(out[j].Prefix) {
			return len(out[i].Prefix) > len(out[j].Prefix)
		}
		return out[i].Prefix < out[j].Prefix
	})
	return out
}

// trackedFiles lists repository files matching a pathspec, from the index.
func trackedFiles(repoRoot string, patterns ...string) ([]string, error) {
	args := append([]string{"-C", repoRoot, "ls-files", "-z", "--"}, patterns...)
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("featureinventory: git ls-files %v: %w", patterns, err)
	}
	var files []string
	for name := range strings.SplitSeq(string(out), "\x00") {
		if name != "" {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("featureinventory: git ls-files %v matched nothing; refusing to report on an empty file set", patterns)
	}
	sort.Strings(files)
	return files, nil
}

// RepoRoot resolves the repository this package is checked out in.
func RepoRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("featureinventory: locating the repository root: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
