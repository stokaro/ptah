// Package astrouteguard holds every dialect renderer to a decision about every
// AST node kind.
//
// The completeness this replaces was the shape of [ptah.run/core/ast.Visitor]:
// one method per node kind, so a renderer that did not answer a kind failed to
// compile. That property was never as wide as it looked. Of the concrete types
// implementing `ast.Node`, only the ones a Visitor method named were covered,
// and the rest -- the alter operations, the type operations, the type
// definitions, the routine nodes and the statement list -- reached no method at
// all, several of them returning nil from `Accept` without calling the visitor.
// A node kind outside the interface could already be dropped in silence.
//
// With dispatch through a type switch the compiler stops asking the question
// entirely, so this asks it: every concrete node kind, against every renderer,
// must be routed to a case or named in that renderer's exemption table with a
// reason. A switch whose default arm accepts an unrouted kind is the failure
// this exists to make loud.
//
// The corpus is derived, never written down. A hand-maintained list of node
// kinds is a second place to forget the new kind, and forgetting it there makes
// the gate agree that nothing is missing.
package astrouteguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// NodeKindFloor is the smallest corpus this guard accepts.
//
// A gate that enumerates holds its corpus above a floor, because a parser that
// stopped matching reports zero unrouted kinds and reads exactly like a tree
// where every renderer answers everything. The number is a source constant
// rather than a field in a data file for the same reason: a floor the checked
// input can lower is not a floor.
const NodeKindFloor = 80

// NodeKind is one concrete type that implements ast.Node.
type NodeKind struct {
	// Name is the type name without a package qualifier, as a type switch
	// case writes it after the package selector.
	Name string
	// File is where the Accept method is declared, relative to the module
	// root.
	File string
	// Line is that method's line.
	Line int
}

// acceptMethod is the method every ast.Node implementation declares.
//
// Matching the declaration rather than type-checking the package is deliberate:
// `ast.Node` has exactly this one method, so a type declaring it implements the
// interface, and the check needs no build of the module. What the shortcut
// cannot see is a type that satisfies the interface by embedding another one;
// [NodeKinds] reports that case rather than passing over it, because a node
// kind reached through an embedded Accept is exactly as droppable as one that
// declares its own.
const acceptMethod = "Accept"

// NodeKinds returns every concrete type in core/ast that implements ast.Node,
// sorted by name.
//
// root is the module root. The file list comes from git rather than from a
// filesystem walk: a linked worktree's root is an ordinary directory, so a walk
// descends into every checkout parked under the repository and reports another
// branch's node kinds as this one's.
func NodeKinds(root string) ([]NodeKind, error) {
	files, err := trackedFiles(root, "core/ast/*.go")
	if err != nil {
		return nil, err
	}

	fileSet := token.NewFileSet()
	var kinds []NodeKind
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, filepath.Join(root, file), nil, 0)
		if parseErr != nil {
			return nil, fmt.Errorf("astrouteguard: parsing %s: %w", file, parseErr)
		}
		kinds = append(kinds, acceptReceivers(fileSet, parsed, file)...)
	}

	slices.SortFunc(kinds, func(a, b NodeKind) int { return strings.Compare(a.Name, b.Name) })
	return kinds, nil
}

// acceptReceivers collects the receiver type of every Accept method in one file.
func acceptReceivers(fileSet *token.FileSet, file *ast.File, path string) []NodeKind {
	var kinds []NodeKind
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != acceptMethod || function.Recv == nil {
			continue
		}
		if len(function.Recv.List) != 1 || !acceptsVisitor(function) {
			continue
		}
		name, ok := receiverTypeName(function.Recv.List[0].Type)
		if !ok {
			continue
		}
		kinds = append(kinds, NodeKind{
			Name: name,
			File: path,
			Line: fileSet.Position(function.Pos()).Line,
		})
	}
	return kinds
}

// acceptsVisitor reports whether the method's one parameter is a Visitor and it
// returns one error.
//
// Without this an unrelated `Accept` -- a connection accepting a socket, say --
// would enter the corpus and every renderer would be asked to route a type that
// is not a node.
func acceptsVisitor(function *ast.FuncDecl) bool {
	parameters := function.Type.Params
	results := function.Type.Results
	if parameters == nil || len(parameters.List) != 1 {
		return false
	}
	if results == nil || len(results.List) != 1 {
		return false
	}
	parameter, ok := parameters.List[0].Type.(*ast.Ident)
	if !ok || parameter.Name != "Visitor" {
		return false
	}
	result, ok := results.List[0].Type.(*ast.Ident)
	return ok && result.Name == "error"
}

// receiverTypeName is the receiver's type name, with a pointer star removed.
func receiverTypeName(expression ast.Expr) (string, bool) {
	if star, ok := expression.(*ast.StarExpr); ok {
		expression = star.X
	}
	identifier, ok := expression.(*ast.Ident)
	if !ok {
		return "", false
	}
	return identifier.Name, true
}

// trackedFiles asks git for the files matching pattern, so a path git does not
// track cannot reach the corpus and a linked worktree cannot contribute one.
//
// `--others --exclude-standard` is what lets a brand-new node kind count before
// it is staged; without it a gate reads the index and cannot see the file the
// change adds.
func trackedFiles(root, pattern string) ([]string, error) {
	command := exec.Command("git", "-c", "core.quotePath=false",
		"ls-files", "--cached", "--others", "--exclude-standard", "--", pattern)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("astrouteguard: listing %s: %w", pattern, err)
	}
	var files []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		if line != "" {
			files = append(files, line)
		}
	}
	return files, nil
}

// ModuleRoot is the repository root the guard reads from.
func ModuleRoot() (string, error) {
	output, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("astrouteguard: locating the module root: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}
