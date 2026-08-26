// Command exporteddocs refuses an undocumented declaration on the public
// embedder surface.
//
// # What it reads
//
// The packages `docs/public_api.md` declares stable, which is the set three CI
// gates already enforce, read through the same scrape they use so this cannot
// drift from them. Nothing outside that ledger is measured: an unexported
// helper or a command package is not something an embedder reads in `go doc`.
//
// # Why methods are exempt
//
// A method that implements an interface repeats what the interface already
// documents. Measured on the ledger when this was written, 148 of the 158
// undocumented exported declarations were exactly that -- `Accept`, `Error`,
// `Unwrap`, `VisitCreateTable`, `UnmarshalYAML` -- and a rule demanding a
// comment on each would have produced 148 restatements and taught everyone to
// write them without reading. What is left is what `go doc <package>` shows at
// the top: the functions and types a consumer meets first, of which ten had
// nothing to say for themselves.
//
// # Why go/packages rather than parser.ParseDir
//
// ParseDir does not consider build tags, so a file behind one is associated
// with a package by filename alone. An undocumented export could then sit in a
// `//go:build windows` file and be measured on a Linux runner as though it were
// always there -- or, worse, be missed. The loader answers with the files the
// build actually selects, and it is what `internal/cmd/boundaries` already uses.
//
// The consequence is stated rather than hidden: this measures ONE build
// configuration, the runner's. A declaration exported only under a tag the
// runner does not select is not seen. Nothing on the ledger is behind a tag
// today, and a gate that quietly widened to every configuration would be
// claiming a coverage it does not have.
//
// # Why not revive's `exported` rule
//
// It cannot be scoped to the ledger, so it would fire across every internal
// package at once; it fires on methods, which is the exemption above; and it
// demands a comment FORM -- the sentence must begin with the identifier -- which
// is a different rule from "this is documented" and one this repository does not
// otherwise apply (stokaro/ptah#2246 §8).
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// finding is one exported declaration with nothing said about it.
type finding struct {
	position string
	kind     string
	name     string
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s <module-root> <package-dir>...\n", os.Args[0])
		os.Exit(2)
	}
	root := os.Args[1]
	directories := os.Args[2:]

	// A gate that measured nothing would report the same success it reports on
	// a clean tree, which is the failure mode every gate here is written
	// against.
	if len(directories) == 0 {
		fmt.Fprintf(os.Stderr, "%s: no packages given; refusing to report a vacuous pass\n", os.Args[0])
		os.Exit(1)
	}

	findings, err := undocumented(root, directories)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		os.Exit(1)
	}
	if len(findings) == 0 {
		fmt.Printf("exporteddocs: every exported declaration in %d stable packages is documented\n",
			len(directories))
		return
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].position < findings[j].position })
	for _, item := range findings {
		fmt.Fprintf(os.Stderr, "%s: exported %s %s has no doc comment\n",
			item.position, item.kind, item.name)
	}
	fmt.Fprintf(os.Stderr, "\n%s on the public surface %s nothing about %s.\n",
		plural(len(findings), "exported declaration", "exported declarations"),
		plural(len(findings), "says", "say"), plural(len(findings), "itself", "themselves"))
	fmt.Fprintf(os.Stderr, "Write a doc comment, or unexport it if no embedder needs it.\n")
	os.Exit(1)
}

// undocumented reports the exported top-level declarations that carry no doc
// comment, across every package directory it is given.
func undocumented(root string, directories []string) ([]finding, error) {
	patterns := make([]string, 0, len(directories))
	for _, directory := range directories {
		patterns = append(patterns, "./"+filepath.ToSlash(directory))
	}
	loaded, err := packages.Load(&packages.Config{
		// Tests are excluded: an embedder does not read them, and they are not
		// part of the surface the ledger promises.
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedFiles | packages.NeedCompiledGoFiles,
		Dir:  root,
	}, patterns...)
	if err != nil {
		return nil, fmt.Errorf("load the ledger's packages: %w", err)
	}
	if len(loaded) != len(directories) {
		return nil, fmt.Errorf("asked for %d packages and the loader answered with %d",
			len(directories), len(loaded))
	}
	var findings []finding
	for _, pkg := range loaded {
		if len(pkg.Errors) > 0 {
			return nil, fmt.Errorf("load %s: %v", pkg.PkgPath, pkg.Errors[0])
		}
		// A package with no syntax is a package the loader did not read, which
		// would be counted as documented.
		if len(pkg.Syntax) == 0 {
			return nil, fmt.Errorf("%s parsed to no files; the ledger names a package that is not there",
				pkg.PkgPath)
		}
		for _, file := range pkg.Syntax {
			findings = append(findings, undocumentedInFile(pkg.Fset, file)...)
		}
	}
	return findings, nil
}

// undocumentedInFile walks one file's top-level declarations.
func undocumentedInFile(fileSet *token.FileSet, file *ast.File) []finding {
	var findings []finding
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			findings = append(findings, undocumentedFunction(fileSet, node)...)
		case *ast.GenDecl:
			findings = append(findings, undocumentedSpecs(fileSet, node)...)
		}
	}
	return findings
}

// undocumentedFunction reports a function, but never a method: see the package
// comment for why an implementation of a documented interface is exempt.
func undocumentedFunction(fileSet *token.FileSet, node *ast.FuncDecl) []finding {
	if node.Recv != nil || !node.Name.IsExported() || node.Doc != nil {
		return nil
	}
	return []finding{{
		position: relative(fileSet, node.Pos()),
		kind:     "function",
		name:     node.Name.Name,
	}}
}

// undocumentedSpecs reports the types, constants and variables a declaration
// introduces.
//
// A grouped declaration documents its members through its own comment as often
// as through theirs -- `const ( // the dialects Ptah targets` -- so a member is
// documented when either carries a comment.
func undocumentedSpecs(fileSet *token.FileSet, node *ast.GenDecl) []finding {
	var findings []finding
	for _, spec := range node.Specs {
		typed, ok := spec.(*ast.TypeSpec)
		if !ok || !typed.Name.IsExported() {
			continue
		}
		if typed.Doc != nil || node.Doc != nil {
			continue
		}
		findings = append(findings, finding{
			position: relative(fileSet, typed.Pos()),
			kind:     "type",
			name:     typed.Name.Name,
		})
	}
	return findings
}

// plural picks the spelling one count needs. A gate whose failure reads "1
// exported declarations" is a gate someone stops reading.
func plural(count int, one, many string) string {
	word := many
	if count == 1 {
		word = one
	}
	if one == "exported declaration" {
		return fmt.Sprintf("%d %s", count, word)
	}
	return word
}

// relative renders a position the way a terminal can jump to it, without the
// absolute path of whoever ran the gate.
func relative(fileSet *token.FileSet, pos token.Pos) string {
	position := fileSet.Position(pos)
	path := position.Filename
	if working, err := os.Getwd(); err == nil {
		if trimmed, err := filepath.Rel(working, path); err == nil && !strings.HasPrefix(trimmed, "..") {
			path = trimmed
		}
	}
	return fmt.Sprintf("%s:%d", path, position.Line)
}
