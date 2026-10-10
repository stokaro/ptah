package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
)

// neutralRoots are the neutral layer of the stokaro/ptah#4140 design (section
// 5): the authored and observed models, the provider contracts, the extension
// and diff envelopes, the runtime that assembles supplied providers, and the
// comparison and planning orchestration. Every package at or under a root
// belongs to the layer, so a package added below one is held to the rule
// without an edit here. engine/builtin is the composition of bundled owners and
// sits above the layer.
var neutralRoots = []string{"catalog", "core", "engine", "migration/planner", "migration/schemadiff"}

// requiredNeutralPackages must load for the rule to mean anything. Each is a
// package the design names; a rename that took one out of the corpus would
// otherwise report nothing about it.
var requiredNeutralPackages = []string{"catalog", "core/ast", "core/schemaext", "core/schemamodel", "engine", "migration/planner", "migration/schemadiff", "migration/schemadiff/difftypes"}

// neutral reports whether a module-relative package path is in the neutral
// layer.
func neutral(rel string) bool {
	if rel == "engine/builtin" || strings.HasPrefix(rel, "engine/builtin/") {
		return false
	}
	return slices.ContainsFunc(neutralRoots, func(root string) bool {
		return rel == root || strings.HasPrefix(rel, root+"/")
	})
}

// ownerFamily is the concrete owner a module-relative package belongs to:
// dialect/<name>, feature/<name> or engine/builtin. A package that belongs to
// none reports false.
func ownerFamily(rel string) (string, bool) {
	if rel == "engine/builtin" || strings.HasPrefix(rel, "engine/builtin/") {
		return "engine/builtin", true
	}
	for _, prefix := range []string{"dialect/", "feature/"} {
		rest, found := strings.CutPrefix(rel, prefix)
		if !found || rest == "" {
			continue
		}
		name, _, _ := strings.Cut(rest, "/")
		return prefix + name, true
	}
	return "", false
}

// neutralOwnerLinks reports each concrete owner a neutral package links,
// directly or through any chain of imports, once per owner.
//
// A finding names the owner rather than the package of it that was reached.
// Moving a type between two packages of one owner is not new debt, while a
// neutral package that starts linking another owner is.
func neutralOwnerLinks(pkg *packages.Package) []finding {
	self := relative(pkg.PkgPath)
	if self == "" || !neutral(self) {
		return nil
	}
	reached := make(map[string]string)
	visited := make(map[string]bool)
	var visit func(*packages.Package)
	visit = func(dependency *packages.Package) {
		if visited[dependency.PkgPath] {
			return
		}
		visited[dependency.PkgPath] = true
		local := relative(dependency.PkgPath)
		if family, owned := ownerFamily(local); owned {
			if witness, seen := reached[family]; !seen || local < witness {
				reached[family] = local
			}
		}
		for _, imported := range dependency.Imports {
			visit(imported)
		}
	}
	visit(pkg)

	found := make([]finding, 0, len(reached))
	for _, family := range slices.Sorted(maps.Keys(reached)) {
		found = append(found, finding{Package: self, Detail: fmt.Sprintf("links %s (%s)", family, reached[family])})
	}
	return found
}
