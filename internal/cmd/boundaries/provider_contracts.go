package main

import (
	"slices"

	"golang.org/x/tools/go/packages"
)

// Each root must be loaded even if it has no forbidden dependency. Otherwise
// deleting or renaming a contract could remove it from the guard's corpus.
var providerContractRoots = []string{"core/objectidentity", "core/renderer", "core/schemaext", "engine"}

func providerContractImports(pkg *packages.Package) []finding {
	self := relative(pkg.PkgPath)
	if !slices.Contains(providerContractRoots, self) {
		return nil
	}
	var found []finding
	visited := make(map[string]bool)
	var visit func(*packages.Package)
	visit = func(dependency *packages.Package) {
		if visited[dependency.PkgPath] {
			return
		}
		visited[dependency.PkgPath] = true
		local := relative(dependency.PkgPath)
		implementation := local == "dbschema" || anyUnder(
			"engine/builtin", "dialect/", "feature/", "internal/dbschema/", "internal/planner/", "migration/",
		)(local)
		external := dependency.Module != nil && dependency.Module.Path != modulePath
		if implementation || external {
			found = append(found, finding{Package: self, Detail: "links " + dependency.PkgPath})
		}
		for _, imported := range dependency.Imports {
			visit(imported)
		}
	}
	visit(pkg)
	return found
}
