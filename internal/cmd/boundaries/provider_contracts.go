package main

import (
	"slices"

	"golang.org/x/tools/go/packages"
)

// Each root must be loaded even if it has no forbidden dependency. Otherwise
// deleting or renaming a contract could remove it from the guard's corpus.
var providerContractRoots = []string{"core/featureplan", "core/objectidentity", "core/plangraph", "core/renderer", "core/schemacapture", "core/schemaext", "core/schemamodel", "core/schemaprojection", "core/schemavalidation", "engine"}

func providerContractImports(pkg *packages.Package) []finding {
	self := relative(pkg.PkgPath)
	if !slices.Contains(providerContractRoots, self) {
		return nil
	}
	return transitiveImportFindings(pkg, func(dependency *packages.Package) bool {
		local := relative(dependency.PkgPath)
		implementation := local == "dbschema" || anyUnder(
			"engine/builtin", "dialect/", "feature/", "internal/dbschema/", "internal/planner/", "migration/",
		)(local)
		external := dependency.Module != nil && dependency.Module.Path != modulePath
		return implementation || external
	})
}

func transitiveImportFindings(pkg *packages.Package, forbidden func(*packages.Package) bool) []finding {
	var found []finding
	visited := make(map[string]bool)
	var visit func(*packages.Package)
	visit = func(dependency *packages.Package) {
		if visited[dependency.PkgPath] {
			return
		}
		visited[dependency.PkgPath] = true
		if forbidden(dependency) {
			found = append(found, finding{Package: relative(pkg.PkgPath), Detail: "links " + dependency.PkgPath})
		}
		for _, imported := range dependency.Imports {
			visit(imported)
		}
	}
	visit(pkg)
	return found
}
