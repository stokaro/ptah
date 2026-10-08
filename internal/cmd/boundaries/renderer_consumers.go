package main

import (
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
)

// These consumers receive rendering from their caller. Following every import
// prevents a helper from reintroducing implicit built-in selection. All roots
// must load, even when their dependency graph has no forbidden package.
var rendererConsumerRoots = []string{
	"internal/embedpg", "internal/genexprprobe", "migration/generator", "migration/importer",
	"migration/planner", "migration/safety", "migration/schemadiff", "migration/shadow",
}

func rendererConsumerImports(pkg *packages.Package) []finding {
	if !slices.Contains(rendererConsumerRoots, relative(pkg.PkgPath)) {
		return nil
	}
	return transitiveImportFindings(pkg, func(dependency *packages.Package) bool {
		local := relative(dependency.PkgPath)
		return local == "engine/builtin" || strings.HasPrefix(local, "engine/builtin/")
	})
}
