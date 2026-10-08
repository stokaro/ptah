package astrouteguard

import (
	"fmt"
	"go/types"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
)

// ExtensionKindFloor retains coverage for the payloads moved out of core/ast.
// It supplements NodeKindFloor; extracting a node must not shrink the combined
// corpus or replace its owner-specific fixtures with one envelope fixture.
const ExtensionKindFloor = 4

// ExtensionKind identifies a concrete owner type implementing ExtensionPayload.
type ExtensionKind struct {
	Package string
	Name    string
}

// ExtensionKinds type-checks tracked owner packages and enumerates every
// concrete payload, including types implementing the interface by embedding.
// Contract payloads must build on every target: excluded production files are
// errors, so build constraints cannot silently reduce this inventory.
func ExtensionKinds(root string) ([]ExtensionKind, error) {
	files, err := trackedFiles(root, "dialect/*.go", "feature/*.go")
	if err != nil {
		return nil, err
	}
	tracked := make(map[string]bool)
	patterns := []string{"ptah.run/core/ast"}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		tracked[filepath.Join(root, file)] = false
		patterns = append(patterns, "./"+filepath.ToSlash(filepath.Dir(file)))
	}
	slices.Sort(patterns)
	loaded, err := packages.Load(&packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedTypes | packages.NeedImports | packages.NeedDeps,
	}, slices.Compact(patterns)...)
	if err != nil {
		return nil, fmt.Errorf("load extension payloads: %w", err)
	}
	contract, err := payloadContract(loaded)
	if err != nil {
		return nil, err
	}
	var kinds []ExtensionKind
	for _, pkg := range loaded {
		if len(pkg.Errors) != 0 {
			return nil, fmt.Errorf("type-check extension payloads: %s", pkg.Errors[0])
		}
		for _, file := range pkg.CompiledGoFiles {
			if _, ok := tracked[file]; ok {
				tracked[file] = true
			}
		}
		kinds = append(kinds, concretePayloads(pkg, contract)...)
	}
	for _, file := range slices.Sorted(maps.Keys(tracked)) {
		if !tracked[file] {
			return nil, fmt.Errorf("extension contract file is excluded from type checking: %s", file)
		}
	}
	slices.SortFunc(kinds, func(a, b ExtensionKind) int {
		return strings.Compare(a.Package+"."+a.Name, b.Package+"."+b.Name)
	})
	return kinds, nil
}

func payloadContract(loaded []*packages.Package) (*types.Interface, error) {
	for _, pkg := range loaded {
		if pkg.PkgPath != "ptah.run/core/ast" || pkg.Types == nil {
			continue
		}
		object := pkg.Types.Scope().Lookup("ExtensionPayload")
		if object != nil {
			if contract, ok := object.Type().Underlying().(*types.Interface); ok {
				return contract.Complete(), nil
			}
		}
	}
	return nil, fmt.Errorf("extension payload contract was not loaded")
}

func concretePayloads(pkg *packages.Package, contract *types.Interface) []ExtensionKind {
	var result []ExtensionKind
	for _, name := range pkg.Types.Scope().Names() {
		object, ok := pkg.Types.Scope().Lookup(name).(*types.TypeName)
		if !ok || object.IsAlias() {
			continue
		}
		named, ok := object.Type().(*types.Named)
		if !ok {
			continue
		}
		if _, isInterface := named.Underlying().(*types.Interface); isInterface {
			continue
		}
		if types.Implements(named, contract) || types.Implements(types.NewPointer(named), contract) {
			result = append(result, ExtensionKind{Package: pkg.PkgPath, Name: name})
		}
	}
	return result
}
