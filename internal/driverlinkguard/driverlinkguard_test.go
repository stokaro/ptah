package driverlinkguard_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"golang.org/x/tools/go/packages"
)

// purePaths are the entry points that turn Go structs into SQL. Each one is
// reachable by an embedder who never connects to a database, and each one is a
// separate root because they do not import one another: `core/goschema` parses
// the struct tags, `core/renderer` writes the DDL, and `migration/planner`
// orders it.
var purePaths = []struct {
	name string
	pkg  string
}{
	{name: "goschema parses the struct tags", pkg: "ptah.run/core/goschema"},
	{name: "renderer writes the DDL", pkg: "ptah.run/core/renderer"},
	{name: "planner orders it", pkg: "ptah.run/migration/planner"},
}

// driverModules are the database drivers this module requires. Every one of
// them belongs to the connection layer under `dbschema`, and none of them has
// anything to say about rendering.
var driverModules = []string{
	"github.com/ClickHouse/clickhouse-go/v2",
	"github.com/go-sql-driver/mysql",
	"github.com/jackc/pgx/v5",
	"github.com/microsoft/go-mssqldb",
	"github.com/sijms/go-ora/v3",
	"github.com/tursodatabase/libsql-client-go",
	"modernc.org/sqlite",
}

// permittedModules is the entire module set a pure path may link, this module
// included.
//
// It is an allowlist rather than a count because a count says nothing about
// which module arrived. `golang.org/x/sys` is here as the platform layer under
// the YAML reader rather than as a dependency any of these packages names.
//
// Growing this list is a decision, not a formality: a module that reaches
// `core/renderer` reaches every embedder who renders SQL. If the new dependency
// belongs to the connection layer, the import that pulled it in is the defect.
var permittedModules = []string{
	"ptah.run",
	"go.yaml.in/yaml/v3",
	"golang.org/x/sys",
}

// TestPureRenderingPathsLinkNoDatabaseDriver is the invariant: none of the
// three roots reaches a driver module.
//
// The failure prints the import chain rather than the module alone, because the
// edge that has to be cut is nearly never in the package that was changed --
// the one this test was written for ran core/renderer -> internal/schemaselection
// -> internal/atlasurl, and only the last of those three had ever heard of a
// driver.
func TestPureRenderingPathsLinkNoDatabaseDriver(t *testing.T) {
	for _, test := range purePaths {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			chain := chainToModule(loadRoot(c, test.pkg), func(module string) bool {
				return slices.Contains(driverModules, module)
			})
			c.Assert(chain, qt.IsNil, qt.Commentf(
				"%s links a database driver through:\n\t%s\n"+
					"Rendering SQL from Go structs must not pull the connection layer. "+
					"Cut the edge, or move the symbol the pure side needs into a leaf package.",
				test.pkg, strings.Join(chain, "\n\t-> "),
			))
		})
	}
}

// TestPureRenderingPathsLinkOnlyPermittedModules is the same invariant asked
// from the other side, so a driver added to go.mod after this test was written
// is still caught.
func TestPureRenderingPathsLinkOnlyPermittedModules(t *testing.T) {
	for _, test := range purePaths {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			chain := chainToModule(loadRoot(c, test.pkg), func(module string) bool {
				return !slices.Contains(permittedModules, module)
			})
			c.Assert(chain, qt.IsNil, qt.Commentf(
				"%s links a module outside the permitted set through:\n\t%s\n"+
					"Permitted: %s.\n"+
					"Widen the list only when the new dependency belongs on the rendering side.",
				test.pkg, strings.Join(chain, "\n\t-> "), strings.Join(permittedModules, ", "),
			))
		})
	}
}

// loadRoot resolves one root and its whole dependency graph. It asks for the
// module of every package because the invariant is about modules: a package
// path can be renamed, and the driver would arrive under a new name.
func loadRoot(c *qt.C, root string) *packages.Package {
	c.Helper()

	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps | packages.NeedModule,
	}, root)
	c.Assert(err, qt.IsNil)
	c.Assert(loaded, qt.HasLen, 1)
	c.Assert(packages.PrintErrors(loaded), qt.Equals, 0,
		qt.Commentf("loading %s reported errors", root))

	return loaded[0]
}

// chainToModule walks the import graph breadth-first and returns the shortest
// import chain from root to a package whose module the predicate accepts, or
// nil when no package in the graph does.
//
// Breadth-first is what makes the failure readable: the shortest chain is the
// one with the fewest packages to argue about, and the edge worth cutting is
// usually its first hop.
func chainToModule(root *packages.Package, want func(module string) bool) []string {
	parent := map[string]*packages.Package{root.PkgPath: nil}
	queue := []*packages.Package{root}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.Module != nil && want(current.Module.Path) {
			return chainTo(current, parent)
		}

		// Sorting keeps the reported chain stable across runs: Imports is a map,
		// and an unsorted walk would name a different equal-length chain each
		// time, which reads as a flapping test rather than one finding.
		for _, path := range sortedKeys(current.Imports) {
			imported := current.Imports[path]
			if _, seen := parent[imported.PkgPath]; seen {
				continue
			}
			parent[imported.PkgPath] = current
			queue = append(queue, imported)
		}
	}
	return nil
}

// chainTo rebuilds the walked path, root first.
func chainTo(target *packages.Package, parent map[string]*packages.Package) []string {
	var chain []string
	for step := target; step != nil; step = parent[step.PkgPath] {
		chain = append(chain, step.PkgPath)
	}
	slices.Reverse(chain)
	return chain
}

func sortedKeys(imports map[string]*packages.Package) []string {
	paths := make([]string, 0, len(imports))
	for path := range imports {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	return paths
}
