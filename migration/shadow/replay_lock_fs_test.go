package shadow_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// minMigratorConstructions is the number of migrators package shadow builds
// today. Below it the guard has stopped finding them, and an empty count
// would pass whatever the code did.
const minMigratorConstructions = 5

// Every migrator package shadow builds replays into a claimed database, and
// every one goes through shadowdb.ReplayMigrator, which drops the migration
// lock. A replay that skips it hangs only on MySQL and MariaDB with the shadow
// on the target's server, where GET_LOCK names are server-wide, so only a live
// test on that layout would notice; this reads the source instead. Each
// function holds at least as many ReplayMigrator calls as migrator
// constructions.
func TestEveryShadowReplayDropsTheMigrationLock(t *testing.T) {
	c := qt.New(t)

	constructed, unwrapped := migratorConstructions(c, ".")

	c.Assert(constructed >= minMigratorConstructions, qt.IsTrue, qt.Commentf("found %d migrator constructions", constructed))
	c.Assert(unwrapped, qt.IsNil)
}

// migratorConstructions reads the non-test Go files of dir and returns how
// many migrators they build, and the functions that build more than they pass
// to shadowdb.ReplayMigrator.
func migratorConstructions(c *qt.C, dir string) (constructed int, unwrapped []string) {
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	files := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(files, filepath.Join(dir, name), nil, 0)
		c.Assert(err, qt.IsNil)
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			built, wrapped := countCalls(function.Body)
			constructed += built
			if wrapped < built {
				unwrapped = append(unwrapped, name+": "+function.Name.Name)
			}
		}
	}
	return constructed, unwrapped
}

// countCalls returns how many migrators body builds and how many
// shadowdb.ReplayMigrator calls it makes.
func countCalls(body *ast.BlockStmt) (built, wrapped int) {
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		switch pkg.Name + "." + selector.Sel.Name {
		case "migrator.NewMigrator", "migrator.NewFSMigrator":
			built++
		case "shadowdb.ReplayMigrator":
			wrapped++
		}
		return true
	})
	return built, wrapped
}
