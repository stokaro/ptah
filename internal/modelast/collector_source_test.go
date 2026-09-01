package modelast_test

import (
	goast "go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCollectDatabaseRemainsACompatibilitySeam(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	c.Assert(collectDatabaseCallers(c), qt.DeepEquals, []string{"atlascompat/atlascompat.go"})
}

func collectDatabaseCallers(c *qt.C) []string {
	root := filepath.Clean(filepath.Join("..", ".."))
	var callers []string

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		packageName := ""
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if importPath != "go.5x5.cz/ptah/internal/modelast" {
				continue
			}
			packageName = "modelast"
			if spec.Name != nil {
				packageName = spec.Name.Name
			}
		}
		if packageName == "" {
			return nil
		}

		file, err = parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		goast.Inspect(file, func(node goast.Node) bool {
			selector, ok := node.(*goast.SelectorExpr)
			if !ok || selector.Sel.Name != "CollectDatabase" {
				return true
			}
			identifier, ok := selector.X.(*goast.Ident)
			if ok && identifier.Name == packageName {
				relative, relErr := filepath.Rel(root, path)
				c.Assert(relErr, qt.IsNil)
				callers = append(callers, filepath.ToSlash(relative))
			}
			return true
		})
		return nil
	})
	c.Assert(err, qt.IsNil)
	return callers
}
