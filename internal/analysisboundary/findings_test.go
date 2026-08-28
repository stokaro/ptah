package analysisboundary_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// findingModels are the shapes an analysis result is published in, each with
// what publishes it.
//
// There are four, they do not share a type, and three of them are decoded by
// something outside this repository. Which one survives a unification is a
// decision about what a consumer of the other three is promised, and it is not
// made here (stokaro/ptah#2395).
//
// What is made here is the sprawl visible and bounded. A fifth shape added
// without a line in this map fails, so the decision keeps the same subject it
// had when it was written down rather than growing while nobody looked.
var findingModels = map[string]string{
	"internal/sqllint.Finding":        "`ptah sql lint --format json` and the SARIF renderer",
	"migration/lint.Finding":          "`ptah migrations lint` in every format, and the compat surface",
	"internal/agentgate.Diagnostic":   "the agent gate result, governed by ADR 0002, 0005 and 0007",
	"internal/schemavalidate.Problem": "`ptah schema validate`",
}

// findingTrees are the packages whose result types this map governs.
var findingTrees = []string{
	"internal/sqllint",
	"migration/lint",
	"internal/agentgate",
	"internal/schemavalidate",
}

// TestEveryAnalysisFindingModelIsRecorded keeps the count of published shapes
// from growing unobserved.
//
// #2395 records four and says the first step is deciding which survives.
// A decision about four shapes is not a decision about five, and nothing
// stopped a fifth: they are ordinary structs in ordinary packages.
func TestEveryAnalysisFindingModelIsRecorded(t *testing.T) {
	c := qt.New(t)

	models := findingModelsIn(c, findingTrees)

	c.Assert(models, qt.Not(qt.HasLen), 0,
		qt.Commentf("the scan found no result type at all, so it is measuring nothing"))
	for _, model := range models {
		c.Assert(findingModels[model], qt.Not(qt.Equals), "",
			qt.Commentf(
				"%s is a published analysis result and is not recorded.\n"+
					"Record it with what publishes it, so the question of which shape "+
					"survives keeps the same subject it was asked about.", model))
	}
}

// TestEveryRecordedFindingModelStillExists is the control.
//
// A recorded shape that has gone leaves the map describing a tree that no
// longer looks like it, and the next reader takes it as current.
func TestEveryRecordedFindingModelStillExists(t *testing.T) {
	c := qt.New(t)

	models := findingModelsIn(c, findingTrees)

	for model := range findingModels {
		c.Assert(models, qt.Contains, model,
			qt.Commentf("%s is recorded and no longer exists", model))
	}
}

// findingModelsIn returns the result types the given trees declare, as
// package-qualified names, read from the source.
func findingModelsIn(c *qt.C, trees []string) []string {
	c.Helper()
	root := repositoryRoot(c)
	models := make([]string, 0)
	for _, tree := range trees {
		for _, name := range findingModelsUnder(c, filepath.Join(root, tree)) {
			models = appendUnique(models, tree+"."+name)
		}
	}
	return models
}

// findingModelsUnder walks one tree for the type names a result is published
// under.
//
// The three names are the ones the tree actually uses. A shape published under
// a fourth name is not caught, which is worth saying rather than implying: this
// bounds the known sprawl, it does not prove there is no other.
func findingModelsUnder(c *qt.C, dir string) []string {
	c.Helper()
	published := []string{"Finding", "Diagnostic", "Problem"}
	names := make([]string, 0)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || !slices.Contains(published, spec.Name.Name) {
				return true
			}
			if _, isStruct := spec.Type.(*ast.StructType); isStruct {
				names = appendUnique(names, spec.Name.Name)
			}
			return true
		})
		return nil
	})
	c.Assert(err, qt.IsNil)
	return names
}
