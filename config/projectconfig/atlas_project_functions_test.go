package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// parseProjectExpression evaluates one expression in an env's `url`, which is a
// plain string attribute every env carries, and returns what it evaluated to.
func parseProjectExpression(c *qt.C, expression string) (projectconfig.Config, error) {
	c.Helper()
	source := "env \"local\" {\n  url = " + expression + "\n}\n"
	return projectconfig.ParseAtlasWithOptions(
		[]byte(source), "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "local"})
}

// TestParseAtlas_ProjectFileEvaluatesTheSchemaFunctionSet is the acceptance
// case of stokaro/ptah#1810.
//
// The project evaluator had eight functions written out by hand while the
// schema evaluator had sixty, so an expression a schema file evaluates was
// refused in `atlas.hcl` -- `join(",", var.schemas)` among them, in the block
// most likely to assemble a list of schemas.
//
// Each row is a function that was absent from the project set before and is a
// plain, deterministic transformation, so a row failing means the shared set
// stopped being shared rather than that the function changed.
func TestParseAtlas_ProjectFileEvaluatesTheSchemaFunctionSet(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "join", expression: `join(",", ["a", "b"])`, want: "a,b"},
		{name: "upper", expression: `upper("sqlite://x.db")`, want: "SQLITE://X.DB"},
		{name: "lower", expression: `lower("SQLITE://X.DB")`, want: "sqlite://x.db"},
		{name: "trimspace", expression: `trimspace("  sqlite://x.db  ")`, want: "sqlite://x.db"},
		{name: "replace", expression: `replace("sqlite://x.db", "x", "y")`, want: "sqlite://y.db"},
		{name: "element", expression: `element(["a", "b"], 1)`, want: "b"},
		{name: "concat", expression: `join("", concat(["sqlite://"], ["x.db"]))`, want: "sqlite://x.db"},
		{name: "sort", expression: `join(",", sort(["b", "a"]))`, want: "a,b"},
		{name: "distinct", expression: `join(",", distinct(["a", "a", "b"]))`, want: "a,b"},
		{name: "substr", expression: `substr("sqlite://x.db", 0, 6)`, want: "sqlite"},
		{name: "format", expression: `format("sqlite://%s.db", "x")`, want: "sqlite://x.db"},
		{name: "strrev", expression: `strrev("abc")`, want: "cba"},
		{name: "tostring", expression: `tostring(42)`, want: "42"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			config, err := parseProjectExpression(c, test.expression)

			c.Assert(err, qt.IsNil)
			c.Assert(config.DatabaseURL, qt.Equals, test.want)
		})
	}
}

// TestParseAtlas_ProjectFileEvaluatesVariablesThroughSharedFunctions is the
// issue's own acceptance sentence, with the variable it names.
func TestParseAtlas_ProjectFileEvaluatesVariablesThroughSharedFunctions(t *testing.T) {
	c := qt.New(t)
	source := `variable "schemas" {
  type    = list(string)
  default = ["public", "app"]
}

env "local" {
  url = join(",", var.schemas)
}
`

	config, err := projectconfig.ParseAtlasWithOptions(
		[]byte(source), "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "local"})

	c.Assert(err, qt.IsNil)
	c.Assert(config.DatabaseURL, qt.Equals, "public,app")
}

// TestWithProjectBoundFunctions_TheBoundSideWins pins the overlay direction on
// the helper itself.
//
// Copying the other way is the failure that would not look like one: `file`
// would exist, take the right arguments, and read a different directory
// (stokaro/ptah#1810).
func TestWithProjectBoundFunctions_TheBoundSideWins(t *testing.T) {
	c := qt.New(t)
	shared := atlashcl.ProjectFunctions()

	// `upper` stands in for any shared name a caller might rebind: the helper
	// must not care which name it is. Binding it to the shared `lower` makes
	// the direction observable -- if the copy went the other way, calling
	// "upper" would still upper-case.
	combined := atlashcl.WithProjectBoundFunctions(
		shared,
		map[string]function.Function{"upper": shared["lower"]},
	)

	// The bound entry replaced the shared one: calling "upper" now lowercases.
	result, err := combined["upper"].Call([]cty.Value{cty.StringVal("ABC")})
	c.Assert(err, qt.IsNil)
	c.Assert(result.AsString(), qt.Equals, "abc")
}

// TestParseAtlas_ProjectBoundFunctionsReadTheProjectDirectory is the same
// direction asserted through behavior rather than through the helper: `file`
// must read the directory holding atlas.hcl.
func TestParseAtlas_ProjectBoundFunctionsReadTheProjectDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "url.txt"), []byte("sqlite://from-project.db"), 0o600), qt.IsNil)
	path := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte("env \"local\" {\n  url = trimspace(file(\"url.txt\"))\n}\n"), 0o600), qt.IsNil)

	config, err := projectconfig.LoadAtlasFileWithOptions(
		path, projectconfig.AtlasLoadOptions{EnvName: "local"})

	// file() read the project directory, and the shared trimspace it is nested
	// in evaluated: the two sets compose rather than one replacing the other.
	c.Assert(err, qt.IsNil)
	c.Assert(config.DatabaseURL, qt.Equals, "sqlite://from-project.db")
}

// TestAtlasProjectFunctions_LeavesTheBoundNamesToTheCaller states the contract
// the overlay depends on. A shared set that bound `file` itself would send it
// looking in whatever directory that binding chose.
func TestAtlasProjectFunctions_LeavesTheBoundNamesToTheCaller(t *testing.T) {
	c := qt.New(t)
	shared := atlashcl.ProjectFunctions()

	c.Assert(atlashcl.ProjectBoundFunctionNames, qt.Not(qt.HasLen), 0)
	for _, name := range atlashcl.ProjectBoundFunctionNames {
		_, present := shared[name]
		c.Assert(present, qt.IsFalse, qt.Commentf("%s must be bound by the caller", name))
	}
}
