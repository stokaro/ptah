package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// Ptah evaluates HCL twice, and the two evaluators disagreed about what the
// language is: the schema-file side registered 67 functions and the project
// side eight, so `join(",", var.schemas)` evaluated inside a schema file and
// failed inside the atlas.hcl that selected it (stokaro/ptah#1696).

// parseWithLocal evaluates an atlas.hcl whose env URL is built from one local,
// and returns the URL, so the assertion is on a value the function produced
// rather than on the absence of an error.
func parseWithLocal(c *qt.C, expression string) (string, error) {
	c.Helper()
	raw := []byte(`
variable "schemas" {
  type    = list(string)
  default = ["app", "audit"]
}
locals {
  computed = ` + expression + `
}
env "local" {
  url = "sqlite://${local.computed}.db"
}
`)
	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")
	if err != nil {
		return "", err
	}
	return cfg.DatabaseURL, nil
}

func TestParseAtlas_ProjectEvaluatorHasTheSchemaFunctionSet(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "join", expression: `join("-", var.schemas)`, want: "sqlite://app-audit.db"},
		{name: "upper", expression: `upper("hello")`, want: "sqlite://HELLO.db"},
		{name: "try", expression: `try(var.absent, "fallback")`, want: "sqlite://fallback.db"},
		{name: "length", expression: `length(var.schemas)`, want: "sqlite://2.db"},
		{name: "element", expression: `element(var.schemas, 1)`, want: "sqlite://audit.db"},
		{name: "coalescelist", expression: `join("", coalescelist([], ["x"]))`, want: "sqlite://x.db"},
		{name: "trimspace", expression: `trimspace("  padded  ")`, want: "sqlite://padded.db"},
		{name: "strrev", expression: `strrev("abc")`, want: "sqlite://cba.db"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			url, err := parseWithLocal(c, test.expression)

			c.Assert(err, qt.IsNil)
			c.Assert(url, qt.Equals, test.want)
		})
	}
}

// TestParseAtlas_KeepsTheProjectBoundFunctions is the control the overlay
// direction needs. `file`, `fileset` and `getenv` are bound to this parse --
// the first two to the project's own filesystem -- so a shared entry of the
// same name must not win over them.
func TestParseAtlas_KeepsTheProjectBoundFunctions(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TEST_SHARED_FUNCTIONS", "from-env")

	url, err := parseWithLocal(c, `getenv("PTAH_TEST_SHARED_FUNCTIONS")`)

	c.Assert(err, qt.IsNil)
	c.Assert(url, qt.Equals, "sqlite://from-env.db")
}

// TestParseAtlas_StillRefusesAnUnknownFunction is the other control: sharing a
// larger set must not turn the evaluator into one that accepts any name.
func TestParseAtlas_StillRefusesAnUnknownFunction(t *testing.T) {
	c := qt.New(t)

	_, err := parseWithLocal(c, `no_such_function("x")`)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no_such_function")
}

// A for_each env is expanded on the verbs that can take a collection and
// refused on the rest. The refusal used to say "use the corresponding
// collection-valued API" -- an internal detail an operator cannot act on, and
// one that reads like a bug rather than a limit of the verb they ran
// (stokaro/ptah#1696).

const forEachProject = `env "tenant" {
  for_each = toset(["a", "b"])
  url      = "sqlite://${each.value}.db"
}
`

// TestParseAtlas_ForEachRefusalNamesTheVerbAndTheBlock holds the sentence a
// single-instance verb owes.
func TestParseAtlas_ForEachRefusalNamesTheVerbAndTheBlock(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParseAtlasWithOptions(
		[]byte(forEachProject), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "tenant", Verb: "ptah schema inspect"},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "ptah schema inspect cannot run against a for_each env")
	c.Assert(err.Error(), qt.Contains, `atlas.hcl env "tenant" expands to 2 environments`)
	c.Assert(err.Error(), qt.Contains, "run the command once per instance")
	c.Assert(err.Error(), qt.Not(qt.Contains), "collection-valued API")
}

// TestParseAtlas_ForEachRefusalStaysGeneralWithoutAVerb is the paired case: a
// caller that names no verb gets a sentence that is still actionable rather
// than one naming the wrong command.
func TestParseAtlas_ForEachRefusalStaysGeneralWithoutAVerb(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParseAtlas([]byte(forEachProject), "atlas.hcl", "tenant")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "this command cannot run against a for_each env")
}

// TestParseAtlas_ForEachStillExpandsForACollectionCaller is the control the
// issue's third bullet asks for: one project file drives the collection through
// the API that can take it, and is refused by the one that cannot.
func TestParseAtlas_ForEachStillExpandsForACollectionCaller(t *testing.T) {
	c := qt.New(t)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(
		[]byte(forEachProject), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "tenant"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(configs, qt.HasLen, 2)
}
