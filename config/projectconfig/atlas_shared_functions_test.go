package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

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

// TestParseAtlas_ProjectFunctionsStayBytePreservingForASecret pins that a
// diagnostic reached through a function call never carries the secret.
//
// Byte replacement alone could not promise this: a function that transforms its
// argument produces a spelling the scrubber cannot find. The diagnostic is now
// withheld whenever the expression READS a sensitive variable, which does not
// depend on what the function did to the value.
func TestParseAtlas_ProjectFunctionsStayBytePreservingForASecret(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`
variable "token" {
  type      = string
  default   = "supersecret"
  sensitive = true
}
env "local" {
  url = "sqlite://${file(format("%s-x", var.token))}.db"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "supersecret")
	c.Assert(err.Error(), qt.Not(qt.Contains), "SUPERSECRET")
	c.Assert(err.Error(), qt.Contains, `reads the sensitive variable "token"`)
}
