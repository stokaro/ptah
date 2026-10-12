package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/internal/builtintest"
)

// A YAML function declares its settings and planner attributes as the other
// sources do (stokaro/ptah#3630). The settings key was accepted and never
// read, and the planner attributes were unknown keys.
func TestParse_FunctionAttributes_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `functions:
  same_tenant:
    params: t uuid
    returns: boolean
    language: sql
    leakproof: true
    parallel: safe
    strict: true
    settings: [search_path=pg_catalog]
    body: SELECT true
`

	db, err := yamlschema.Parse(builtintest.Runtime().YAML(), []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Functions, qt.HasLen, 1)
	function := db.Functions[0]
	properties, _, propertyErr := pgroutine.PropertiesOf(function.Facets)
	c.Assert(propertyErr, qt.IsNil)
	c.Assert([]any{properties.Leakproof, properties.Parallel, properties.Strict}, qt.DeepEquals, []any{true, "SAFE", true})
	c.Assert(function.Settings, qt.DeepEquals, []string{"search_path=pg_catalog"})
}

// A parallel level that is none of the three is refused rather than read as
// the default, which is the most restrictive.
func TestParse_FunctionAttributes_FailurePath(t *testing.T) {
	c := qt.New(t)
	document := `functions:
  same_tenant:
    returns: boolean
    language: sql
    parallel: sometimes
    body: SELECT true
`

	db, err := yamlschema.Parse(builtintest.Runtime().YAML(), []byte(document))

	c.Assert(err, qt.ErrorMatches, `function "same_tenant": .*parallel must be SAFE, RESTRICTED or UNSAFE, not "SOMETIMES"`)
	c.Assert(db, qt.IsNil)
}
