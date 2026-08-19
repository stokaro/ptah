package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// envReferenceSchema is a database with two tables, so a selector that works
// can be told apart from one that matches nothing.
func envReferenceSchema() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "keepme", Schema: "public"},
			{Name: "skipme", Schema: "public"},
		},
	}
}

// TestExclude_RefusesAnEnvReference pins the answer to a selector carrying a
// scheme this flag does not resolve.
//
// Only --to, --from and --url run through the classifier that expands
// env:// references. Passed through here, `env://exclude` is an exclusion
// pattern spelled `env://exclude`: it matches no object, so the run either
// refuses for the wrong reason -- blaming the glob rather than the scheme --
// or, where the unmatched-selection guard warns instead of failing, proceeds
// with NOTHING excluded and exits 0. On `schema apply` and `schema clean`
// that difference is destructive (stokaro/ptah#1697).
func TestExclude_RefusesAnEnvReference(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "attribute", value: "env://exclude"},
		{name: "upper case scheme", value: "ENV://exclude"},
		{name: "inside a comma-separated list", value: "keepme,env://exclude"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlasfilter.ExcludeDatabase(envReferenceSchema(), []string{test.value})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "--exclude does not resolve env:// references")
			// The remedy names what the flag actually needs, and says the env's
			// own list is already applied -- which is why resolving the
			// reference would add nothing.
			c.Assert(err.Error(), qt.Contains, "already applied without this flag")
		})
	}
}

// TestExclude_AnOrdinaryPatternStillWorks is the control the row above cannot
// be. A parser that refused every selector would satisfy it and would break
// the flag entirely.
func TestExclude_AnOrdinaryPatternStillWorks(t *testing.T) {
	c := qt.New(t)

	filtered, err := atlasfilter.ExcludeDatabase(envReferenceSchema(), []string{"keepme"})

	c.Assert(err, qt.IsNil)
	c.Assert(filtered.Tables, qt.HasLen, 1)
	c.Assert(filtered.Tables[0].Name, qt.Equals, "skipme")
}

// TestInclude_RefusesAnEnvReference pins the same answer on the other
// selector flag, with its own remedy: there is no include list on an
// environment to point at.
func TestInclude_RefusesAnEnvReference(t *testing.T) {
	c := qt.New(t)

	err := atlasfilter.ValidateIncludeSelectors([]string{"env://src"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "--include does not resolve env:// references")
	c.Assert(err.Error(), qt.Contains, "write the object pattern itself")
	c.Assert(err.Error(), qt.Not(qt.Contains), "already applied")
}

// TestInclude_AnOrdinaryPatternStillWorks is that refusal's control.
func TestInclude_AnOrdinaryPatternStillWorks(t *testing.T) {
	c := qt.New(t)

	err := atlasfilter.ValidateIncludeSelectors([]string{"keepme"})

	c.Assert(err, qt.IsNil)
}

// TestExclude_APatternThatMerelyContainsTheSchemeIsNotRefused pins that the
// refusal reads a prefix rather than a substring.
//
// A selector is a glob over object names, and one that happens to contain the
// text elsewhere is not a reference; refusing it would be the "reject
// everything" failure the controls above exist to catch, in a narrower form.
func TestExclude_APatternThatMerelyContainsTheSchemeIsNotRefused(t *testing.T) {
	c := qt.New(t)
	schema := envReferenceSchema()
	schema.Tables = append(schema.Tables, dbschematypes.DBTable{Name: "my_env_table", Schema: "public"})

	filtered, err := atlasfilter.ExcludeDatabase(schema, []string{"my_env*"})

	c.Assert(err, qt.IsNil)
	c.Assert(filtered.Tables, qt.HasLen, 2)
}
