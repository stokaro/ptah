package dbtest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// atlasVariableDocument renders a one-case suite whose single step carries the
// value `var.who` resolved to, so a parsed case says which value reached it.
func atlasVariableDocument(variableBody string) []byte {
	return []byte(`
variable "who" {
` + variableBody + `
}

test "schema" "greets" {
  exec {
    sql = "SELECT '${var.who}'"
  }
}
`)
}

// stepExec returns the SQL of the first step of the first case.
func stepExec(c *qt.C, cases []dbtest.Case) string {
	c.Helper()
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Steps, qt.HasLen, 1)
	return cases[0].Steps[0].Exec
}

// TestParseAtlasTestCases_SuppliedVariableSatisfiesABlockWithNoDefault_HappyPath
// pins that `--var` reaches a `.test.hcl` document.
//
// The test verbs accepted the flag and discarded it: a document declaring a
// variable without a default was refused however the flag was spelled, and the
// refusal named no way to satisfy it (stokaro/ptah#3119).
func TestParseAtlasTestCases_SuppliedVariableSatisfiesABlockWithNoDefault_HappyPath(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases(
		atlasVariableDocument(`  type = string`),
		"a.test.hcl",
		dbtest.AtlasTestKindSchema,
		dbtest.WithAtlasTestVars([]string{"who=ada"}),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stepExec(c, cases), qt.Equals, "SELECT 'ada'")
}

// TestParseAtlasTestCases_SuppliedVariableWinsOverTheDefault_HappyPath pins the
// precedence `--var` has everywhere else in Ptah.
//
// Without it, "read the default when there is one" would satisfy the test above
// while leaving the flag inert on every document that declares a default, which
// is the shape that ran on the wrong value and said nothing.
func TestParseAtlasTestCases_SuppliedVariableWinsOverTheDefault_HappyPath(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases(
		atlasVariableDocument("  type    = string\n  default = \"ada\""),
		"a.test.hcl",
		dbtest.AtlasTestKindSchema,
		dbtest.WithAtlasTestVars([]string{"who=grace"}),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stepExec(c, cases), qt.Equals, "SELECT 'grace'")
}

// TestParseAtlasTestCases_DefaultIsUsedWhenNothingIsSupplied_HappyPath is the
// control for the precedence above: threading the flag must not change a
// document nobody passed one for.
func TestParseAtlasTestCases_DefaultIsUsedWhenNothingIsSupplied_HappyPath(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases(
		atlasVariableDocument("  type    = string\n  default = \"ada\""),
		"a.test.hcl",
		dbtest.AtlasTestKindSchema,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stepExec(c, cases), qt.Equals, "SELECT 'ada'")
}

// TestParseAtlasTestCases_UnrelatedSuppliedNameIsNotAnError_HappyPath keeps the
// repair from breaking the spelling that already worked.
//
// The same `--var` reaches the HCL schema file a run compares against, so a name
// this document does not declare is that file's, not a mistake.
func TestParseAtlasTestCases_UnrelatedSuppliedNameIsNotAnError_HappyPath(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases(
		atlasVariableDocument("  type    = string\n  default = \"ada\""),
		"a.test.hcl",
		dbtest.AtlasTestKindSchema,
		dbtest.WithAtlasTestVars([]string{"tname=widgets"}),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stepExec(c, cases), qt.Equals, "SELECT 'ada'")
}

// TestParseAtlasTestCases_UnsatisfiedVariable_FailurePath pins that a variable
// with neither a supplied value nor a default is still refused, and that the
// refusal now names the flag that satisfies it.
//
// Binding it to null would let `var.who` reach a statement as the string "null":
// a test that runs, passes, and asserts against a value nobody wrote.
func TestParseAtlasTestCases_UnsatisfiedVariable_FailurePath(t *testing.T) {
	rows := []struct {
		name string
		vars []string
	}{
		{name: "nothing supplied", vars: nil},
		{name: "another name supplied", vars: []string{"other=ada"}},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			cases, err := dbtest.ParseAtlasTestCases(
				atlasVariableDocument(`  type = string`),
				"a.test.hcl",
				dbtest.AtlasTestKindSchema,
				dbtest.WithAtlasTestVars(row.vars),
			)

			c.Assert(err, qt.ErrorMatches, `(?s).*variable "who" has no `+"`default`"+`, and nothing supplies one: pass --var who=<value>.*`)
			c.Assert(cases, qt.IsNil)
		})
	}
}

// TestParseAtlasTestCases_MalformedVariableFlag_FailurePath pins that the flag
// is decoded through the one `--var` grammar, so a spelling the schema surface
// refuses is refused here with the same message.
func TestParseAtlasTestCases_MalformedVariableFlag_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		vars    []string
		wantErr string
	}{
		{name: "no equals sign", vars: []string{"who"}, wantErr: `(?s).*--var must use name=value, got "who".*`},
		{name: "empty name", vars: []string{"=ada"}, wantErr: `(?s).*--var "=ada" has an empty name.*`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			cases, err := dbtest.ParseAtlasTestCases(
				atlasVariableDocument(`  type = string`),
				"a.test.hcl",
				dbtest.AtlasTestKindSchema,
				dbtest.WithAtlasTestVars(row.vars),
			)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(cases, qt.IsNil)
		})
	}
}
