package dbtest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// TestParseAtlasTestCases_CatchAssertAndLog translates the step blocks this
// slice added (stokaro/ptah#2866).
//
// The assertions are on what each block becomes in the shared model rather than
// on the runner's verdict, because the translation is where the meaning is
// decided: `catch` with a pattern is a different expectation from `catch`
// without one, and both look alike from the outside once they have passed.
func TestParseAtlasTestCases_CatchAssertAndLog(t *testing.T) {
	const document = `
test "schema" "translated" {
  log {
    message = "starting"
  }
  catch {
    sql = "SELECT * FROM nope"
  }
  catch {
    sql   = "SELECT * FROM nope"
    error = "no such .able"
  }
  assert {
    sql           = "SELECT 1"
    error_message = "the invariant"
  }
  exec {
    sql   = "SELECT 'ada'"
    match = "^a.a$"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	steps := cases[0].Steps
	c.Assert(steps, qt.HasLen, 5)

	c.Assert(steps[0].Log, qt.Equals, "starting")

	// No pattern: the expectation is that something failed, and nothing more.
	c.Assert(steps[1].Assert.ExpectAnyError, qt.IsTrue)
	c.Assert(steps[1].Assert.ErrorMatches, qt.Equals, "")

	// With a pattern: a regular expression rather than the substring the native
	// YAML condition takes, so the two do not share a field.
	c.Assert(steps[2].Assert.ErrorMatches, qt.Equals, "no such .able")
	c.Assert(steps[2].Assert.ErrorContains, qt.Equals, "")
	c.Assert(steps[2].Assert.ExpectAnyError, qt.IsFalse)

	c.Assert(steps[3].Assert.True, qt.IsTrue)
	c.Assert(steps[3].Assert.Message, qt.Equals, "the invariant")

	c.Assert(steps[4].Assert.Match, qt.Equals, "^a.a$")
	c.Assert(steps[4].Assert.Scalar, qt.IsNil)
}

// TestParseAtlasTestCases_AnExecWithoutAnAssertionStaysAStatement is the
// control on the row above.
//
// `exec` is a statement until an attribute turns it into an assertion, and a
// translation that made every `exec` an assertion would satisfy every positive
// row while quietly checking a result nobody asked about.
func TestParseAtlasTestCases_AnExecWithoutAnAssertionStaysAStatement(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases(
		[]byte("test \"schema\" \"plain\" {\n  exec {\n    sql = \"SELECT 1\"\n  }\n}\n"),
		"s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases[0].Steps, qt.HasLen, 1)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 1")
	c.Assert(cases[0].Steps[0].Assert, qt.IsNil)
}

// TestParseAtlasTestCases_StepRefusals_FailurePath keeps every new block
// failing closed, with the source location an author needs.
//
// The `output` beside `match` row is the one worth having: honoring one of them
// silently would leave a typo in the other as an unchecked statement, which is
// the shape the whole fail-closed rule exists for.
func TestParseAtlasTestCases_StepRefusals_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "exec takes output or match, not both",
			document: "test \"schema\" \"a\" {\n  exec {\n    sql    = \"SELECT 1\"\n    output = \"1\"\n    match  = \"1\"\n  }\n}\n",
			want:     ".*s.test.hcl:2: `exec` takes `output` or `match`, not both.*",
		},
		{
			name:     "exec rejects an unknown attribute",
			document: "test \"schema\" \"a\" {\n  exec {\n    sql  = \"SELECT 1\"\n    nope = \"x\"\n  }\n}\n",
			want:     ".*`exec` does not take \\[nope\\].*",
		},
		{
			name:     "catch rejects an unknown attribute",
			document: "test \"schema\" \"a\" {\n  catch {\n    sql  = \"SELECT 1\"\n    nope = \"x\"\n  }\n}\n",
			want:     ".*`catch` does not take \\[nope\\].*",
		},
		{
			name:     "assert rejects an unknown attribute",
			document: "test \"schema\" \"a\" {\n  assert {\n    sql  = \"SELECT 1\"\n    nope = \"x\"\n  }\n}\n",
			want:     ".*`assert` does not take \\[nope\\].*",
		},
		{
			name:     "log rejects an unknown attribute",
			document: "test \"schema\" \"a\" {\n  log {\n    message = \"m\"\n    nope    = \"x\"\n  }\n}\n",
			want:     ".*`log` does not take \\[nope\\].*",
		},
		{
			name:     "log requires its message",
			document: "test \"schema\" \"a\" {\n  log {\n  }\n}\n",
			want:     ".*`log` requires message.*",
		},
		{
			name:     "catch requires its statement",
			document: "test \"schema\" \"a\" {\n  catch {\n    error = \"x\"\n  }\n}\n",
			want:     ".*`catch` requires sql.*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.ParseAtlasTestCases([]byte(test.document), "s.test.hcl", dbtest.AtlasTestKindSchema)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
