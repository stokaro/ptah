package atlasscript_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasscript"
)

// parse reads one document and requires it to be accepted.
func parse(c *qt.C, document string) []atlasscript.Script {
	c.Helper()

	scripts, err := atlasscript.Parse([]byte(document), "script.hcl")
	c.Assert(err, qt.IsNil)
	return scripts
}

// A documented script parses into the steps it declares, in order.
//
// Order is the program: a script is a sequence, and a parser that returned its
// steps as a set would run a purge before the condition that guards it.
func TestParse_ReadsAScriptIntoItsStepsInOrder(t *testing.T) {
	c := qt.New(t)

	scripts := parse(c, `
script "exec" "purge_inactive" {
  condition "not_empty" {
    sql = "SELECT count(*) FROM users WHERE active = 0"
  }
  exec "purge" {
    sql         = "DELETE FROM users WHERE active = 0"
    expect_rows = 3
  }
  output {
    message = "purged"
  }
}
`)

	c.Assert(scripts, qt.HasLen, 1)
	c.Assert(scripts[0].Kind, qt.Equals, atlasscript.KindExec)
	c.Assert(scripts[0].Name, qt.Equals, "purge_inactive")
	c.Assert(scripts[0].Steps, qt.HasLen, 3)
	c.Assert(scripts[0].Steps[0].Kind, qt.Equals, atlasscript.StepCondition)
	c.Assert(scripts[0].Steps[1].Kind, qt.Equals, atlasscript.StepExec)
	c.Assert(scripts[0].Steps[2].Kind, qt.Equals, atlasscript.StepOutput)
	c.Assert(*scripts[0].Steps[1].ExpectRows, qt.Equals, 3)
	c.Assert(scripts[0].Steps[2].Message, qt.Equals, "purged")
}

// `expect_rows = 0` is an assertion, and its absence is not.
//
// A script that expects to change nothing is a real thing to write — a guard
// that a purge already ran — and it is not the same as not caring. Modelling
// both as the zero value would silently turn one into the other.
func TestParse_ExpectRowsZeroIsNotTheSameAsAbsent(t *testing.T) {
	c := qt.New(t)

	withZero := parse(c, `
script "exec" "s" {
  exec "e" {
    sql         = "DELETE FROM t WHERE 1 = 0"
    expect_rows = 0
  }
}
`)
	withNone := parse(c, `
script "exec" "s" {
  exec "e" {
    sql = "DELETE FROM t WHERE 1 = 0"
  }
}
`)

	c.Assert(withZero[0].Steps[0].ExpectRows, qt.IsNotNil)
	c.Assert(*withZero[0].Steps[0].ExpectRows, qt.Equals, 0)
	c.Assert(withNone[0].Steps[0].ExpectRows, qt.IsNil)
}

// A `do` block's steps are the script's steps, in the position the block holds.
//
// The nesting must not reorder the program. A script that guards a purge with a
// condition, wraps the purge in `do`, and reports afterwards has three steps in
// one order, and flattening that into any other order runs the purge before its
// guard. Steps outside the block on BOTH sides are what makes the assertion
// able to fail: with only the inner steps, every flattening produces the same
// list and the test passes for a parser that lost the position entirely.
func TestParse_DescendsIntoDoWithoutReorderingTheProgram(t *testing.T) {
	c := qt.New(t)

	scripts := parse(c, `
script "exec" "s" {
  condition "guard" {
    sql = "SELECT count(*) FROM t"
  }
  do {
    exec "one" { sql = "DELETE FROM t" }
    output { message = "inner" }
  }
  output { message = "after" }
}
`)

	steps := scripts[0].Steps
	c.Assert(steps, qt.HasLen, 4)
	names := []string{steps[0].Name, steps[1].Name, steps[2].Message, steps[3].Message}
	c.Assert(names, qt.DeepEquals, []string{"guard", "one", "inner", "after"})
}

// A reusable mask is resolved by name, and the order of `use` is kept.
//
// Order matters because the first covering mask wins, so a parser that
// collected them into a map would make the outcome depend on iteration order.
func TestParse_ResolvesReusableMasksInTheOrderTheyAreUsed(t *testing.T) {
	c := qt.New(t)

	scripts := parse(c, `
mask "email" {
  method  = "REDACT"
  columns = ["email"]
  token   = "<email>"
}

mask "everything" {
  method     = "PARTIAL"
  keep_right = 2
}

script "query" "report" {
  query "rows" {
    sql = "SELECT id, email FROM users"
    use = [mask.email, mask.everything]
  }
}
`)

	masks := scripts[0].Steps[0].Masks
	c.Assert(masks, qt.HasLen, 2)
	c.Assert(masks.Apply("email", "ada@example.com"), qt.Equals, "<email>")
	c.Assert(masks.Apply("id", "12345"), qt.Equals, "***45")
}

// A mask that is used but never declared is refused.
//
// Skipping it would run the query with one fewer mask than the author wrote,
// which is a leak that looks like a working script.
func TestParse_RefusesAMaskThatWasNeverDeclared(t *testing.T) {
	c := qt.New(t)

	_, err := atlasscript.Parse([]byte(`
script "query" "report" {
  query "rows" {
    sql = "SELECT email FROM users"
    use = [mask.nowhere]
  }
}
`), "script.hcl")

	c.Assert(err, qt.ErrorMatches, `.*"nowhere" is used but never declared.*`)
}

// Blocks the grammar does not read yet are refused by name, not ignored.
//
// An iterator decides which rows a loop's body runs against, and a loop whose
// body is where the deletes are must not run over the wrong set because a block
// was skipped.
func TestParse_RefusesWhatItDoesNotReadYet(t *testing.T) {
	tests := []struct {
		name     string
		document string
		says     string
	}{
		{
			name: "an iterator with no init",
			document: `
script "loop" "purge" {
  iterator "keyset" {
    cursor { id = int }
  }
  do {
    exec "e" { sql = "DELETE FROM t" }
  }
}`,
			says: "no init sql",
		},
		{
			name: "an http step",
			document: `
script "exec" "s" {
  http "notify" { url = "https://example.invalid" }
}`,
			says: "http blocks are not read yet",
		},
		{
			name: "an unknown block inside a script",
			document: `
script "exec" "s" {
  frobnicate "x" { sql = "SELECT 1" }
}`,
			says: "unsupported block",
		},
		{
			name:     "an unknown top-level block",
			document: `frobnicate "x" {}`,
			says:     "unsupported block",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlasscript.Parse([]byte(test.document), "script.hcl")

			c.Assert(err, qt.ErrorMatches, `.*`+test.says+`.*`)
		})
	}
}

// The shapes a script cannot have are refused, each with its own reason.
func TestParse_RefusesAMalformedScript(t *testing.T) {
	tests := []struct {
		name     string
		document string
		says     string
	}{
		{name: "no scripts at all", document: `mask "m" { method = "REDACT" }`, says: "declares no script"},
		{
			name: "an unknown kind",
			document: `
script "frobnicate" "s" {
  exec "e" { sql = "SELECT 1" }
}`,
			says: "unsupported script kind",
		},
		{
			name: "one label",
			document: `
script "exec" {
  exec "e" { sql = "SELECT 1" }
}`,
			says: "two labels",
		},
		{name: "no steps", document: `script "exec" "s" {}`, says: "has no steps"},
		{
			name: "a step with no sql",
			document: `
script "exec" "s" {
  exec "e" {}
}`,
			says: "has no sql",
		},
		{
			name: "output with no message",
			document: `
script "exec" "s" {
  output {}
}`,
			says: "has no message",
		},
		{
			name: "a negative expect_rows",
			document: `
script "exec" "s" {
  exec "e" {
    sql         = "DELETE FROM t"
    expect_rows = -1
  }
}`,
			says: "expect_rows is negative",
		},
		{
			name: "two scripts sharing a name",
			document: `
script "exec" "s" {
  exec "e" { sql = "SELECT 1" }
}
script "exec" "s" {
  exec "e" { sql = "SELECT 2" }
}`,
			says: "declared twice",
		},
		{
			name: "two masks sharing a name",
			document: `
mask "m" { method = "REDACT" }
mask "m" { method = "HASH" }
script "query" "q" {
  query "r" { sql = "SELECT 1" }
}`,
			says: "declared twice",
		},
		{
			name: "a mask with no method",
			document: `
mask "m" { token = "x" }
script "query" "q" {
  query "r" { sql = "SELECT 1" }
}`,
			says: "has no method",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlasscript.Parse([]byte(test.document), "script.hcl")

			c.Assert(err, qt.ErrorMatches, `.*`+test.says+`.*`)
		})
	}
}

// A refusal names the file and the line, because a script is a program and its
// author needs to find the block.
func TestParse_ARefusalNamesWhereItHappened(t *testing.T) {
	c := qt.New(t)

	_, err := atlasscript.Parse([]byte("\n\n\nscript \"frobnicate\" \"s\" {\n  exec \"e\" { sql = \"SELECT 1\" }\n}\n"), "purge.hcl")

	c.Assert(err, qt.ErrorMatches, `purge\.hcl:4: .*`)
}
