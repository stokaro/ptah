package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// derivedSecretRow is one expression that reaches a failing call with a value
// derived from a sensitive variable, plus the spelling the secret would take in
// the diagnostic if it were passed through.
type derivedSecretRow struct {
	name       string
	expression string
	value      string
	leaked     string
}

func TestParseAtlasWithheldDiagnosticDoesNotDiscloseADerivedSecret(t *testing.T) {
	rows := []derivedSecretRow{{
		// jsonencode escapes the backslash, so the diagnostic contains
		// `se\\cret` while the scrubber looks for `se\cret`. Replacing the
		// literal bytes cannot find it, and the secret survives into a log.
		name:       "escaped by jsonencode",
		expression: `file(jsonencode(var.token))`,
		value:      `se\cret`,
		leaked:     `se\\cret`,
	}, {
		// The byte-preserving case the scrubber did handle. It must keep
		// working, and the value must not appear either way.
		name:       "preserved by format",
		expression: `file(format("%s", var.token))`,
		value:      "plainsecret",
		leaked:     "plainsecret",
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  migration {
    dir = "file://${` + row.expression + `}"
  }
}
`)

			_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
				EnvName: "local",
				Vars:    []string{"token=" + row.value},
			})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), row.leaked)
			c.Assert(err.Error(), qt.Not(qt.Contains), row.value)
			c.Assert(err.Error(), qt.Contains, `reads the sensitive variable "token"`)
		})
	}
}

func TestParseAtlasKeepsTheDiagnosticWhenNoSecretIsRead(t *testing.T) {
	c := qt.New(t)
	// Withholding is the exception, not the rule: an ordinary failure still
	// names the offending sub-expression, which our own message cannot.
	raw := []byte(`variable "name" {
  type = string
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  migration {
    dir = "file://${file(var.name)}"
  }
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars:    []string{"name=no-such-file"},
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no-such-file")
	c.Assert(err.Error(), qt.Not(qt.Contains), "withheld")
}

// TestParseAtlasWithholdsThroughNewlyRegisteredFunctions is the guard
// stokaro/ptah#1810 asks for by name.
//
// That change replaced eight hand-written functions with the schema
// evaluator's whole set, so a secret can now reach a failing call through
// dozens of transformations that did not exist in a project file before. The
// withholding keys on what the EXPRESSION NAMES rather than on what a function
// did to the value, which is what makes it hold for functions nobody had in
// mind when it was written -- and this is the test that says so rather than
// leaving it to be true by luck.
//
// Every row derives the secret through a function the project evaluator gained
// in that change.
func TestParseAtlasWithholdsThroughNewlyRegisteredFunctions(t *testing.T) {
	rows := []derivedSecretRow{
		{
			name:       "upper",
			expression: `file(upper(var.token))`,
			value:      "plainsecret",
			leaked:     "PLAINSECRET",
		},
		{
			name:       "join",
			expression: `file(join("/", [var.token, "x"]))`,
			value:      "plainsecret",
			leaked:     "plainsecret",
		},
		{
			name:       "replace",
			expression: `file(replace(var.token, "a", "b"))`,
			value:      "plainsecret",
			leaked:     "plbinsecret",
		},
		{
			name:       "substr",
			expression: `file(substr(var.token, 0, 5))`,
			value:      "plainsecret",
			leaked:     "plain",
		},
		{
			name:       "strrev",
			expression: `file(strrev(var.token))`,
			value:      "plainsecret",
			leaked:     "tercesnialp",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  migration {
    dir = "file://${` + row.expression + `}"
  }
}
`)

			_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
				EnvName: "local",
				Vars:    []string{"token=" + row.value},
			})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), row.leaked)
			c.Assert(err.Error(), qt.Not(qt.Contains), row.value)
			c.Assert(err.Error(), qt.Contains, `reads the sensitive variable "token"`)
		})
	}
}
