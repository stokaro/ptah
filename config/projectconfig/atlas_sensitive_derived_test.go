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

// TestParseAtlasWithholdsATransformedSecretFromAValueError is the second half
// of the disclosure the withholding covers, and it was reopened by giving the
// project evaluator transforming functions (stokaro/ptah#1810).
//
// A value that EVALUATES and then fails a Ptah rule went through scrubbing
// rather than withholding, and scrubbing replaces the sensitive value's own
// bytes. `upper(var.token)` with `token=s3://secret` therefore put `S3://SECRET`
// in the error while the scrubber looked for `s3://secret`. Measured before the
// fix; the rows below are the transformations that defeat byte replacement.
func TestParseAtlasWithholdsATransformedSecretFromAValueError(t *testing.T) {
	rows := []derivedSecretRow{
		{
			name:       "upper",
			expression: `upper(var.token)`,
			value:      "s3://secret",
			leaked:     "S3://SECRET",
		},
		{
			name:       "replace",
			expression: `replace(var.token, "s3", "gs")`,
			value:      "s3://secret",
			leaked:     "gs://secret",
		},
		{
			name:       "join",
			expression: `join("", [var.token, "/x"])`,
			value:      "s3://secret",
			leaked:     "s3://secret/x",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "hcl_schema" "x" {
  path = ` + row.expression + `
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  src = data.hcl_schema.x.url
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

// TestParseAtlasKeepsTheValueReasonWhenNoSecretIsRead is the control: an
// ordinary bad value still says what was wrong with it, because withholding is
// the exception rather than the rule.
func TestParseAtlasKeepsTheValueReasonWhenNoSecretIsRead(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`data "hcl_schema" "x" {
  path = upper("s3://plain")
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  src = data.hcl_schema.x.url
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "local"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "S3://PLAIN")
	c.Assert(err.Error(), qt.Not(qt.Contains), "withheld")
}

// TestParseAtlasScrubsABareSensitiveValueInAValueError is the paired case, and
// the reason the withholding above is narrow.
//
// A bare `var.secret` reaches the error with the variable's own bytes, so
// scrubbing finds them and the operator still learns WHAT was wrong with the
// value. Withholding here would trade a working diagnostic for nothing
// (stokaro/ptah#1810).
func TestParseAtlasScrubsABareSensitiveValueInAValueError(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "secret" {
  type      = string
  default   = "s3://secret"
  sensitive = true
}

data "hcl_schema" "x" {
  path = var.secret
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  src = data.hcl_schema.x.url
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "local"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "s3://secret")
	c.Assert(err.Error(), qt.Contains, "(sensitive value)")
	// The reason survives, which is the whole point of scrubbing over
	// withholding where the bytes are known to match.
	c.Assert(err.Error(), qt.Contains, "unsupported URL scheme")
	c.Assert(err.Error(), qt.Not(qt.Contains), "withheld")
}
