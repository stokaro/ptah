package projectconfig_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
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

// A sensitive value too short to take out of every word of a diagnostic is
// taken out of none of them, so a diagnostic that still holds it is withheld
// whole: what it quotes cannot be told from a word of its own. A value equal to
// a database name is as short as a path.
func TestParseAtlasWithholdsADiagnosticThatHoldsAShortSensitiveValue(t *testing.T) {
	for _, value := range []string{"x", "ex", "shop"} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "target" {
  type      = string
  sensitive = true
  default   = "` + value + `"
}

env {
  for_each = toset([var.target])
  name     = atlas.env
  url      = file(each.value)
}
`)

			_, err := projectconfig.ParseAtlasFSCollectionWithOptions(raw, "atlas.hcl", fstest.MapFS{},
				projectconfig.AtlasLoadOptions{EnvName: "local"})

			c.Assert(err, qt.ErrorMatches, `.*cannot evaluate atlas\.hcl "url" at atlas\.hcl:10: the underlying diagnostic is withheld because it may quote the value of a sensitive variable.*`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "open "+value)
		})
	}
}

// The value-error path withholds a reason that holds a short sensitive value in
// the same way.
func TestParseAtlasWithholdsAValueErrorThatHoldsAShortSensitiveValue(t *testing.T) {
	for _, value := range []string{"/", "/x"} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "secret" {
  type      = string
  default   = "` + value + `"
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

			c.Assert(err, qt.ErrorMatches, `atlas\.hcl "path" at atlas\.hcl:8: the value was refused; the underlying reason is withheld because it may quote the value of a sensitive variable.*`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "absolute")
		})
	}
}

// The control for the two tests above: a short sensitive value whose letters
// stand only inside the words of a diagnostic, as "on" stands in "function",
// leaves the diagnostic whole.
func TestParseAtlasKeepsADiagnosticThatHoldsAShortSensitiveValueOnlyInsideWords(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "token" {
  type      = string
  sensitive = true
  default   = "on"
}

variable "pw" {
  type    = string
  default = "x"
}

env "dev" {
  url = "postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Visib1eValue"}})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
	c.Assert(err.Error(), qt.Contains, `Invalid function argument`)
	c.Assert(err.Error(), qt.Contains, `cannot convert "Visib1eValue" to number`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// shortSensitiveProject is an atlas.hcl with a sensitive variable token, a
// plain variable pw, a local db whose size reads token and says nothing of
// its value, a local app, and env "dev" taking
// url on line 14.
func shortSensitiveProject(url string) []byte {
	return []byte(`locals {
  db  = { name = "shop", size = length([var.token]) }
  app = { name = "shop" }
}
variable "token" {
  sensitive = true
  default   = "x"
}

variable "pw" {
  default = "x"
}
env "dev" {
  url = ` + url + `
}
`)
}

// A diagnostic is withheld for a short sensitive value only where it may quote
// one: its summary or its detail holds the value as a word, and the expression
// it blames reads a sensitive variable. A word of a diagnostic about another
// value, and the file name, line and column Ptah puts before it, leave it
// whole.
func TestParseAtlasKeepsADiagnosticOfAnExpressionThatReadsNoSensitiveVariable(t *testing.T) {
	tests := []struct {
		name  string
		token string
		url   string
		want  string
	}{
		{
			name:  "a word of a call's diagnostic",
			token: "a",
			url:   `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`,
			want:  `cannot convert "Visib1eValue" to number; given string must be a decimal`,
		},
		{
			name:  "a word of an attribute's diagnostic",
			token: "an",
			url:   `"postgres://app@127.0.0.1:1/${local.app.nosuch}"`,
			want:  `This object does not have an attribute named "nosuch".`,
		},
		{
			name:  "a word of an unknown function's diagnostic",
			token: "is",
			url:   `nosuchfn("x")`,
			want:  `There is no function named "nosuchfn".`,
		},
		{
			name:  "the file name",
			token: "hcl",
			url:   `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`,
			want:  `cannot convert "Visib1eValue" to number`,
		},
		{
			name:  "the line",
			token: "14",
			url:   `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`,
			want:  `atlas.hcl:14,`,
		},
		{
			name:  "the file name before a diagnostic of an expression that reads a sensitive variable",
			token: "hcl",
			url:   `tonumber("${local.db.size}x")`,
			want:  `cannot convert "1x" to number`,
		},
		{
			name:  "a word of a call's diagnostic beside a value that reads a sensitive variable",
			token: "is",
			url:   `"${local.db.size}${nosuchfn("x")}"`,
			want:  `There is no function named "nosuchfn".`,
		},
		{
			name:  "a word of a call's diagnostic in a for expression over values that are no secret",
			token: "is",
			url:   `join("", [for s in ["x"] : nosuchfn(s)])`,
			want:  `There is no function named "nosuchfn".`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(shortSensitiveProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=" + test.token, "pw=Visib1eValue"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" at atlas\.hcl:14: atlas\.hcl:14,.*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// A Ptah rule's reason for refusing a value is withheld for a short sensitive
// value only where the expression reads a sensitive variable: a path written
// out keeps the reason, though it holds the value as a word.
func TestParseAtlasKeepsAValueErrorOfAnExpressionThatReadsNoSensitiveVariable(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "secret" {
  type      = string
  default   = "x"
  sensitive = true
}

data "hcl_schema" "x" {
  path = "/x"
}

env "local" {
  url = "sqlite://x.db?_fk=1"
  src = data.hcl_schema.x.url
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "local"})

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl "path" at atlas\.hcl:8: .*absolute.*`)
}

// A short sensitive value a failing call quotes is withheld wherever the
// expression the diagnostic blames reads it from: through a local, and
// through a data block, as well as through the for_each the test above follows.
func TestParseAtlasWithholdsADiagnosticOfAnExpressionThatReadsAShortSensitiveValue(t *testing.T) {
	tests := []struct {
		name     string
		preamble string
		url      string
	}{
		{
			name:     "through a local",
			preamble: "locals {\n  path = var.target\n}\n",
			url:      `file(local.path)`,
		},
		{
			name:     "through a data block",
			preamble: "data \"runtimevar\" \"path\" {\n  url = \"constant://?val=${var.target}&decoder=string\"\n}\n",
			url:      `file(data.runtimevar.path)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "target" {
  type      = string
  sensitive = true
  default   = "x"
}

` + test.preamble + `
env "local" {
  url = ` + test.url + `
}
`)

			_, err := projectconfig.ParseAtlasFSCollectionWithOptions(raw, "atlas.hcl", fstest.MapFS{},
				projectconfig.AtlasLoadOptions{EnvName: "local"})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" at atlas\.hcl:\d+: the underlying diagnostic is withheld because it may quote the value of a sensitive variable.*`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "open x")
		})
	}
}

// shortSensitiveSpellingsProject is an atlas.hcl with a sensitive variable
// tok, a sensitive list toks and a sensitive list secrets, followed by body.
func shortSensitiveSpellingsProject(body string) []byte {
	return []byte(`variable "tok" {
  type      = string
  sensitive = true
  default   = "zz"
}

variable "toks" {
  type      = list(string)
  sensitive = true
  default   = []
}

variable "secrets" {
  type      = list(string)
  sensitive = true
  default   = []
}

` + body)
}

// A diagnostic that may quote a short sensitive value is withheld whatever
// spelling reads it: a string index, which HCL reads as the same lookup as an
// attribute, an iteration variable of a for expression or a for directive,
// whose value the expression it ranges over gives, and a data block whose
// nested block reads it. leak is a part of the diagnostic shown when it is
// not withheld.
func TestParseAtlasWithholdsADiagnosticThatMayQuoteAShortSensitiveValueInEverySpelling(t *testing.T) {
	tests := []struct {
		name string
		body string
		vars []string
		want string
		leak string
	}{
		{
			name: "a string index of a variable in a template",
			body: "env \"dev\" {\n  url = \"${file(var[\"tok\"])}\"\n}\n",
			vars: []string{"tok=ab"},
			want: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:20: the expression reads the sensitive variable "tok" and failed; .*`,
			leak: "open ab",
		},
		{
			name: "a string index of a variable in a call",
			body: "env \"dev\" {\n  url = regex(var[\"tok\"], \"x\")\n}\n",
			vars: []string{"tok=a("},
			want: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:20: the expression reads the sensitive variable "tok" and failed; .*`,
			leak: "missing closing",
		},
		{
			name: "a string index of a local",
			body: "locals {\n  a = var.tok\n}\nenv \"dev\" {\n  url = regex(local[\"a\"], \"x\")\n}\n",
			vars: []string{"tok=a("},
			want: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:23: the underlying diagnostic is withheld because it may quote .*`,
			leak: "missing closing",
		},
		{
			name: "a string index of a variable in a local",
			body: "locals {\n  y = file(var[\"tok\"])\n}\nenv \"dev\" {\n  url = local.y\n}\n",
			vars: []string{"tok=ab"},
			want: `cannot evaluate atlas\.hcl "y" at atlas\.hcl:20: the expression reads the sensitive variable "tok" and failed; .*`,
			leak: "open ab",
		},
		{
			name: "a for expression over a local in a local",
			body: "locals {\n  a = var.toks\n  y = join(\"\", [for s in local.a : file(s)])\n}\nenv \"dev\" {\n  url = local.y\n}\n",
			vars: []string{"toks=ab", "toks=cd"},
			want: `cannot evaluate atlas\.hcl "y" at atlas\.hcl:21: the underlying diagnostic is withheld because it may quote .*`,
			leak: "open ab",
		},
		{
			name: "a for expression over a local in a template",
			body: "locals {\n  a = [var.tok]\n}\nenv \"dev\" {\n  url = \"${join(\"\", [for s in local.a : file(s)])}\"\n}\n",
			vars: []string{"tok=ab"},
			want: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:23: the underlying diagnostic is withheld because it may quote .*`,
			leak: "open ab",
		},
		{
			name: "a for directive over a local",
			body: "locals {\n  a = [var.tok]\n}\nenv \"dev\" {\n  url = \"%{ for s in local.a }${file(s)}%{ endfor }\"\n}\n",
			vars: []string{"tok=ab"},
			want: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:23: the underlying diagnostic is withheld because it may quote .*`,
			leak: "open ab",
		},
		{
			name: "a for expression over the value of a for_each",
			body: "env {\n  name     = atlas.env\n  for_each = toset(var.secrets)\n  url      = join(\"\", [for s in [each.value] : regex(s, \"x\")])\n}\n",
			vars: []string{"secrets=a(", "secrets=b("},
			want: `schemahcl: evaluate env block instance 1: cannot evaluate atlas\.hcl "url" at atlas\.hcl:22: the underlying diagnostic is withheld because it may quote .*`,
			leak: "missing closing",
		},
		{
			name: "a nested block of a data block",
			body: "data \"composite_schema\" \"app\" {\n  schema \"main\" {\n    url = \"file://${var.tok}.hcl\"\n  }\n}\nenv \"dev\" {\n  url = nosuchfn(data.composite_schema.app.url)\n}\n",
			vars: []string{"tok=is"},
			want: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:25: the underlying diagnostic is withheld because it may quote .*`,
			leak: "no function named",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := projectconfig.ParseAtlasFSCollectionWithOptions(shortSensitiveSpellingsProject(test.body), "atlas.hcl",
				fstest.MapFS{}, projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: test.vars})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err.Error(), qt.Not(qt.Contains), test.leak)
		})
	}
}
