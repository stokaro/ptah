package projectconfig_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/dburldisplay"
)

// addressSecretsProject is a project whose env "dev" takes url, with the
// variables pw, cred and pre, a local that converts pw to a number, a local
// that writes a user info around that conversion, and a local that writes the
// scheme and the user of an address.
func addressSecretsProject(url string) []byte {
	return []byte(`variable "pw" {
  type    = string
  default = "x"
}

variable "cred" {
  type    = string
  default = "x"
}

variable "pre" {
  type    = string
  default = "x"
}

locals {
  port = tonumber(var.pw)
  ui   = "app:${tonumber(var.pw)}"
  base = "postgres://app:"
}

env "dev" {
  url = ` + url + `
}
`)
}

// A value that reaches the password of an address the file writes is masked
// in the error HCL writes when a call on it fails, though no variable declares
// it sensitive. The value never becomes an address, so only the place it
// takes in one says it is a password.
func TestParseAtlas_MasksWhatReachesAPassword_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		vars []string
		want string
	}{
		{
			name: "a call in the password",
			url:  `"postgres://app:${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a call on the password elsewhere in the address",
			url:  `"postgres://app:${var.pw}@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password query parameter",
			url:  `"sqlserver://app@127.0.0.1:1?database=db&password=${tonumber(var.pw)}"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a local in the password whose own expression fails",
			url:  `"postgres://app:${local.port}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a user info that carries the password",
			url:  `"postgres://${var.cred}@127.0.0.1:1/db?connect_timeout=${tonumber(var.cred)}"`,
			vars: []string{"cred=app:Cr3dPassX9"},
			want: `cannot convert "app:redacted" to number`,
		},
		{
			name: "a format() verb in the password",
			url:  `format("postgres://%s:%s@127.0.0.1:1/db", "app", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a format() verb that takes its argument by index",
			url:  `format("postgres://app:%[1]s@127.0.0.1:1/db", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "the password of urluserinfo()",
			url:  `urluserinfo("postgres://127.0.0.1:1/db", "app", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a --var value that is an address",
			url:  `"${var.cred}&x=${tonumber(var.cred)}"`,
			vars: []string{"cred=postgres://app:Cr3dPassX9@127.0.0.1:1/db?sslmode=disable"},
			want: `cannot convert "postgres://app:redacted@127.0.0.1:1/db?sslmode=disable" to number`,
		},
		{
			name: "a whole user info whose value does not evaluate",
			url:  `"postgres://${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a local that writes the user info and whose own expression fails",
			url:  `"postgres://${local.ui}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password after a local that writes the scheme and the user",
			url:  `"${local.base}${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password a format() writes after a local that writes the scheme",
			url:  `format("%s%s@127.0.0.1:1/db", local.base, tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a value that writes the user info and the host",
			url:  `"postgres://${lower("APP:CR3DPASSX9@127.0.0.1:1")}/db?connect_timeout=${tonumber(lower("APP:CR3DPASSX9@127.0.0.1:1"))}"`,
			want: `cannot convert "app:redacted@127.0.0.1:1" to number`,
		},
		{
			name: "the second of two values in one password",
			url:  `"postgres://app:${var.pre}${tonumber(var.pw)}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password urlqueryset() sets",
			url:  `urlqueryset("sqlserver://app@127.0.0.1:1?database=app", "password", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a value urlqueryset() sets under a key the file does not write out",
			url:  `urlqueryset("sqlserver://app@127.0.0.1:1?database=app", var.cred, tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a password join() writes",
			url:  `join("", ["postgres://app:", tonumber(var.pw), "@127.0.0.1:1/db"])`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{ // #nosec G101 -- a placeholder replace() fills, not a credential
			name: "a password replace() writes",
			url:  `replace("postgres://app:PASS@127.0.0.1:1/db", "PASS", tonumber(var.pw))`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
		{
			name: "a user info a format() in the address writes",
			url:  `"postgres://${format("%s:%s", "app", tonumber(var.pw))}@127.0.0.1:1/db"`,
			vars: []string{"pw=Pa55wordX9"},
			want: `cannot convert "redacted" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(addressSecretsProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: test.vars})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl ".*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(err.Error(), qt.Not(qt.Contains), "Pa55wordX9")
			c.Assert(err.Error(), qt.Not(qt.Contains), "Cr3dPassX9")
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The value of getenv() in a password is masked as a --var value is: the
// variable's name says nothing about what it holds.
func TestParseAtlas_MasksAnEnvironmentValueInAPassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TEST_ADDRESS_PART", "Gh7envPassX9")

	got, err := projectconfig.ParseAtlasWithOptions(
		addressSecretsProject(`"postgres://app:${tonumber(getenv("PTAH_TEST_ADDRESS_PART"))}@127.0.0.1:1/db"`),
		"atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "dev"})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "Gh7envPassX9")
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A --var value that a typed variable refuses is masked when the variable
// reaches a password, before any expression that reads it runs.
func TestParseAtlas_MasksATypedVariableThatReachesAPassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "pw" {
  type = number
}

env "dev" {
  url = "postgres://app:${var.pw}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9"}})

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl variable "pw" expects number, got --var value "redacted"`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// The control for the tests above: a value in a place where an address keeps
// no secret keeps its text in the error, so the masking reads the place and
// does not take out every value the file is given.
func TestParseAtlas_KeepsWhatReachesNoPassword_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "the user name beside a password", url: `"postgres://${tonumber(var.pw)}:x@127.0.0.1:1/db"`},
		{name: "a query parameter", url: `"postgres://app@127.0.0.1:1/db?connect_timeout=${tonumber(var.pw)}"`},
		{name: "a format() verb in the path", url: `format("postgres://app@127.0.0.1:1/%s", tonumber(var.pw))`},
		{name: "the address of urluserinfo()", url: `urluserinfo(tostring(tonumber(var.pw)), "app", "x")`},
		{name: "a query parameter urlqueryset() sets", url: `urlqueryset("postgres://app@127.0.0.1:1/db", "application_name", tonumber(var.pw))`},
		{name: "a path join() writes", url: `join("/", ["postgres://app:x@127.0.0.1:1", tonumber(var.pw)])`}, // #nosec G101 -- a made-up value, not a credential
		{name: "a port replace() writes", url: `replace("postgres://app:x@127.0.0.1:PORT/db", "PORT", tonumber(var.pw))`},
		{name: "a port with no user info", url: `"postgres://127.0.0.1:${tonumber(var.pw)}/db"`},
		{name: "a query parameter after a port another value fills", url: `"postgres://127.0.0.1:${var.pre}/db?application_name=${tonumber(var.pw)}"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(addressSecretsProject(test.url), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Visib1eValue"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "Visib1eValue" to number.*`)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// A sensitive value a data source is given as a bare value, with no name or
// address beside it, is masked in the error the data source fails with.
func TestParseAtlas_MasksASensitiveValueADataSourceQuotes_FailurePath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "runtimevar" "probe" {
  url = "constant://?val=${var.token}&decoder=json"
}

env "dev" {
  url = "postgres://app:${data.runtimevar.probe}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.ErrorMatches,
		`data\.runtimevar\.probe: opening variable: open variable constant:\?val=redacted&decoder=json: .*`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A data source error longer than the display bound loses its middle, and a
// mask that runs after the cut cannot find a secret the cut split. The token
// here starts a few bytes before the end of the part the bound keeps, so the
// data source masks it before the bound, or a prefix of it reaches the error.
func TestParseAtlas_MasksASensitiveValueBeforeTheDisplayBound_FailurePath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "runtimevar" "probe" {
  url = "constant://?val=` + strings.Repeat("a", 955) + `${var.token}` + strings.Repeat("b", 1100) + `&decoder=json"
}

env "dev" {
  url = "postgres://app:${data.runtimevar.probe}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.ErrorMatches, `data\.runtimevar\.probe: opening variable: open variable constant:\?val=a+redacted\.\.\.\[truncated\]\.\.\.b+&decoder=json: .*`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// externalSchemaSecretsProject declares the sensitive variable token, passes
// it to an external_schema program, and selects the program with src.
func externalSchemaSecretsProject(src string) []byte {
	return []byte(`variable "token" {
  type      = string
  sensitive = true
}

data "external_schema" "app" {
  program = ["./export.sh", var.token]
}

data "composite_schema" "all" {
  schema "app" {
    url = data.external_schema.app.url
  }
}

env "dev" {
  src = ` + src + `
  url = "sqlite://file?mode=memory"
}
`)
}

// An external_schema program carries the values its diagnostics are masked
// against, so the program can print a sensitive argument alone and still not
// show it. They survive a merge with a ptah.yaml config, which is how the
// native binary reads them.
func TestParseAtlas_ExternalSchemaCarriesItsSecrets_HappyPath(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParseAtlasWithOptions(externalSchemaSecretsProject("data.external_schema.app.url"), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Secrets, qt.Contains, "Tok3nValueX9")
	merged := projectconfig.Merge(projectconfig.Config{}, cfg)
	c.Assert(merged.ExternalSchema.Secrets, qt.Contains, "Tok3nValueX9")
}

// An external_schema program that is a part of a composite_schema carries the
// same values as one an env selects itself.
func TestParseAtlas_CompositeExternalSchemaCarriesItsSecrets_HappyPath(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParseAtlasWithOptions(externalSchemaSecretsProject("data.composite_schema.all.url"), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.HasLen, 1)
	composite, ok := cfg.CompositeSchema(cfg.SchemaSources[0])
	c.Assert(ok, qt.IsTrue)
	c.Assert(composite.Parts, qt.HasLen, 1)
	c.Assert(composite.Parts[0].ExternalSchema, qt.IsNotNil)
	c.Assert(composite.Parts[0].ExternalSchema.Secrets, qt.Contains, "Tok3nValueX9")
}

// objectLocalProject is a project whose local db is an object holding a
// password beside a user, a host, a database name and a label, with env "dev"
// writing them into its url and passing field, a traversal of local.db, to a
// call that fails. The user and the host are --var values, so a reading of
// the whole object would find them.
func objectLocalProject(field string) []byte {
	return []byte(`variable "pw" {
  type    = string
  default = "x"
}

variable "user" {
  type    = string
  default = "x"
}

variable "host" {
  type    = string
  default = "x"
}

locals {
  db = {
    user     = var.user
    host     = var.host
    name     = "appdb"
    label    = "password"
    password = var.pw
  }
}

env "dev" {
  url = "postgres://${local.db.user}:${local.db.password}@${local.db.host}:5432/${local.db.name}?application_name=${tonumber(` + field + `)}"
}
`)
}

// The control for a local that is an object: only the item the password slot
// names is a secret. The user, the host, the database name, a value that is
// the word "password", and the names of the items keep their text.
func TestParseAtlas_KeepsWhatAnObjectLocalHoldsBesideAPassword_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		field string
		want  string
	}{
		{name: "the user", field: "local.db.user", want: `cannot convert "appuser" to number`},
		{name: "the host", field: "local.db.host", want: `cannot convert "dbhost.internal" to number`},
		{name: "the database name", field: "local.db.name", want: `cannot convert "appdb" to number`},
		{name: "the word password", field: "local.db.label", want: `cannot convert "password" to number`},
		{name: "a key the file writes as an index", field: `local.db["label"]`, want: `cannot convert "password" to number`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.ParseAtlasWithOptions(objectLocalProject(test.field), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9", "user=appuser", "host=dbhost.internal"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}

// The item of an object local that the password slot names is masked when a
// call on it fails.
func TestParseAtlas_MasksThePasswordAnObjectLocalHolds_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := projectconfig.ParseAtlasWithOptions(objectLocalProject("local.db.password"), "atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9"}})

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// A value a for_each env gives each instance through each is read in the
// instance's context, though the error is masked after the instance is gone.
func TestParseAtlas_MasksAValueEachGivesAPassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TEST_EACH_VALUE", "Gh7eachValX9")
	raw := []byte(`env {
  for_each = toset([getenv("PTAH_TEST_EACH_VALUE")])
  name     = atlas.env
  url      = "postgres://app:${tonumber(each.value)}@127.0.0.1:1/db"
}
`)

	got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{EnvName: "dev"})

	c.Assert(err, qt.ErrorMatches, `schemahcl: evaluate env block instance 1: cannot evaluate atlas\.hcl "url" .*cannot convert "redacted" to number.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "Gh7eachValX9")
	c.Assert(got.DatabaseURL, qt.Equals, "")
}

// The command that loads a project masks its later errors against what the
// project holds secret: a sensitive value a data "hcl_schema" block passes to
// its schema file is quoted by the schema file's parser, which runs long after
// the project's own parser is gone.
func TestParseAtlas_RecordsItsSecretsForTheCommand_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := dburldisplay.WithErrorRedaction(context.Background())
	raw := []byte(`variable "token" {
  type      = string
  sensitive = true
}

env "dev" {
  url = "sqlite://file?mode=memory"
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
		projectconfig.AtlasLoadOptions{Context: ctx, EnvName: "dev", Vars: []string{"token=Tok3nValueX9"}})

	c.Assert(err, qt.IsNil)
	c.Assert(dburldisplay.RedactRecordedText(ctx, `cannot convert "Tok3nValueX9" to number`), qt.Equals, `cannot convert "***" to number`)
}

// The control for a password read from a local the file does not write out
// item by item: a local built by merge() is read by its value, so its user
// keeps its text; and a password read from an object whole takes the values
// of its items, never their names.
func TestParseAtlas_KeepsWhatALocalReadByItsValueHolds_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		locals string
		url    string
		want   string
	}{
		{
			name:   "the user of a local merge() builds",
			locals: `db = merge({ user = "appuser" }, { password = var.pw })`,
			url:    `"postgres://${local.db.user}:${local.db.password}@127.0.0.1:1/db?application_name=${tonumber(local.db.user)}"`,
			want:   `cannot convert "appuser" to number`,
		},
		{
			name: "the user of a local whose password is read by a key the file does not write out",
			locals: `db = merge({ user = "appuser" }, { password = var.pw })
  k  = "password"`,
			url:  `"postgres://${local.db.user}:${local.db[local.k]}@127.0.0.1:1/db?application_name=${tonumber(local.db.user)}"`,
			want: `cannot convert "appuser" to number`,
		},
		{
			name: "the name of an item of an object read whole",
			locals: `db   = { username = "appuser", password = var.pw }
  word = "username"`,
			url:  `"postgres://app:${join("", values(local.db))}@127.0.0.1:1/db?application_name=${tonumber(local.word)}"`,
			want: `cannot convert "username" to number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(`variable "pw" {
  type    = string
  default = "x"
}

locals {
  ` + test.locals + `
}

env "dev" {
  url = ` + test.url + `
}
`)

			got, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "dev", Vars: []string{"pw=Pa55wordX9"}})

			c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" .*`)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got.DatabaseURL, qt.Equals, "")
		})
	}
}
