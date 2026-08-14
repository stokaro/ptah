package projectconfig_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// TestParseAtlasEnvSchemasSelectsTheSchemaUniverse pins the decode half of
// `env { schemas }`.
//
// The attribute reached [projectconfig.Config] through the unknown-name
// tolerance until stokaro/ptah#934: it had no parser arm, so it was recorded as
// ignored, warned about, and dropped. The pinned Atlas community binary v1.3.0
// decodes it — measured by planting a value the field cannot hold, which a name
// the binary ignores cannot refuse:
//
//	env { schemas = "one" }  ->  schemahcl: field is of type slice but attr
//	                             "schemas" is type: string, exit 1
//
// so tolerating it was the wrong classification and the value has to select
// something.
func TestParseAtlasEnvSchemasSelectsTheSchemaUniverse(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantSchemas []string
		wantPresent bool
	}{
		{
			name: "one schema restricts to it",
			raw: `env "local" {
  schemas = ["one"]
}
`,
			wantSchemas: []string{"one"},
			wantPresent: true,
		},
		{
			name: "two schemas keep both, in the written order",
			raw: `env "local" {
  schemas = ["two", "one"]
}
`,
			wantSchemas: []string{"two", "one"},
			wantPresent: true,
		},
		{
			// Measured on the pinned binary against a database holding `one`,
			// `two` and `public`: a schema that does not exist describes
			// nothing and exits 0. It is a selection that matched nothing, not
			// an error, so the name has to survive the parse to reach the
			// reader.
			name: "a schema that does not exist is still a selection",
			raw: `env "local" {
  schemas = ["nosuchschema"]
}
`,
			wantSchemas: []string{"nosuchschema"},
			wantPresent: true,
		},
		{
			// Measured on the pinned binary: `schemas = []` describes the same
			// three schemas an absent attribute describes. An empty list is
			// therefore not a restriction naming nothing.
			//
			// It is still PRESENT, which is the same answer `exclude = []`
			// already gives: presence is what lets an atlas.hcl empty list
			// clear a ptah.yaml value rather than silently inherit it. The
			// no-restriction behavior comes from the value, not the presence
			// bit — an empty list joins to the empty flag value, which
			// [go.5x5.cz/ptah/cmd/internal/dbcli.ParseSchemas] reads as no
			// schemas named.
			name: "an empty list is present and selects nothing",
			raw: `env "local" {
  schemas = []
}
`,
			wantSchemas: []string{},
			wantPresent: true,
		},
		{
			name: "an absent attribute restricts nothing",
			raw: `env "local" {
  url = "postgres://example/db"
}
`,
			wantSchemas: nil,
			wantPresent: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar)(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Check(cfg.Schemas, qt.DeepEquals, test.wantSchemas)
			c.Check(cfg.SchemasValue().Present, qt.Equals, test.wantPresent)
			// A decoded attribute must not also be reported as having no
			// effect. Without this row the warning and the selection could both
			// be emitted and every other assertion here would still pass.
			c.Check(ignoredConstructNames(cfg), qt.Not(qt.Contains), "schemas")
		})
	}
}

// TestParseAtlasEnvSchemasRefusesValuesTheFieldCannotHold is the refusal half,
// and it is what closes compatibility rule (a) for this attribute: the pinned
// binary exits 1 on each of these and `ptah-compat` exited 0.
func TestParseAtlasEnvSchemasRefusesValuesTheFieldCannotHold(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{
			name: "a bare string where a list belongs",
			raw: `env "local" {
  schemas = "one"
}
`,
			err: `atlas\.hcl "schemas" at atlas\.hcl:2 must be a list of strings`,
		},
		{
			name: "an object where a list belongs",
			raw: `env "local" {
  schemas = { first = "one" }
}
`,
			err: `atlas\.hcl "schemas" at atlas\.hcl:2 must be a list of strings`,
		},
		{
			name: "a list of numbers",
			raw: `env "local" {
  schemas = [1, 2]
}
`,
			err: `atlas\.hcl "schemas" at atlas\.hcl:2 must be a list of strings`,
		},
		{
			// The tolerance Atlas CE applies is name-level, not subtree-level,
			// and this attribute is no longer an unknown name at all: its
			// expression is evaluated like any other decoded attribute's.
			name: "an unresolvable reference inside the list",
			raw: `env "local" {
  schemas = [var.nope]
}
`,
			err: `cannot evaluate atlas\.hcl "schemas" at atlas\.hcl:2: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar)(t)

			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, test.err)
		})
	}
}

// TestParseAtlasEnvSchemasOptOut covers PTAH_ATLAS_IGNORE_ENV_SCHEMAS.
//
// The opt-out exists because acting on the attribute removes a description Ptah
// used to emit, and it governs the SELECTION only. The refusal row is the one
// that matters most: a Ptah environment variable may not reopen an exit 0 where
// the pinned binary exits 1, so the type check has to run before the variable is
// consulted and return before it.
func TestParseAtlasEnvSchemasOptOut(t *testing.T) {
	const selecting = `env "local" {
  schemas = ["one"]
}
`
	const malformed = `env "local" {
  schemas = "one"
}
`

	tests := []struct {
		name        string
		env         func(testing.TB)
		raw         string
		err         string
		wantSchemas []string
		wantIgnored bool
	}{
		{
			name:        "unset selects, and records no ignored construct",
			env:         envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar),
			raw:         selecting,
			wantSchemas: []string{"one"},
			wantIgnored: false,
		},
		{
			name:        "a valid false selects too",
			env:         envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "false"),
			raw:         selecting,
			wantSchemas: []string{"one"},
			wantIgnored: false,
		},
		{
			name:        "a valid true drops the selection and says so",
			env:         envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "1"),
			raw:         selecting,
			wantSchemas: nil,
			wantIgnored: true,
		},
		{
			name: "the opt-out does not reopen the refusal",
			env:  envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "true"),
			raw:  malformed,
			err:  `atlas\.hcl "schemas" at atlas\.hcl:2 must be a list of strings`,
		},
		{
			name: "an unparsable value is a configuration error, not a silent default",
			env:  envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "yes"),
			raw:  selecting,
			err:  `invalid boolean value "yes" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
		{
			name: "an exported empty value is a configuration error too",
			env:  envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, ""),
			raw:  selecting,
			err:  `invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			assertOptionalErrorMatches(c, err, test.err)
			c.Check(cfg.Schemas, qt.DeepEquals, test.wantSchemas)
			c.Check(ignoredConstructContains(cfg, "schemas"), qt.Equals, test.wantIgnored)
		})
	}
}

// TestParseAtlasEnvSchemasOptOutValidatesWithoutTheAttribute pins WHEN the
// value is read, which the rows above cannot see.
//
// Every row in [TestParseAtlasEnvSchemasOptOut] spells `schemas` in the
// selected environment, so all of them reach the parser arm that used to do the
// resolving. A malformed value was therefore refused on a config that names the
// attribute and honored as its default on one that does not — the same broken
// environment, two answers, chosen by the file under parse. Resolving when the
// parser is built is what makes the answer the environment's alone.
func TestParseAtlasEnvSchemasOptOutValidatesWithoutTheAttribute(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		env  string
	}{
		{
			name: "selected environment omits schemas",
			raw: `env "local" {
  url = "sqlite://project.db"
}
`,
			env: "local",
		},
		{
			name: "project has no environment block",
			raw:  "",
			env:  "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "")(t)

			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", test.env)

			c.Assert(err, qt.ErrorMatches,
				`invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`)
		})
	}
}

// TestLoadAtlasEnvSchemasOptOutValidatesWithoutAConfigFile covers the load path
// that never builds a parser at all.
//
// A project with no atlas.hcl is the common case, and it returns an empty
// config before any parse, so the construction-time resolve is unreachable
// there. Without the resolve the loader itself makes, a typo in the variable
// would be reported by whichever project happened to carry a config file and by
// no other.
func TestLoadAtlasEnvSchemasOptOutValidatesWithoutAConfigFile(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "")(t)

	_, err := projectconfig.LoadAtlasFileCollectionWithOptions(
		filepath.Join(t.TempDir(), projectconfig.AtlasFileName),
		projectconfig.AtlasLoadOptions{},
	)

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`)
}

// assertOptionalErrorMatches asserts the error against want, treating an empty
// want as "no error". It is a helper rather than a branch in the test body
// because the repository's test-style gate rejects control flow there.
func assertOptionalErrorMatches(c *qt.C, err error, want string) {
	c.Helper()
	checks := map[bool]func(){
		true:  func() { c.Assert(err, qt.IsNil) },
		false: func() { c.Assert(err, qt.ErrorMatches, want) },
	}
	checks[want == ""]()
}

func ignoredConstructNames(cfg projectconfig.Config) []string {
	names := make([]string, 0, len(cfg.IgnoredConstructs))
	for _, construct := range cfg.IgnoredConstructs {
		names = append(names, construct.Name)
	}
	return names
}

func ignoredConstructContains(cfg projectconfig.Config, name string) bool {
	for _, construct := range cfg.IgnoredConstructs {
		if construct.Name == name {
			return true
		}
	}
	return false
}
