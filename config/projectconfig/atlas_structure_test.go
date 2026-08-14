package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParseAtlasProjectConfigRejectsUnsupportedEnvStructureRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "schema mode nested block",
			body: `schema {
    mode {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "schema repo nested block",
			body: `schema {
    repo {
      cloud {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "cloud" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint concurrent index nested block",
			body: `lint {
    concurrent_index {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint constraint drop nested block",
			body: `lint {
    condrop {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint data dependency nested block",
			body: `lint {
    data_depend {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint destructive nested block",
			body: `lint {
    destructive {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint incompatible nested block",
			body: `lint {
    incompatible {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint nested transaction nested block",
			body: `lint {
    nestedtx {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint git nested block",
			body: `lint {
    git {
      remote {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "remote" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "diff skip nested block",
			body: `diff {
    skip {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "diff concurrent index nested block",
			body: `diff {
    concurrent_index {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
			c.Assert(err, qt.ErrorMatches, test.wantErr)

			_, err = projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// ignoredAtlasNames lists the names a parsed config recorded as accepted and
// not acted on, in the order the parser recorded them.
func ignoredAtlasNames(cfg projectconfig.Config) []string {
	names := make([]string, 0, len(cfg.IgnoredConstructs))
	for _, ignored := range cfg.IgnoredConstructs {
		names = append(names, ignored.Name)
	}
	return names
}

// TestParseAtlasProjectConfigAcceptsEnvStructureAtlasCEAccepts pins the env
// bodies the pinned community binary v1.3.0 reads and Ptah used to refuse.
//
// Every row was measured with `schema inspect --env local` against that binary
// in a directory holding only the project file, with the exit code read
// directly from an unpiped invocation: each answered 0 while Ptah answered 1
// with `unsupported atlas.hcl construct`.
//
// The in-block control that the new tolerance did not swallow the four template
// names those blocks really do decode is the `format migrate template name`
// row of TestParseAtlasProjectConfigRefusesAtlasCEDecodedLeafValues: that
// binary answers 1 for `format { migrate { apply = 1 } }` and so does Ptah.
func TestParseAtlasProjectConfigAcceptsEnvStructureAtlasCEAccepts(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		body    string
		ignored string
	}{
		{
			name: "migration nested block",
			body: `migration {
    remote {}
  }`,
			ignored: "remote",
		},
		{
			name: "migrate format unknown attribute",
			body: `format {
    migrate {
      custom = "template"
    }
  }`,
			ignored: "custom",
		},
		{
			name: "migrate format nested block",
			body: `format {
    migrate {
      custom {}
    }
  }`,
			ignored: "custom",
		},
		{
			name: "schema format unknown attribute",
			body: `format {
    schema {
      custom = "template"
    }
  }`,
			ignored: "custom",
		},
		{
			name: "schema format nested block",
			body: `format {
    schema {
      custom {}
    }
  }`,
			ignored: "custom",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "local" {
  url = "sqlite://selected.db"
  ` + test.body + `
}
`)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")
			c.Assert(err, qt.IsNil)
			c.Assert(ignoredAtlasNames(cfg), qt.Contains, test.ignored)
		})
	}
}

// TestParseAtlasProjectConfigRefusesAtlasCEDecodedLeafValues pins the names the
// pinned community binary v1.3.0 type-checks into a SCALAR field of its project
// type while Ptah does not act on them. Ptah tolerated the name and every value
// with it, which is the direction rule (a) forbids: the binary exits 1 on each
// body below.
//
// The top-level rows are not duplicates of the env ones. parseDiffSkip and
// parseLintAttr each serve a top-level block and an env block, so the scope key
// is bare, and both spellings were measured to refuse on that binary.
func TestParseAtlasProjectConfigRefusesAtlasCEDecodedLeafValues(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "env diff skip bool given a string",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_column = "true"
    }
  }
}
`,
			wantErr: `atlas\.hcl "drop_column" at atlas\.hcl:5 must be a bool`,
		},
		{
			name: "env diff skip bool given a number",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      modify_table = 1
    }
  }
}
`,
			wantErr: `atlas\.hcl "modify_table" at atlas\.hcl:5 must be a bool`,
		},
		{
			name: "env diff skip bool given a list",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      add_index = [true]
    }
  }
}
`,
			wantErr: `atlas\.hcl "add_index" at atlas\.hcl:5 must be a bool`,
		},
		{
			name: "env diff skip bool given an object",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_foreign_key = { k = "v" }
    }
  }
}
`,
			wantErr: `atlas\.hcl "drop_foreign_key" at atlas\.hcl:5 must be a bool`,
		},
		{
			name: "top level diff skip bool given an object",
			raw: `diff {
  skip {
    add_schema = { k = "v" }
  }
}

env "local" {
  url = "sqlite://s.db"
}
`,
			wantErr: `atlas\.hcl "add_schema" at atlas\.hcl:3 must be a bool`,
		},
		{
			name: "env lint review given a number",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    review = 1
  }
}
`,
			wantErr: `atlas\.hcl "review" at atlas\.hcl:4 must be a string`,
		},
		{
			name: "top level lint review given a number",
			raw: `lint {
  review = 1
}

env "local" {
  url = "sqlite://s.db"
}
`,
			wantErr: `atlas\.hcl "review" at atlas\.hcl:2 must be a string`,
		},
		{
			name: "env migration repo written as a non-empty object",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir  = "file://m"
    repo = { k = "v" }
  }
}
`,
			wantErr: `atlas\.hcl "repo" at atlas\.hcl:5 must be a block, or an empty object`,
		},
		{
			name: "env migration repo written as a string",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir  = "file://m"
    repo = "x"
  }
}
`,
			wantErr: `atlas\.hcl "repo" at atlas\.hcl:5 must be a block, or an empty object`,
		},
		{
			name: "env migration repo block name given a number",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir = "file://m"
    repo {
      name = 1
    }
  }
}
`,
			wantErr: `atlas\.hcl "name" at atlas\.hcl:6 must be a string`,
		},
		{
			name: "env lint review given a bool",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    review = true
  }
}
`,
			wantErr: `atlas\.hcl "review" at atlas\.hcl:4 must be a string`,
		},
		{
			name: "env migration baseline given a list",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir      = "file://m"
    baseline = [1, 2]
  }
}
`,
			wantErr: `atlas\.hcl "baseline" at atlas\.hcl:5 must be a string`,
		},
		{
			name: "env migration exclude given an object",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    exclude = { k = "v" }
  }
}
`,
			wantErr: `atlas\.hcl "exclude" at atlas\.hcl:5 must be a list of strings`,
		},
		{
			// An empty object is the shape that separates a list-valued name
			// from a struct-valued one: `repo = {}` is accepted and
			// `exclude = {}` is not, on the pinned binary and here.
			name: "env migration exclude given an empty object",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    exclude = {}
  }
}
`,
			wantErr: `atlas\.hcl "exclude" at atlas\.hcl:5 must be a list of strings`,
		},
		{
			// One string is not a one-element list here, unlike `env.src`.
			name: "env migration exclude given a string",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    exclude = "public.t1"
  }
}
`,
			wantErr: `atlas\.hcl "exclude" at atlas\.hcl:5 must be a list of strings`,
		},
		{
			name: "env migration exclude given a list carrying null",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    exclude = ["public.t1", null]
  }
}
`,
			wantErr: `atlas\.hcl "exclude" at atlas\.hcl:5 must be a list of strings`,
		},
		{
			name: "env include given an object",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = { k = "v" }
}
`,
			wantErr: `atlas\.hcl "include" at atlas\.hcl:3 must be a list of strings`,
		},
		{
			name: "env include given a string",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = "public.t1"
}
`,
			wantErr: `atlas\.hcl "include" at atlas\.hcl:3 must be a list of strings`,
		},
		{
			name: "env include given a list of numbers",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = [1, 2]
}
`,
			wantErr: `atlas\.hcl "include" at atlas\.hcl:3 must be a list of strings`,
		},
		{
			name: "top level env written as an object",
			raw: `env = { k = "v" }

env "local" {
  url = "sqlite://s.db"
}
`,
			wantErr: `atlas\.hcl "env" at atlas\.hcl:1 must be a block`,
		},
		{
			// The row that keeps `env` out of atlasStructAttributes: a
			// struct-valued name takes an empty object and this one does not.
			name: "top level env written as an empty object",
			raw: `env = {}

env "local" {
  url = "sqlite://s.db"
}
`,
			wantErr: `atlas\.hcl "env" at atlas\.hcl:1 must be a block`,
		},
		{
			name: "top level env written as a list of objects",
			raw: `env = [{ name = "local" }]

env "local" {
  url = "sqlite://s.db"
}
`,
			wantErr: `atlas\.hcl "env" at atlas\.hcl:1 must be a block`,
		},
		{
			// `test` is dropped whole by both binaries, and the community
			// binary still decodes `schema` and `migrate` inside it. A name
			// under an ignored block is the third place a value rule has to
			// reach.
			name: "top level test schema written as a non-empty object",
			raw: `test {
  schema = { q = "v" }
}

env "local" {
  url = "sqlite://s.db"
}
`,
			wantErr: `atlas\.hcl "schema" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		{
			name: "env test schema written as a string",
			raw: `env "local" {
  url = "sqlite://s.db"
  test {
    schema = "x"
  }
}
`,
			wantErr: `atlas\.hcl "schema" at atlas\.hcl:4 must be a block, or an empty object`,
		},
		{
			name: "env test migrate written as a non-empty object",
			raw: `env "local" {
  url = "sqlite://s.db"
  test {
    migrate = { q = "v" }
  }
}
`,
			wantErr: `atlas\.hcl "migrate" at atlas\.hcl:4 must be a block, or an empty object`,
		},
		{
			// The control for the format tolerance widened alongside these
			// rules: the four template names each format block decodes are
			// still decoded, so a non-string value for one is still refused.
			name: "format migrate template name",
			raw: `env "local" {
  url = "sqlite://s.db"
  format {
    migrate {
      apply = 1
    }
  }
}
`,
			wantErr: `atlas\.hcl "apply" at atlas\.hcl:5 must be a string`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParseAtlasProjectConfigAcceptsAtlasCEDecodedLeafValues is the other half
// of the rule above, and it is what keeps it from being a blanket refusal.
//
// The last two rows are the scope controls. `review` is decoded under `lint`
// and nowhere else, `add_index` under `diff.skip` and nowhere else: the pinned
// community binary v1.3.0 answers 0 for `migration { review = 1 }` and for
// `lint { add_index = { k = "v" } }`, so a check keyed on the name alone rather
// than on the scope would refuse a project file that binary reads. The
// `frobnicate9` rows are the nonsense controls that keep the silences
// meaningful.
func TestParseAtlasProjectConfigAcceptsAtlasCEDecodedLeafValues(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		raw     string
		ignored string
	}{
		{
			name: "diff skip bool given a bool",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_column = true
    }
  }
}
`,
			ignored: "drop_column",
		},
		{
			name: "diff skip bool given null",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_column = null
    }
  }
}
`,
			ignored: "drop_column",
		},
		{
			name: "diff skip nonsense name given an object",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      frobnicate9 = { k = "v" }
    }
  }
}
`,
			ignored: "frobnicate9",
		},
		{
			name: "lint review given a string",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    review = "ALWAYS"
  }
}
`,
			ignored: "review",
		},
		{
			name: "lint review given null",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    review = null
  }
}
`,
			ignored: "review",
		},
		{
			name: "lint nonsense name given an object",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    frobnicate9 = { k = "v" }
  }
}
`,
			ignored: "frobnicate9",
		},
		{
			name: "migration repo written as an empty object",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir  = "file://m"
    repo = {}
  }
}
`,
			ignored: "repo",
		},
		{
			name: "migration repo written as null",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir  = "file://m"
    repo = null
  }
}
`,
			ignored: "repo",
		},
		{
			name: "migration repo block with a string name",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir = "file://m"
    repo {
      name = "myrepo"
    }
  }
}
`,
			ignored: "name",
		},
		{
			name: "migration repo block with an unknown member",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir = "file://m"
    repo {
      frobnicate9 = "x"
    }
  }
}
`,
			ignored: "frobnicate9",
		},
		{
			name: "review outside the lint scope",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir    = "file://m"
    review = 1
  }
}
`,
			ignored: "review",
		},
		{
			name: "diff skip bool name outside the skip scope",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    add_index = { k = "v" }
  }
}
`,
			ignored: "add_index",
		},
		{
			// The value is a variable reference, so this row also pins where
			// the check runs: on the tolerance path, after `var` is in the
			// evaluation context, not in the structure validator that runs
			// before it. The community binary exits 0 here too.
			name: "diff skip bool given a variable reference",
			raw: `variable "flag" {
  type    = bool
  default = true
}

env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_column = var.flag
    }
  }
}
`,
			ignored: "drop_column",
		},
		{
			// A well-formed baseline is tolerated and reported, not acted on:
			// wiring it into `migrate apply` is stokaro/ptah#934 item 5a and is
			// not this change. The community binary exits 0 on this file too.
			name: "migration baseline given a string",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir      = "file://m"
    baseline = "20240101000000"
  }
}
`,
			ignored: "baseline",
		},
		{
			name: "migration baseline given null",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir      = "file://m"
    baseline = null
  }
}
`,
			ignored: "baseline",
		},
		{
			name: "migration skip_report beside a decoded baseline",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir         = "file://m"
    skip_report = [1, 2]
  }
}
`,
			ignored: "skip_report",
		},
		{
			name: "baseline outside the migration scope",
			raw: `env "local" {
  url      = "sqlite://s.db"
  baseline = [1, 2]
}
`,
			ignored: "baseline",
		},
		{
			name: "baseline in the lint scope",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    baseline = [1, 2]
  }
}
`,
			ignored: "baseline",
		},
		{
			name: "env include given a list of strings",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = ["public.t1", "public.t2"]
}
`,
			ignored: "include",
		},
		{
			name: "env include given an empty list",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = []
}
`,
			ignored: "include",
		},
		{
			name: "env include given null",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = null
}
`,
			ignored: "include",
		},
		{
			// A set is what `toset` produces, and the pinned binary reads it.
			// Refusing it here would trade one divergence for another.
			name: "env include given a set of strings",
			raw: `env "local" {
  url     = "sqlite://s.db"
  include = toset(["public.t1", "public.t2"])
}
`,
			ignored: "include",
		},
		{
			name: "include outside the env scope",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    include = { k = "v" }
  }
}
`,
			ignored: "include",
		},
		{
			name: "env migration exclude given a list of strings",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    exclude = ["public.t1"]
  }
}
`,
			ignored: "exclude",
		},
		{
			name: "env migration exclude given null",
			raw: `env "local" {
  url = "sqlite://s.db"
  migration {
    dir     = "file://m"
    exclude = null
  }
}
`,
			ignored: "exclude",
		},
		{
			name: "exclude outside the migration scope",
			raw: `env "local" {
  url = "sqlite://s.db"
  schema {
    exclude = { k = "v" }
  }
}
`,
			ignored: "exclude",
		},
		{
			name: "top level env written as null",
			raw: `env = null

env "local" {
  url = "sqlite://s.db"
}
`,
			ignored: "env",
		},
		{
			// The ignored name is the `test` block itself: the value rule runs
			// inside a body whose own name carries no effect, so nothing new is
			// reported for the members.
			name: "top level test schema written as an empty object",
			raw: `test {
  schema = {}
}

env "local" {
  url = "sqlite://s.db"
}
`,
			ignored: "test",
		},
		{
			name: "env test schema written as null",
			raw: `env "local" {
  url = "sqlite://s.db"
  test {
    schema = null
  }
}
`,
			ignored: "test",
		},
		{
			name: "env test nonsense name given an object",
			raw: `env "local" {
  url = "sqlite://s.db"
  test {
    frobnicate9 = { q = "v" }
  }
}
`,
			ignored: "test",
		},
		{
			name: "schema outside the test scope",
			raw: `schema = { q = "v" }

env "local" {
  url = "sqlite://s.db"
}
`,
			ignored: "schema",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")
			c.Assert(err, qt.IsNil)
			c.Assert(ignoredAtlasNames(cfg), qt.Contains, test.ignored)
		})
	}
}

func TestParseAtlasProjectConfigSkipsIgnoredExpressionEvaluationInUnselectedEnv(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "attribute",
			body: `project = missing.value`,
		},
		{
			name: "block body",
			body: `cloud {
    value = missing.value
  }`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
	  url = "sqlite://selected.db"
	}
	env "other" {
	  ` + test.body + `
}
`)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://selected.db")
			c.Assert(cfg.IgnoredConstructs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasProjectConfigToleratesOpenEnvBodiesRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		body string
	}{
		{name: "env attribute", body: `custom = "literal"`},
		{name: "env block", body: `custom {}`},
		{name: "diff attribute", body: `diff { custom = "literal" }`},
		{name: "diff block", body: `diff {
    custom {}
  }`},
		{name: "format attribute", body: `format { custom = "literal" }`},
		{name: "format block", body: `format {
    custom {}
  }`},
		{name: "lint attribute", body: `lint { custom = "literal" }`},
		{name: "lint block", body: `lint {
    custom {}
  }`},
		{name: "schema attribute", body: `schema { custom = "literal" }`},
		{name: "schema block", body: `schema {
    custom {}
  }`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			selected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
			c.Assert(err, qt.IsNil)
			c.Assert(selected.IgnoredConstructs, qt.HasLen, 1)

			unselected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
			c.Assert(err, qt.IsNil)
			c.Assert(unselected.IgnoredConstructs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasProjectConfigToleratesUnknownLeafAttributesRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "schema mode",
			body: `schema {
    mode {
      custom = "literal"
    }
  }`,
		},
		{
			name: "diff skip",
			body: `diff {
    skip {
      custom = "literal"
    }
  }`,
		},
		{
			name: "diff concurrent index",
			body: `diff {
    concurrent_index {
      custom = "literal"
    }
  }`,
		},
		{
			name: "lint analyzer",
			body: `lint {
    destructive {
      custom = "literal"
    }
  }`,
		},
		{
			name: "lint git",
			body: `lint {
    git {
      custom = "literal"
    }
  }`,
		},
		{
			name: "migration",
			body: `migration {
    custom = "literal"
  }`,
		},
		{
			name: "schema repository",
			body: `schema {
    repo {
      custom = "literal"
    }
  }`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			selected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
			c.Assert(err, qt.IsNil)
			c.Assert(selected.IgnoredConstructs, qt.HasLen, 1)

			unselected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
			c.Assert(err, qt.IsNil)
			c.Assert(unselected.IgnoredConstructs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasProjectConfigClassifiesSensitiveModeRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "selected" {
  schema {
    mode {
      sensitive = "ALLOW"
    }
  }
}
env "other" {
  schema {
    mode {
      sensitive = DENY
    }
  }
}
`)
	want := []projectconfig.IgnoredAtlasConstruct{
		{Name: "sensitive", Kind: "attribute", Filename: "atlas.hcl", Line: 4},
		{Name: "sensitive", Kind: "attribute", Filename: "atlas.hcl", Line: 11},
	}

	selected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
	c.Assert(err, qt.IsNil)
	c.Assert(selected.IgnoredConstructs, qt.DeepEquals, want)

	other, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
	c.Assert(err, qt.IsNil)
	c.Assert(other.IgnoredConstructs, qt.DeepEquals, want)
}

func TestParseAtlasProjectConfigEvaluatesIgnoredExpressionsInSelectedEnv(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "attribute",
			body:    `project = missing.value`,
			wantErr: `cannot evaluate atlas\.hcl "project" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "block body",
			body: `cloud {
    value = missing.value
  }`,
			wantErr: `cannot evaluate atlas\.hcl "value" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "schema mode attribute",
			body: `schema {
    mode {
      custom = missing.value
    }
  }`,
			wantErr: `cannot evaluate atlas\.hcl "custom" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "diff skip attribute",
			body: `diff {
    skip {
      custom = missing.value
    }
  }`,
			wantErr: `cannot evaluate atlas\.hcl "custom" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "diff concurrent index attribute",
			body: `diff {
    concurrent_index {
      custom = missing.value
    }
  }`,
			wantErr: `cannot evaluate atlas\.hcl "custom" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestParseAtlasProjectConfigRecordsIgnoredConstructsOnceAcrossAllEnvs(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "selected" {
  url     = "sqlite://selected.db"
  project = "local"
}
env "other" {
  cloud {}
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "project", Kind: "attribute", Filename: "atlas.hcl", Line: 3},
		{Name: "cloud", Kind: "block", Filename: "atlas.hcl", Line: 6},
	})
}

func TestParseAtlasProjectConfigOrdersIgnoredGlobalLintAttributesDeterministically(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  zebra = "last"
  alpha = "first"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "alpha", Kind: "attribute", Filename: "atlas.hcl", Line: 3},
		{Name: "zebra", Kind: "attribute", Filename: "atlas.hcl", Line: 2},
	})
}
