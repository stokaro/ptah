package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// The `atlas.hcl` names Atlas CE decodes into a struct field accept a BLOCK
// body. Written as an ATTRIBUTE they reach CE's object decoder, which refuses
// every member name -- including the ones the block spelling accepts -- so the
// only values the pinned community binary v1.3.0 takes are an empty object and
// null.
//
// Ptah's structure validator classifies attributes and blocks separately, so the
// attribute spelling used to fall through to the unknown-name tolerance and exit
// 0 where the binary exits 1: a compatibility rule (a) violation, and the reason
// these tests exist. Measured with `schema inspect --env local`, every exit code
// read directly from an unpiped invocation.
//
// The membership is measured name by name and is NOT "every known block type".
// Within one scope the two sets interleave -- `lint.git` is decoded and
// `lint.condrop` is not -- and the same name can be decoded under `env` and
// tolerated at the top level. TestParseAtlasStructAttributeToleratesTheRest is
// the control that holds that line: an implementation keyed off the block map
// would pass every row of the refusal test and fail nine rows there.

// TestParseAtlasStructAttributeRefusesAnObjectBody covers the thirteen names,
// across six scopes, that the pinned binary refuses an object body for.
//
// Each row's message on the pinned binary is quoted beside it. `ptah-compat`
// exited 0 on all thirteen before this change.
func TestParseAtlasStructAttributeRefusesAnObjectBody(t *testing.T) {

	tests := []struct {
		name string
		raw  string
		err  string
	}{
		// converting cty.Value to *cmdapi.Diff: unsupported attribute "k"
		{
			name: "env diff",
			raw: `env "local" {
  diff = { k = "v" }
}
`,
			err: `atlas\.hcl "diff" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		// set field "format" of type reflect.Value: unsupported attribute "k"
		{
			name: "env format",
			raw: `env "local" {
  format = { k = "v" }
}
`,
			err: `atlas\.hcl "format" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		// converting cty.Value to *cmdapi.Lint: unsupported attribute "k"
		{
			name: "env lint",
			raw: `env "local" {
  lint = { k = "v" }
}
`,
			err: `atlas\.hcl "lint" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		// converting cty.Value to *cmdapi.Migration: unsupported attribute "k"
		{
			name: "env migration",
			raw: `env "local" {
  migration = { k = "v" }
}
`,
			err: `atlas\.hcl "migration" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		// converting cty.Value to *cmdapi.Schema: unsupported attribute "k"
		{
			name: "env schema",
			raw: `env "local" {
  schema = { k = "v" }
}
`,
			err: `atlas\.hcl "schema" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		// converting cty.Value to *cmdapi.Test: unsupported attribute "k".
		// `test` is the one name here Ptah does not model in any spelling.
		{
			name: "env test",
			raw: `env "local" {
  test = { k = "v" }
}
`,
			err: `atlas\.hcl "test" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		// converting cty.Value to *cmdapi.SkipChanges: unsupported attribute "k"
		{
			name: "env diff skip",
			raw: `env "local" {
  diff {
    skip = { k = "v" }
  }
}
`,
			err: `atlas\.hcl "skip" at atlas\.hcl:3 must be a block, or an empty object`,
		},
		// set field "migrate" of type reflect.Value: unsupported attribute "k"
		{
			name: "env format migrate",
			raw: `env "local" {
  format {
    migrate = { k = "v" }
  }
}
`,
			err: `atlas\.hcl "migrate" at atlas\.hcl:3 must be a block, or an empty object`,
		},
		// set field "schema" of type reflect.Value: unsupported attribute "k"
		{
			name: "env format schema",
			raw: `env "local" {
  format {
    schema = { k = "v" }
  }
}
`,
			err: `atlas\.hcl "schema" at atlas\.hcl:3 must be a block, or an empty object`,
		},
		// set field "git" of type reflect.Value: unsupported attribute "k"
		{
			name: "env lint git",
			raw: `env "local" {
  lint {
    git = { k = "v" }
  }
}
`,
			err: `atlas\.hcl "git" at atlas\.hcl:3 must be a block, or an empty object`,
		},
		// converting cty.Value to *cmdapi.Repo: unsupported attribute "k"
		{
			name: "env schema repo",
			raw: `env "local" {
  schema {
    repo = { k = "v" }
  }
}
`,
			err: `atlas\.hcl "repo" at atlas\.hcl:3 must be a block, or an empty object`,
		},
		// The top-level set is smaller than the env one, and these three are all
		// of it. converting cty.Value to *cmdapi.Diff / *cmdapi.Lint /
		// *cmdapi.Test: unsupported attribute "k".
		{
			name: "top level diff",
			raw: `diff = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`,
			err: `atlas\.hcl "diff" at atlas\.hcl:1 must be a block, or an empty object`,
		},
		{
			name: "top level lint",
			raw: `lint = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`,
			err: `atlas\.hcl "lint" at atlas\.hcl:1 must be a block, or an empty object`,
		},
		{
			name: "top level test",
			raw: `test = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`,
			err: `atlas\.hcl "test" at atlas\.hcl:1 must be a block, or an empty object`,
		},
		// `diff` and `lint` are the two blocks that may sit at the top level as
		// well as inside `env`, and the nested names behave the same in both
		// places. Without these two rows the scope table would document a
		// narrower rule than the parser enforces.
		{
			name: "top level diff skip",
			raw: `diff {
  skip = { k = "v" }
}
env "local" {
  url = "sqlite://file.db"
}
`,
			err: `atlas\.hcl "skip" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		{
			name: "top level lint git",
			raw: `lint {
  git = { k = "v" }
}
env "local" {
  url = "sqlite://file.db"
}
`,
			err: `atlas\.hcl "git" at atlas\.hcl:2 must be a block, or an empty object`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, test.err)
		})
	}
}

// TestParseAtlasStructAttributeRefusesAMemberTheBlockSpellingAccepts is the row
// that separates "the attribute spelling is an alternative spelling" from what
// the binary actually does.
//
// Each value below is valid in the BLOCK spelling and exits 0 there. As an
// attribute the pinned binary refuses it by name -- `unsupported attribute
// "latest"`, `unsupported attribute "dir"` -- so there is no configuration to
// carry over and nothing for Ptah to implement, only a refusal to reproduce.
func TestParseAtlasStructAttributeRefusesAMemberTheBlockSpellingAccepts(t *testing.T) {

	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{
			name: "lint latest",
			raw: `env "local" {
  lint = { latest = 1 }
}
`,
			err: `atlas\.hcl "lint" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		{
			name: "migration dir",
			raw: `env "local" {
  migration = { dir = "file://migrations" }
}
`,
			err: `atlas\.hcl "migration" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		{
			name: "schema src",
			raw: `env "local" {
  schema = { src = "file://s.hcl" }
}
`,
			err: `atlas\.hcl "schema" at atlas\.hcl:2 must be a block, or an empty object`,
		},
		{
			name: "lint git dir",
			raw: `env "local" {
  lint {
    git = { dir = "x" }
  }
}
`,
			err: `atlas\.hcl "git" at atlas\.hcl:3 must be a block, or an empty object`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, test.err)
		})
	}
}

// TestParseAtlasStructAttributeRefusesNonObjectValues covers the value shapes
// the pinned binary answers `object or tuple value is required` for, plus the
// two tuple spellings it refuses for their own reasons.
//
// No tuple value was found that the binary accepts: a homogeneous one converts
// to a list, and a heterogeneous one is refused by HCL before the decoder is
// reached. Ptah refuses every tuple, which is the safe direction if one that
// these probes did not reach were to decode.
func TestParseAtlasStructAttributeRefusesNonObjectValues(t *testing.T) {

	tests := []struct {
		name string
		raw  string
	}{
		{name: "a string", raw: `env "local" {
  lint = "x"
}
`},
		{name: "a number", raw: `env "local" {
  lint = 1
}
`},
		{name: "a bool", raw: `env "local" {
  lint = true
}
`},
		{name: "a list", raw: `env "local" {
  lint = ["x"]
}
`},
		// a tuple of 5 elements is required
		{name: "an empty tuple", raw: `env "local" {
  lint = []
}
`},
		{name: "a five element list", raw: `env "local" {
  lint = [1, 2, 3, 4, 5]
}
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, `atlas\.hcl "lint" at atlas\.hcl:2 must be a block, or an empty object`)
		})
	}
}

// TestParseAtlasStructAttributeAcceptsTheEmptyShapes covers the two values the
// pinned binary takes at exit 0. Both carry no configuration, which is why it
// takes them, so Ptah records each as an ignored construct rather than acting on
// it.
func TestParseAtlasStructAttributeAcceptsTheEmptyShapes(t *testing.T) {

	tests := []struct {
		name    string
		raw     string
		ignored string
	}{
		{name: "env lint empty object", ignored: "lint", raw: `env "local" {
  lint = {}
}
`},
		{name: "env lint null", ignored: "lint", raw: `env "local" {
  lint = null
}
`},
		{name: "env migration empty object", ignored: "migration", raw: `env "local" {
  migration = {}
}
`},
		{name: "env schema empty object", ignored: "schema", raw: `env "local" {
  schema = {}
}
`},
		{name: "env test null", ignored: "test", raw: `env "local" {
  test = null
}
`},
		{name: "env lint git empty object", ignored: "git", raw: `env "local" {
  lint {
    git = {}
  }
}
`},
		{name: "env diff skip empty object", ignored: "skip", raw: `env "local" {
  diff {
    skip = {}
  }
}
`},
		{name: "top level lint empty object", ignored: "lint", raw: `lint = {}
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level test null", ignored: "test", raw: `test = null
env "local" {
  url = "sqlite://file.db"
}
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			// Accepting it silently would be the outcome the tolerance policy
			// calls the worst available: the user writes a name, nothing acts on
			// it, and nothing says so.
			c.Check(ignoredConstructContains(cfg, test.ignored), qt.IsTrue)
		})
	}
}

// TestParseAtlasStructAttributeToleratesTheRest is the non-vacuity control for
// the whole file.
//
// Every name here was probed with the SAME object value that refuses above and
// answered exit 0 on the pinned binary, alongside a `frobnicate9` nonsense
// sibling that also answered 0 -- which is what keeps these silences meaningful
// rather than assumed.
//
// The rows matter because the cheap implementation of this rule is "refuse any
// attribute whose name is a known block type at this scope", and that
// implementation passes every refusal test in this file while failing nine rows
// here: `diff.concurrent_index`, `schema.mode`, the six `lint` analyzer blocks,
// and the three top-level names that are decoded under `env` and tolerated here.
func TestParseAtlasStructAttributeToleratesTheRest(t *testing.T) {

	tests := []struct {
		name string
		raw  string
	}{
		{name: "env frobnicate9 control", raw: `env "local" {
  frobnicate9 = { k = "v" }
}
`},
		{name: "env diff concurrent_index", raw: `env "local" {
  diff {
    concurrent_index = { k = "v" }
  }
}
`},
		{name: "env schema mode", raw: `env "local" {
  schema {
    mode = { k = "v" }
  }
}
`},
		{name: "env lint concurrent_index", raw: `env "local" {
  lint {
    concurrent_index = { k = "v" }
  }
}
`},
		{name: "env lint condrop", raw: `env "local" {
  lint {
    condrop = { k = "v" }
  }
}
`},
		{name: "env lint data_depend", raw: `env "local" {
  lint {
    data_depend = { k = "v" }
  }
}
`},
		{name: "env lint destructive", raw: `env "local" {
  lint {
    destructive = { k = "v" }
  }
}
`},
		{name: "env lint incompatible", raw: `env "local" {
  lint {
    incompatible = { k = "v" }
  }
}
`},
		{name: "env lint nestedtx", raw: `env "local" {
  lint {
    nestedtx = { k = "v" }
  }
}
`},
		// The three names decoded under env and tolerated at the top level. A
		// scope-blind implementation exits 1 on each where the binary exits 0.
		{name: "top level format", raw: `format = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level migration", raw: `migration = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level schema", raw: `schema = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level atlas", raw: `atlas = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level data", raw: `data = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level locals", raw: `locals = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level variable", raw: `variable = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level frobnicate9 control", raw: `frobnicate9 = { k = "v" }
env "local" {
  url = "sqlite://file.db"
}
`},
		// Same scopes as the two top-level refusals above, and the reason the
		// table cannot simply say "top level and env alike" for every nested
		// row: these two names are tolerated where `skip` and `git` are not.
		{name: "top level diff concurrent_index", raw: `diff {
  concurrent_index = { k = "v" }
}
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level lint condrop", raw: `lint {
  condrop = { k = "v" }
}
env "local" {
  url = "sqlite://file.db"
}
`},
		// `format` and `schema` are env-scoped for this rule. A top-level block
		// of either name is not decoded into the structure by the pinned binary
		// -- both exit 0 there -- and Ptah drops the whole block, so a row that
		// labelled these "top level and env alike" would exit 1 where the binary
		// exits 0.
		{name: "top level format schema", raw: `format {
  schema = { k = "v" }
}
env "local" {
  url = "sqlite://file.db"
}
`},
		{name: "top level schema repo", raw: `schema {
  repo = { k = "v" }
}
env "local" {
  url = "sqlite://file.db"
}
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestParseAtlasStructAttributeLeavesTheBlockSpellingAlone guards the direction
// this change must not move: the block spelling of every name in the class is
// what the pinned binary configures the field with, and it exits 0 there.
//
// Without these rows a refusal keyed off the NAME rather than the attribute /
// block distinction would pass the whole refusal suite.
func TestParseAtlasStructAttributeLeavesTheBlockSpellingAlone(t *testing.T) {

	tests := []struct {
		name string
		raw  string
	}{
		{name: "env lint block", raw: `env "local" {
  lint {
    latest = 1
  }
}
`},
		{name: "env migration block", raw: `env "local" {
  migration {
    dir = "file://migrations"
  }
}
`},
		{name: "env diff skip block", raw: `env "local" {
  diff {
    skip {
      drop_table = true
    }
  }
}
`},
		{name: "env lint git block", raw: `env "local" {
  lint {
    git {
      dir = "x"
    }
  }
}
`},
		{name: "env schema repo block", raw: `env "local" {
  schema {
    repo {
      name = "x"
    }
  }
}
`},
		{name: "env format schema block", raw: `env "local" {
  format {
    schema {
      inspect = "{{ sql . }}"
    }
  }
}
`},
		{name: "env test block", raw: `env "local" {
  test {
    k = "v"
  }
}
`},
		{name: "top level lint block", raw: `lint {
  latest = 1
}
env "local" {
  url = "sqlite://file.db"
}
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestParseAtlasStructAttributeWaitsForTheEvaluationContext pins where the shape
// check runs.
//
// It has to run on the tolerance path, which the parser reaches once per
// SELECTED env with `var`, `local` and `data` already in the evaluation context.
// Running it in the structure validator instead -- which walks every env before
// that context is built -- refuses configurations the pinned binary accepts at
// exit 0, in the direction that breaks working project files:
//
//	env "local" { lint = local.nothing }                  binary 0
//	env "dev" {} env "prod" { lint = missing.value }      binary 0, selecting dev
//	env "dev" {} env "prod" { lint = { k = "v" } }        binary 0, selecting dev
//
// The third row is the one that shows the binary does not decode an unselected
// env at all: the same object body refuses at exit 1 when its env IS selected.
func TestParseAtlasStructAttributeWaitsForTheEvaluationContext(t *testing.T) {

	tests := []struct {
		name    string
		envName string
		raw     string
	}{
		{
			name:    "a local resolves in the selected env",
			envName: "local",
			raw: `locals {
  nothing = {}
}
env "local" {
  url  = "sqlite://file.db"
  lint = local.nothing
}
`,
		},
		{
			name:    "an unresolvable reference in an unselected env is not reached",
			envName: "dev",
			raw: `env "dev" {
  url = "sqlite://file.db"
}
env "prod" {
  url  = "sqlite://file.db"
  lint = missing.value
}
`,
		},
		{
			name:    "an object body in an unselected env is not reached",
			envName: "dev",
			raw: `env "dev" {
  url = "sqlite://file.db"
}
env "prod" {
  url = "sqlite://file.db"
  lint = {
    k = "v"
  }
}
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", test.envName)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestParseAtlasStructAttributeStillRefusesInTheSelectedEnv is the other half of
// the rows above, and what stops that fix from being a blanket exemption: the
// same two-env file refuses when the offending env is the one selected.
func TestParseAtlasStructAttributeStillRefusesInTheSelectedEnv(t *testing.T) {
	c := qt.New(t)
	const raw = `env "dev" {
  url = "sqlite://file.db"
}
env "prod" {
  url = "sqlite://file.db"
  lint = {
    k = "v"
  }
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "prod")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl "lint" at atlas\.hcl:6 must be a block, or an empty object`)
}

// TestParseAtlasStructAttributeReportsAnUnevaluableValueAsSuch keeps the two
// failure kinds apart.
//
// Atlas CE's tolerance is name-level, not subtree-level: an unresolvable
// reference inside a value it drops is still fatal. The value has to be
// evaluated before its shape can be judged, so the evaluation failure must be
// the one reported -- it names the offending sub-expression, and the shape
// message would blame the key instead.
func TestParseAtlasStructAttributeReportsAnUnevaluableValueAsSuch(t *testing.T) {

	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{
			name: "env lint",
			raw: `env "local" {
  lint = { k = var.nope }
}
`,
			err: `cannot evaluate atlas\.hcl "lint" at atlas\.hcl:2: .*`,
		},
		{
			name: "top level lint",
			raw: `lint = { k = var.nope }
env "local" {
  url = "sqlite://file.db"
}
`,
			err: `cannot evaluate atlas\.hcl "lint" at atlas\.hcl:1: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, test.err)
		})
	}
}
