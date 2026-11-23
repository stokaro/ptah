package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// envInstance is what a caller that runs once per instance reads off each
// config: the instance's number and the database it names.
type envInstance struct {
	Number int
	URL    string
}

func envInstances(configs []projectconfig.Config) []envInstance {
	instances := make([]envInstance, len(configs))
	for i, cfg := range configs {
		instances[i] = envInstance{Number: cfg.EnvInstance, URL: cfg.DatabaseURL}
	}
	return instances
}

// Each instance of a for_each env carries its position in the expansion, and
// a block without for_each carries none. The position counts the elements
// whose name was not selected, so the one instance a name selects still says
// which element it is.
func TestParseAtlasCollectionNumbersEachInstance_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want []envInstance
	}{
		{
			name: "a tuple numbers its elements in source order",
			raw: `env {
  for_each = ["sqlite://c.db", "sqlite://a.db", "sqlite://b.db"]
  name     = atlas.env
  url      = each.value
}
`,
			want: []envInstance{{1, "sqlite://c.db"}, {2, "sqlite://a.db"}, {3, "sqlite://b.db"}},
		},
		{
			name: "a set numbers its elements in key order",
			raw: `env "local" {
  for_each = toset(["sqlite://b.db", "sqlite://a.db"])
  url      = each.value
}
`,
			want: []envInstance{{1, "sqlite://a.db"}, {2, "sqlite://b.db"}},
		},
		{
			name: "a name selects one element and keeps its position",
			raw: `env "local" {
  for_each = { zz = "sqlite://zz.db", local = "sqlite://local.db", aa = "sqlite://aa.db" }
  name     = each.key
  url      = each.value
}
`,
			want: []envInstance{{2, "sqlite://local.db"}},
		},
		{
			name: "a block without for_each is no instance",
			raw: `env "local" {
  url = "sqlite://local.db"
}
`,
			want: []envInstance{{0, "sqlite://local.db"}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			configs, err := projectconfig.ParseAtlasCollectionWithOptions([]byte(test.raw), "atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "local"})

			c.Assert(err, qt.IsNil)
			c.Assert(envInstances(configs), qt.DeepEquals, test.want)
		})
	}
}

// No env selected is no instance, whatever the file declares.
func TestParseAtlasCollectionNumbersEachInstance_HappyPathWithoutAnEnv(t *testing.T) {
	c := qt.New(t)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions([]byte(`env {
  for_each = ["sqlite://a.db", "sqlite://b.db"]
  name     = atlas.env
  url      = each.value
}
`), "atlas.hcl", projectconfig.AtlasLoadOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(envInstances(configs), qt.DeepEquals, []envInstance{{0, ""}})
}

// A merge keeps the instance a config is, and an override that is one
// replaces it.
func TestMergeKeepsTheEnvInstance(t *testing.T) {
	for _, test := range []struct {
		name           string
		base, override int
		want           int
	}{
		{name: "an override that is no instance keeps the base's", base: 2, override: 0, want: 2},
		{name: "an override that is an instance replaces the base's", base: 2, override: 3, want: 3},
		{name: "a base that is no instance takes the override's", base: 0, override: 1, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			merged := projectconfig.Merge(
				projectconfig.Config{EnvInstance: test.base},
				projectconfig.Config{EnvInstance: test.override},
			)

			c.Assert(merged.EnvInstance, qt.Equals, test.want)
		})
	}
}
