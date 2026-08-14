package projectconfig_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

type dynamicEnvSummary struct {
	Name string
	URL  string
	Key  string
}

func TestParseAtlasCollectionExpandsUpstreamMultiTenantFixture(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  for_each = toset([
    "sqlite://bar.db?_fk=1",
    "sqlite://foo.db?_fk=1",
  ])
  name = atlas.env
  url  = each.value

  migration {
    dir = "file://migrations"
  }
}
`)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(configs, qt.HasLen, 2)
	c.Assert(dynamicEnvSummaries(configs), qt.DeepEquals, []dynamicEnvSummary{
		{Name: "local", URL: "sqlite://bar.db?_fk=1"},
		{Name: "local", URL: "sqlite://foo.db?_fk=1"},
	})
	c.Assert(configs[0].Migration.Dir, qt.Equals, "migrations")
	c.Assert(configs[1].Migration.Dir, qt.Equals, "migrations")
}

func TestParseAtlasCollectionOrdersEachBindingsDeterministically(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		prefix  string
		forEach string
		want    []dynamicEnvSummary
	}{
		{
			name:    "tuple keeps source order",
			forEach: `["second", "first"]`,
			want: []dynamicEnvSummary{
				{Name: "local", URL: "second", Key: "0"},
				{Name: "local", URL: "first", Key: "1"},
			},
		},
		{
			name: "typed list keeps source order",
			prefix: `variable "targets" {
  type    = list(string)
  default = ["second", "first"]
}
`,
			forEach: `var.targets`,
			want: []dynamicEnvSummary{
				{Name: "local", URL: "second", Key: "0"},
				{Name: "local", URL: "first", Key: "1"},
			},
		},
		{
			name:    "object sorts keys",
			forEach: `{ z = "last", a = "first" }`,
			want: []dynamicEnvSummary{
				{Name: "local", URL: "first", Key: `"a"`},
				{Name: "local", URL: "last", Key: `"z"`},
			},
		},
		{
			name: "typed map sorts keys",
			prefix: `variable "targets" {
  type = map(string)
  default = {
    z = "last"
    a = "first"
  }
}
`,
			forEach: `var.targets`,
			want: []dynamicEnvSummary{
				{Name: "local", URL: "first", Key: `"a"`},
				{Name: "local", URL: "last", Key: `"z"`},
			},
		},
		{
			name:    "set sorts and removes duplicates",
			forEach: `toset(["second", "first", "second"])`,
			want: []dynamicEnvSummary{
				{Name: "local", URL: "first", Key: `"first"`},
				{Name: "local", URL: "second", Key: `"second"`},
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(test.prefix + `env {
  for_each = ` + test.forEach + `
  name     = atlas.env
  url      = each.value
  dev      = jsonencode(each.key)
}
`)

			configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
				EnvName: "local",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(dynamicEnvSummaries(configs), qt.DeepEquals, test.want)
		})
	}
}

func TestParseAtlasCollectionCanRejectListAndMapForEach(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		forEach string
		wantErr string
	}{
		{
			name: "typed list",
			prefix: `variable "targets" {
  type    = list(string)
  default = ["sqlite://file?mode=memory"]
}
`,
			forEach: "var.targets",
			wantErr: "schemahcl: for_each does not support list of string type",
		},
		{
			name: "typed map",
			prefix: `variable "targets" {
  type    = map(string)
  default = { local = "sqlite://file?mode=memory" }
}
`,
			forEach: "var.targets",
			wantErr: "schemahcl: for_each does not support map of string type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			raw := []byte(test.prefix + `env {
  for_each = ` + test.forEach + `
  name     = atlas.env
  url      = each.value
}
`)

			_, err := projectconfig.ParseAtlasCollectionWithOptions(
				raw,
				"atlas.hcl",
				projectconfig.AtlasLoadOptions{
					EnvName:              "local",
					RejectListMapForEach: true,
				},
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			configs, err := projectconfig.ParseAtlasCollectionWithOptions(
				raw,
				"atlas.hcl",
				projectconfig.AtlasLoadOptions{EnvName: "local"},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(configs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasCollectionPreservesLabeledEnvPrecedence(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  url = "labeled"
  dev = atlas.env
}

env {
  for_each = var.not_evaluated
  name     = atlas.env
  url      = each.value
}
`)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(dynamicEnvSummaries(configs), qt.DeepEquals, []dynamicEnvSummary{
		{Name: "local", URL: "labeled", Key: "local"},
	})
}

func TestParseAtlasCollectionExpandsLabeledEnv(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  for_each = toset(["sqlite://foo.db", "sqlite://bar.db"])
  url      = each.value
}
`)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(dynamicEnvSummaries(configs), qt.DeepEquals, []dynamicEnvSummary{
		{Name: "local", URL: "sqlite://bar.db"},
		{Name: "local", URL: "sqlite://foo.db"},
	})
}

func TestParseAtlasCollectionFiltersLabeledEnvByEvaluatedName(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  name = "other"
  url  = "sqlite://other.db"
}
`)

	_, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `atlas env "local" not found`)
}

func TestParseAtlasCollectionRejectsUnlabeledStaticEnvSelection(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  name = "local"
  url  = "sqlite://local.db"
}

env {
  name = "prod"
  url  = var.not_evaluated
}
`)

	_, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `atlas env "local" not found`)
}

func TestParseAtlasCollectionRejectsUnlabeledEachKeySelection(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  for_each = {
    prod  = "sqlite://prod.db"
    local = "sqlite://local.db"
  }
  name = each.key
  url  = each.value
}
`)

	_, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `atlas env "local" not found`)
}

func TestParseAtlasCollectionSelectsComputedAtlasEnvName(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  name = format("%s", atlas.env)
  url  = "sqlite://local.db"
}
`)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(dynamicEnvSummaries(configs), qt.DeepEquals, []dynamicEnvSummary{
		{Name: "local", URL: "sqlite://local.db"},
	})
}

func TestParseAtlasCollectionEvaluatesRejectedLabeledInstance(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  for_each = {
    local = "sqlite://local.db"
    prod  = "sqlite://prod.db"
  }
  name = each.key
  url  = each.key == "prod" ? var.missing : each.value
}
`)

	_, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "schemahcl: evaluate env block instance 2")
	c.Assert(err.Error(), qt.Contains, `no variable named "var"`)
}

func TestParseAtlasCollectionMergesGlobalPolicyIntoEveryInstance(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  latest = 3
}

diff {
  skip {
    drop_table = true
  }
}

env {
  for_each = ["first", "second"]
  name     = atlas.env
  url      = each.value
}
`)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(configs, qt.HasLen, 2)
	c.Assert(configs[0].Lint.Latest, qt.IsNotNil)
	c.Assert(configs[1].Lint.Latest, qt.IsNotNil)
	c.Assert(*configs[0].Lint.Latest, qt.Equals, 3)
	c.Assert(*configs[1].Lint.Latest, qt.Equals, 3)
	c.Assert(configs[0].Diff.Skip.DropTable, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
	c.Assert(configs[1].Diff.Skip.DropTable, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
}

func TestParseAtlasCollectionSharesIgnoredConstructsAcrossInstances(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  for_each = ["first", "second"]
  name     = atlas.env
  url      = each.value
  custom   = each.key
}
`)

	configs, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(configs, qt.HasLen, 2)
	want := []projectconfig.IgnoredAtlasConstruct{
		{Name: "custom", Kind: "attribute", Filename: "atlas.hcl", Line: 5},
	}
	c.Assert(configs[0].IgnoredConstructs, qt.DeepEquals, want)
	c.Assert(configs[1].IgnoredConstructs, qt.DeepEquals, want)
}

func TestParseAtlasCollectionReusesRootedFileEvaluation(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  for_each = toset(["first.txt", "second.txt"])
  name     = atlas.env
  url      = file(each.value)
}
`)
	fsys := fstest.MapFS{
		"first.txt":  {Data: []byte("first")},
		"second.txt": {Data: []byte("second")},
	}

	configs, err := projectconfig.ParseAtlasFSCollectionWithOptions(
		raw,
		"atlas.hcl",
		fsys,
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(dynamicEnvSummaries(configs), qt.DeepEquals, []dynamicEnvSummary{
		{Name: "local", URL: "first"},
		{Name: "local", URL: "second"},
	})
}

func TestParseAtlasCollectionRejectsInvalidForEach(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "number", value: `42`, wantErr: `schemahcl: for_each does not support number type`},
		{name: "string", value: `"target"`, wantErr: `schemahcl: for_each does not support string type`},
		{name: "null", value: `null`, wantErr: `schemahcl: for_each cannot be null`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env {
  for_each = ` + test.value + `
  name     = atlas.env
  url      = each.value
}
`)

			_, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
				EnvName: "local",
			})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestParseAtlasCollectionDoesNotExposeForEachValueInBodyError(t *testing.T) {
	c := qt.New(t)
	const secret = "SUPERSECRET_DYNAMIC_TARGET_12345"
	raw := []byte(`variable "target" {
	  type      = string
	  sensitive = true
	  default   = "` + secret + `"
}

env {
	  for_each = toset([var.target])
  name     = atlas.env
  url      = true
}
`)

	_, err := projectconfig.ParseAtlasCollectionWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "evaluate env block instance 1")
	c.Assert(err.Error(), qt.Not(qt.Contains), secret)
}

func TestParseAtlasCollectionScrubsSensitiveForEachValueFromHCLDiagnostic(t *testing.T) {
	c := qt.New(t)
	const secret = "SUPERSECRET_DYNAMIC_FILE_12345"
	raw := []byte(`variable "target" {
	  type      = string
	  sensitive = true
	  default   = "` + secret + `"
}

env {
	  for_each = toset([var.target])
  name     = atlas.env
  url      = file(each.value)
}
`)

	_, err := projectconfig.ParseAtlasFSCollectionWithOptions(
		raw,
		"atlas.hcl",
		fstest.MapFS{},
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "(sensitive value)")
	c.Assert(err.Error(), qt.Not(qt.Contains), secret)
}

func TestParseAtlasCollectionScrubsSensitiveForEachKeyFromHCLDiagnostic(t *testing.T) {
	c := qt.New(t)
	const secret = "SUPERSECRET_DYNAMIC_KEY_12345"
	raw := []byte(`variable "target" {
	  type      = string
	  sensitive = true
	  default   = "` + secret + `"
}

env {
	  for_each = {
	    (var.target) = "sqlite://target.db"
	  }
  name     = atlas.env
  url      = file(each.key)
}
`)

	_, err := projectconfig.ParseAtlasFSCollectionWithOptions(
		raw,
		"atlas.hcl",
		fstest.MapFS{},
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "(sensitive value)")
	c.Assert(err.Error(), qt.Not(qt.Contains), secret)
}

func TestSingularProjectConfigAdaptersRejectMultipleSelectedInstances(t *testing.T) {
	c := qt.New(t)
	raw := `env {
  for_each = toset(["first", "second"])
  name     = atlas.env
  url      = each.value
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")
	c.Assert(
		err,
		qt.ErrorMatches,
		`atlas env "local" selected 2 project config instances; use the corresponding collection-valued API`,
	)

	chdirWith(c, map[string]string{"atlas.hcl": raw})
	configs, err := projectconfig.LoadCollection(projectconfig.LoadOptions{EnvName: "local"})
	c.Assert(err, qt.IsNil)
	c.Assert(configs, qt.HasLen, 2)

	_, err = projectconfig.Load(projectconfig.LoadOptions{EnvName: "local"})
	c.Assert(
		err,
		qt.ErrorMatches,
		`atlas env "local" selected 2 project config instances; use the corresponding collection-valued API`,
	)
}

func dynamicEnvSummaries(configs []projectconfig.Config) []dynamicEnvSummary {
	summaries := make([]dynamicEnvSummary, len(configs))
	for i, cfg := range configs {
		summaries[i] = dynamicEnvSummary{
			Name: cfg.EnvName,
			URL:  cfg.DatabaseURL,
			Key:  cfg.DevURL,
		}
	}
	return summaries
}
