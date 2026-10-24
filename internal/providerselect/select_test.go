package providerselect_test

import (
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerselect"
)

// fixtureConfig claims the engine "fixture" for an executable that does not
// exist, so a Choice is made without any process being started.
func fixtureConfig(c *qt.C) providerconfig.Configuration {
	c.Helper()
	return providerconfig.Configuration{
		FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{
			Spec:    providerhost.Spec{ProviderID: "example.org/fixture", Executable: filepath.Join(c.TempDir(), "missing"), SHA256: strings.Repeat("a", 64)},
			Engines: []providerconfig.Engine{{ID: "fixture", Aliases: []string{"fix"}}},
		}},
	}
}

// emptyResolver finds no bundled or stored official provider.
func emptyResolver() *providerconfig.Resolver { return &providerconfig.Resolver{} }

func TestChoose_HappyPath(t *testing.T) {
	t.Run("a built-in engine keeps its implementation without a request", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{
			Targets:  []providerselect.Target{{Flag: "db-url", Value: "ydb://localhost:2136/local"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(choice, qt.IsNil)
	})
	t.Run("a manifest claim selects the provider by URL scheme", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{
			Config: fixtureConfig(c), Configured: true,
			Targets:  []providerselect.Target{{Flag: "db-url", Value: "fixture:///tmp/x.db"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(choice.Target, qt.Equals, "fixture")
	})
	t.Run("an alias selects the engine it belongs to", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{
			Config: fixtureConfig(c), Configured: true,
			Targets:  []providerselect.Target{{Flag: "db-url", Value: "fix:///tmp/x.db"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(choice.Target, qt.Equals, "fixture")
	})
	t.Run("a docker dev URL selects the provider of its engine", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{
			Config: fixtureConfig(c), Configured: true,
			Targets:  []providerselect.Target{{Flag: "dev-url", Value: "docker://fixture/1/dev"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(choice.Target, qt.Equals, "fixture")
	})
	t.Run("one configured engine is the default of a command with no target", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{Config: fixtureConfig(c), Configured: true, Resolver: emptyResolver()})
		c.Assert(err, qt.IsNil)
		c.Assert(choice.Target, qt.Equals, "fixture")
	})
	t.Run("an unknown scheme with no manifest is left to the caller", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{
			Targets:  []providerselect.Target{{Flag: "db-url", Value: "nothing://host/db"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(choice, qt.IsNil)
	})
}

func TestChoose_FailurePath(t *testing.T) {
	t.Run("an explicit official engine with nothing installed is missing", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{Engine: "ydb", Explicit: true, Resolver: emptyResolver()})
		c.Assert(err, qt.ErrorMatches, `(?s).*ptah providers install ydb.*`)
		c.Assert(choice, qt.IsNil)
	})
	t.Run("a target the provider does not own is refused", func(t *testing.T) {
		c := qt.New(t)
		choice, err := providerselect.Choose(providerselect.Request{
			Config: fixtureConfig(c), Configured: true, Engine: "fixture", Explicit: true,
			Targets:  []providerselect.Target{{Flag: "db-url", Value: "sqlite:///:memory:"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.ErrorMatches, `(?s).*not owned by the selected provider engine.*`)
		c.Assert(choice, qt.IsNil)
	})
	t.Run("a manifest of two engines needs an explicit one", func(t *testing.T) {
		c := qt.New(t)
		config := fixtureConfig(c)
		config.Providers[0].Engines = append(config.Providers[0].Engines, providerconfig.Engine{ID: "second"})
		choice, err := providerselect.Choose(providerselect.Request{Config: config, Configured: true, Resolver: emptyResolver()})
		c.Assert(err, qt.ErrorMatches, `--provider-engine is required.*`)
		c.Assert(choice, qt.IsNil)
	})
	t.Run("a disabled entry is not replaced by another provider", func(t *testing.T) {
		c := qt.New(t)
		config := fixtureConfig(c)
		config.Providers[0].Disabled = true
		choice, err := providerselect.Choose(providerselect.Request{
			Config: config, Configured: true,
			Targets:  []providerselect.Target{{Flag: "db-url", Value: "fixture:///x"}},
			Resolver: emptyResolver(),
		})
		c.Assert(err, qt.ErrorMatches, `(?s).*disabled.*`)
		c.Assert(choice, qt.IsNil)
	})
}

func TestStart_FailurePath(t *testing.T) {
	c := qt.New(t)
	choice, err := providerselect.Choose(providerselect.Request{
		Config: fixtureConfig(c), Configured: true,
		Targets:  []providerselect.Target{{Flag: "db-url", Value: "fixture:///x"}},
		Resolver: emptyResolver(),
	})
	c.Assert(err, qt.IsNil)
	started, err := choice.Start(c.Context(), providerselect.Exclusive)
	c.Assert(err, qt.ErrorMatches, `(?s).*executable was not found.*`)
	c.Assert(started, qt.IsNil)
}

func TestSelector(t *testing.T) {
	tests := []struct {
		name    string
		targets []providerselect.Target
		want    string
	}{
		{"no targets", nil, ""},
		{"blank values", []providerselect.Target{{Flag: "db-url", Value: "  "}}, ""},
		{"a URL scheme", []providerselect.Target{{Flag: "db-url", Value: "Fixture://h/db"}}, "fixture"},
		{"a catalog name", []providerselect.Target{{Flag: "dialect", Value: "ydb"}}, "ydb"},
		{"a dialect that is no URL", []providerselect.Target{{Flag: "dialect", Value: "Fixture"}}, "fixture"},
		{"a docker dev URL", []providerselect.Target{{Flag: "dev-url", Value: "docker://fixture/1?x=y"}}, "fixture"},
		{"a docker URL on another flag is a scheme", []providerselect.Target{{Flag: "db-url", Value: "docker://fixture/1"}}, "docker"},
		{"the first target that names an engine wins", []providerselect.Target{{Flag: "db-url", Value: ""}, {Flag: "dialect", Value: "first"}, {Flag: "dev-url", Value: "docker://second/1"}}, "first"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(providerselect.Selector(test.targets), qt.Equals, test.want)
		})
	}
}
