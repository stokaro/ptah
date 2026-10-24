package providerselect_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerselect"
)

func newLazy(c *qt.C) *providerselect.Lazy {
	c.Helper()
	return providerselect.NewLazy(fixtureConfig(c), true, emptyResolver())
}

func TestLazyServes(t *testing.T) {
	tests := []struct {
		name, scheme string
		want         bool
	}{
		{"a manifest engine", "fixture", true},
		{"an alias of a manifest engine", "fix", true},
		{"an upper-case scheme", "FIXTURE", true},
		{"an official engine with no built-in implementation", "duckdb", true},
		{"a built-in engine", "sqlite", false},
		{"an official engine that is still built in", "ydb", false},
		{"a scheme nothing claims", "nothing", false},
		{"an empty scheme", "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(newLazy(c).Serves(test.scheme), qt.Equals, test.want)
		})
	}
	t.Run("no session", func(t *testing.T) {
		c := qt.New(t)
		var none *providerselect.Lazy
		c.Assert(none.Serves("fixture"), qt.IsFalse)
	})
}

func TestLazyListsWhatItServesWithoutStartingAnything(t *testing.T) {
	c := qt.New(t)
	lazy := newLazy(c)

	c.Assert(lazy.NativeSchemes(), qt.DeepEquals, map[string]string{"fixture": "fixture", "fix": "fixture", "duckdb": "duckdb"})
	c.Assert(lazy.DevEngines(), qt.DeepEquals, []string{"duckdb", "fix", "fixture"})
}

func TestLazyNativeRegistration_HappyPath(t *testing.T) {
	t.Run("a built-in scheme is not served and starts nothing", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)

		registration, found, err := lazy.NativeRegistration(c.Context(), "sqlite")

		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsFalse)
		c.Assert(registration.Connector, qt.IsNil)
	})
	t.Run("a built-in scheme is still not served after the session closed", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)
		lazy.Close()

		_, found, err := lazy.NativeRegistration(c.Context(), "sqlite")

		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsFalse)
	})
	t.Run("the session round-trips through a context", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)

		c.Assert(providerselect.FromContext(lazy.With(c.Context())), qt.Equals, lazy)
		c.Assert(providerselect.FromContext(c.Context()), qt.IsNil)
	})
}

func TestLazyNativeRegistration_FailurePath(t *testing.T) {
	t.Run("a provider that cannot start fails the same way every time", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)

		_, found, first := lazy.NativeRegistration(c.Context(), "fixture")
		_, _, second := lazy.NativeRegistration(c.Context(), "fixture")

		c.Assert(first, qt.ErrorMatches, `(?s).*executable was not found.*`)
		c.Assert(found, qt.IsFalse)
		c.Assert(second, qt.Equals, first)
	})
	t.Run("a provider scheme after the session closed is refused", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)
		lazy.Close()

		_, found, err := lazy.NativeRegistration(c.Context(), "fixture")

		c.Assert(err, qt.ErrorIs, providerselect.ErrClosed)
		c.Assert(found, qt.IsFalse)
	})
	t.Run("an official engine with no pinned provider is reported as missing", func(t *testing.T) {
		c := qt.New(t)
		lazy := providerselect.NewLazy(providerconfig.Configuration{}, false, emptyResolver())

		_, found, err := lazy.NativeRegistration(c.Context(), "duckdb")

		c.Assert(err, qt.ErrorMatches, `(?s).*pins no provider release.*`)
		c.Assert(found, qt.IsFalse)
	})
}

func TestLazyRuntime(t *testing.T) {
	t.Run("targets that name no provider engine leave the runtime built in", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)

		runtime, err := lazy.Runtime(c.Context(),
			providerselect.Target{Flag: "db-url", Value: "sqlite:///x.db"},
			providerselect.Target{Flag: "db-url", Value: "file://migrations"},
			providerselect.Target{Flag: "db-url", Value: "  "},
		)

		c.Assert(err, qt.IsNil)
		_, err = runtime.ResolveTarget("sqlite")
		c.Assert(err, qt.IsNil)
	})
	t.Run("a provider target that cannot start is an error", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)

		runtime, err := lazy.Runtime(c.Context(), providerselect.Target{Flag: "db-url", Value: "fixture:///x"})

		c.Assert(err, qt.ErrorMatches, `(?s).*executable was not found.*`)
		c.Assert(runtime, qt.IsNil)
	})
}

func TestLazyProvisionDev_FailurePath(t *testing.T) {
	t.Run("a dev URL naming no engine is refused", func(t *testing.T) {
		c := qt.New(t)
		_, _, err := newLazy(c).ProvisionDev(c.Context(), "postgres://host/db")
		c.Assert(err, qt.ErrorMatches, `the dev URL names no engine`)
	})
	t.Run("a provider that cannot start is reported", func(t *testing.T) {
		c := qt.New(t)
		_, _, err := newLazy(c).ProvisionDev(c.Context(), "docker://fixture/1")
		c.Assert(err, qt.ErrorMatches, `(?s).*executable was not found.*`)
	})
	t.Run("a closed session starts nothing", func(t *testing.T) {
		c := qt.New(t)
		lazy := newLazy(c)
		lazy.Close()
		_, _, err := lazy.ProvisionDev(c.Context(), "docker://fixture/1")
		c.Assert(err, qt.ErrorIs, providerselect.ErrClosed)
	})
}
