package engineruntime_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerselect"
)

func TestExplicitRuntimeDoesNotFallBackToBundledTargets(t *testing.T) {
	c := qt.New(t)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)
	selected, err := engineruntime.With(c.Context(), empty)
	c.Assert(err, qt.IsNil)
	actual, err := engineruntime.Resolve(selected)
	c.Assert(err, qt.IsNil)
	c.Assert(actual, qt.Equals, empty)
	_, err = actual.ResolveTarget("sqlite")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	plain, err := engineruntime.Resolve(c.Context())
	c.Assert(err, qt.IsNil)
	_, err = plain.ResolveTarget("sqlite")
	c.Assert(err, qt.IsNil)
	invalid, err := engineruntime.With(c.Context(), nil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(invalid, qt.IsNil)
}

// A selection is reported as one, and a context without one reports none.
func TestSelectedReportsOnlyAnExplicitRuntime(t *testing.T) {
	c := qt.New(t)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)
	selected, err := engineruntime.With(c.Context(), empty)
	c.Assert(err, qt.IsNil)

	actual, found := engineruntime.Selected(selected)
	c.Assert(found, qt.IsTrue)
	c.Assert(actual, qt.Equals, empty)

	none, found := engineruntime.Selected(c.Context())
	c.Assert(found, qt.IsFalse)
	c.Assert(none, qt.IsNil)
}

// fixtureSession is a lazy provider session whose manifest claims the fixture
// engine for an executable that does not exist, so a target naming it fails
// to start and nothing else starts.
func fixtureSession(c *qt.C) context.Context {
	c.Helper()
	config := providerconfig.Configuration{
		FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{
			Spec:    providerhost.Spec{ProviderID: "example.org/fixture", Executable: filepath.Join(c.TempDir(), "missing"), SHA256: strings.Repeat("a", 64)},
			Engines: []providerconfig.Engine{{ID: "fixture"}},
		}},
	}
	session := providerselect.NewLazy(config, true, &providerconfig.Resolver{})
	c.Cleanup(session.Close)
	return session.With(c.Context())
}

// Under a session that has started nothing, the runtime is the shared bundled
// one, whatever built-in engines the targets name.
func TestResolve_HappyPath_ASessionThatStartedNothingIsTheBundledRuntime(t *testing.T) {
	c := qt.New(t)
	bundled, err := builtin.Bundled()
	c.Assert(err, qt.IsNil)

	runtime, err := engineruntime.Resolve(fixtureSession(c),
		engineruntime.DatabaseURL("sqlite:///x.db"), engineruntime.Dialect("postgres"), engineruntime.DevURL(""))

	c.Assert(err, qt.IsNil)
	c.Assert(runtime, qt.Equals, bundled)
}

// A target naming an engine the session serves starts its provider before the
// runtime is built, so a provider that cannot start fails the resolution.
func TestResolve_FailurePath_ATargetTheSessionServesStartsItsProvider(t *testing.T) {
	for _, target := range []providerselect.Target{
		engineruntime.DatabaseURL("fixture:///x.db"),
		engineruntime.Dialect("fixture"),
		engineruntime.DevURL("docker://fixture/1"),
	} {
		t.Run(target.Flag, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engineruntime.Resolve(fixtureSession(c), target)

			c.Assert(err, qt.ErrorMatches, `(?s).*executable was not found.*`)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// An explicit selection is returned as it is, even under a session.
func TestResolve_HappyPath_AnExplicitSelectionWinsOverASession(t *testing.T) {
	c := qt.New(t)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)
	selected, err := engineruntime.With(fixtureSession(c), empty)
	c.Assert(err, qt.IsNil)

	runtime, err := engineruntime.Resolve(selected, engineruntime.Dialect("fixture"))

	c.Assert(err, qt.IsNil)
	c.Assert(runtime, qt.Equals, empty)
}

func TestTargetName(t *testing.T) {
	c := qt.New(t)
	bundled, err := builtin.Bundled()
	c.Assert(err, qt.IsNil)
	for _, row := range []struct{ name, want string }{
		{"postgres", "postgres"},
		{" PostgreSQL ", "postgres"},
		{"crdb", "cockroachdb"},
		{"", ""},
		{"nosuch", ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(engineruntime.TargetName(bundled, row.name), qt.Equals, row.want)
		})
	}
}
