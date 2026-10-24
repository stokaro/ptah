//go:build integration

package providercli_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerselect"
	"ptah.run/internal/testutils"
)

// TestProviderSelectStart starts the independently built fixture provider
// through the shared resolver and reads back what the started runtime holds.
func TestProviderSelectStart(t *testing.T) {
	c := qt.New(t)
	binary := testutils.BuildFixtureProvider(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	spec := providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}

	choose := func(c *qt.C, f providerCLI) *providerselect.Choice {
		c.Helper()
		config, err := providerconfig.Load(f.manifestPath)
		c.Assert(err, qt.IsNil)
		choice, err := providerselect.Choose(providerselect.Request{
			Config: config, Configured: true,
			Targets:  []providerselect.Target{{Flag: "db-url", Value: f.address}},
			Resolver: &providerconfig.Resolver{},
		})
		c.Assert(err, qt.IsNil)
		c.Assert(choice, qt.IsNotNil)
		return choice
	}

	t.Run("the started runtime holds only the provider", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		started, err := choose(c, f).Start(c.Context(), providerselect.Exclusive)
		c.Assert(err, qt.IsNil)
		c.Cleanup(started.Close)
		_, err = started.Runtime.ResolveTarget("fixture")
		c.Assert(err, qt.IsNil)
		_, err = started.Runtime.ResolveTarget("sqlite")
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		c.Assert(started.Connections, qt.HasLen, 1)
	})
	t.Run("additive mode keeps the built-in engines", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		started, err := choose(c, f).Start(c.Context(), providerselect.Additive)
		c.Assert(err, qt.IsNil)
		c.Cleanup(started.Close)
		_, err = started.Runtime.ResolveTarget("fixture")
		c.Assert(err, qt.IsNil)
		_, err = started.Runtime.ResolveTarget("sqlite")
		c.Assert(err, qt.IsNil)
	})
	t.Run("closing ends the process and is repeatable", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		started, err := choose(c, f).Start(c.Context(), providerselect.Exclusive)
		c.Assert(err, qt.IsNil)
		started.Close()
		started.Close()
		f.assertStopped(c)
	})
}

// lazySession returns a Lazy over the fixture manifest and a context that
// carries it, closed with the test.
func lazySession(c *qt.C, f providerCLI) (*providerselect.Lazy, context.Context) {
	c.Helper()
	config, err := providerconfig.Load(f.manifestPath)
	c.Assert(err, qt.IsNil)
	lazy := providerselect.NewLazy(config, true, &providerconfig.Resolver{})
	c.Cleanup(lazy.Close)
	return lazy, lazy.With(c.Context())
}

func sqliteAddress(c *qt.C) string {
	c.Helper()
	return atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "builtin.db"))
}

// fixtureSpec builds one command of the fixture module and describes it as an
// installation.
func fixtureSpec(c *qt.C, command string) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, command)
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
}

func TestLazyProviderSession_HappyPath(t *testing.T) {
	spec := fixtureSpec(qt.New(t), "locks")
	devSpec := fixtureSpec(qt.New(t), "devdb")

	t.Run("nothing starts until a connection needs the provider", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		_, ctx := lazySession(c, f)

		builtIn, err := dbschema.ConnectToDatabase(ctx, sqliteAddress(c))
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { c.Check(builtIn.Close(), qt.IsNil) })
		c.Assert(builtIn.Info().Dialect, qt.Equals, "sqlite")
		_, err = os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("a provider scheme and a built-in scheme connect side by side", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		lazy, ctx := lazySession(c, f)

		served, err := dbschema.ConnectToDatabase(ctx, f.address)
		c.Assert(err, qt.IsNil)
		first, err := os.ReadFile(f.pidPath)
		c.Assert(err, qt.IsNil)
		builtIn, err := dbschema.ConnectToDatabase(ctx, sqliteAddress(c))
		c.Assert(err, qt.IsNil)
		again, err := dbschema.ConnectToDatabase(ctx, f.address)
		c.Assert(err, qt.IsNil)

		c.Assert(served.Info().Dialect, qt.Equals, "fixture")
		c.Assert(builtIn.Info().Dialect, qt.Equals, "sqlite")
		second, err := os.ReadFile(f.pidPath)
		c.Assert(err, qt.IsNil)
		c.Assert(string(second), qt.Equals, string(first), qt.Commentf("a second connection started a second process"))
		c.Assert(again.Close(), qt.IsNil)
		c.Assert(builtIn.Close(), qt.IsNil)
		c.Assert(served.Close(), qt.IsNil)
		lazy.Close()
		f.assertStopped(c)
	})
	t.Run("a docker dev URL of the provider's engine is provisioned and released", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, devSpec)
		lazy, ctx := lazySession(c, f)
		c.Assert(lazy.DevEngines(), qt.Contains, "fixture")

		resolved, release, err := devdocker.Resolve(ctx, "docker://fixture/1", devdocker.Options{})

		c.Assert(err, qt.IsNil)
		c.Assert(resolved, qt.Matches, `fixture://localhost/lease-\d+`)
		c.Assert(devdocker.Provisioned(resolved), qt.IsTrue)
		release()
		c.Assert(devdocker.Provisioned(resolved), qt.IsFalse)
	})
	t.Run("the runtime holds the provider and the built-in engines", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		lazy, ctx := lazySession(c, f)

		runtime, err := lazy.Runtime(ctx, providerselect.Target{Flag: "db-url", Value: f.address})

		c.Assert(err, qt.IsNil)
		_, err = runtime.ResolveTarget("fixture")
		c.Assert(err, qt.IsNil)
		_, err = runtime.ResolveTarget("sqlite")
		c.Assert(err, qt.IsNil)
	})
	t.Run("targets that name no provider engine leave the runtime built in", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		lazy, ctx := lazySession(c, f)

		runtime, err := lazy.Runtime(ctx,
			providerselect.Target{Flag: "db-url", Value: sqliteAddress(c)},
			providerselect.Target{Flag: "db-url", Value: "file://migrations"},
			providerselect.Target{Flag: "dev-url", Value: "docker://postgres/16"},
		)

		c.Assert(err, qt.IsNil)
		_, err = runtime.ResolveTarget("fixture")
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		_, err = os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
}

func TestLazyProviderSession_FailurePath(t *testing.T) {
	spec := fixtureSpec(qt.New(t), "locks")

	t.Run("an exclusive native selection refuses the built-in scheme", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		choice := chooseFixture(c, f)
		started, err := choice.Start(c.Context(), providerselect.Exclusive)
		c.Assert(err, qt.IsNil)
		c.Cleanup(started.Close)
		ctx, err := dbschema.WithNativeConnections(c.Context(), started.Connections)
		c.Assert(err, qt.IsNil)

		conn, err := dbschema.ConnectToDatabase(ctx, sqliteAddress(c))

		c.Assert(err, qt.ErrorMatches, `the selected provider does not own the database URL scheme`)
		c.Assert(conn, qt.IsNil)
	})
	t.Run("a connection after the session closed starts nothing", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		lazy, ctx := lazySession(c, f)
		lazy.Close()

		conn, err := dbschema.ConnectToDatabase(ctx, f.address)

		c.Assert(err, qt.ErrorIs, providerselect.ErrClosed)
		c.Assert(conn, qt.IsNil)
		_, err = os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("a failed start is remembered and not retried", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		changeManifest(c, f, func(config *providerconfig.Configuration) { config.Providers[0].SHA256 = strings.Repeat("a", 64) })
		_, ctx := lazySession(c, f)

		_, first := dbschema.ConnectToDatabase(ctx, f.address)
		_, second := dbschema.ConnectToDatabase(ctx, f.address)

		c.Assert(first, qt.ErrorMatches, `(?s).*does not match its trusted SHA-256 checksum.*`)
		c.Assert(second, qt.Equals, first)
		_, err := os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
}

// chooseFixture decides the provider of the fixture manifest's address.
func chooseFixture(c *qt.C, f providerCLI) *providerselect.Choice {
	c.Helper()
	config, err := providerconfig.Load(f.manifestPath)
	c.Assert(err, qt.IsNil)
	choice, err := providerselect.Choose(providerselect.Request{
		Config: config, Configured: true,
		Targets:  []providerselect.Target{{Flag: "db-url", Value: f.address}},
		Resolver: &providerconfig.Resolver{},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(choice, qt.IsNotNil)
	return choice
}
