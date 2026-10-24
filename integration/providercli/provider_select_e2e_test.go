//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerselect"
)

// TestProviderSelectStart starts the independently built fixture provider
// through the shared resolver and reads back what the started runtime holds.
func TestProviderSelectStart(t *testing.T) {
	c := qt.New(t)
	binary := independentCLIProvider(c)
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
		started, err := choose(c, f).Start(c.Context())
		c.Assert(err, qt.IsNil)
		c.Cleanup(started.Close)
		_, err = started.Runtime.ResolveTarget("fixture")
		c.Assert(err, qt.IsNil)
		_, err = started.Runtime.ResolveTarget("sqlite")
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		c.Assert(started.Connections, qt.HasLen, 1)
	})
	t.Run("closing ends the process and is repeatable", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		started, err := choose(c, f).Start(c.Context())
		c.Assert(err, qt.IsNil)
		started.Close()
		started.Close()
		f.assertStopped(c)
	})
}
