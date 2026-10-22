//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/providerhost"
)

func TestProviderProvisionsAndReleasesADevDatabase(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	owner := host.DevDatabases()
	c.Assert(owner, qt.IsNotNil)
	c.Assert(owner.DevEngines(), qt.DeepEquals, []string{"fixture"})
	ctx := devdocker.WithExternal(c.Context(), owner)

	first, releaseFirst, err := devdocker.Resolve(ctx, "docker://fixture/1", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	second, releaseSecond, err := devdocker.Resolve(ctx, "docker://fixture", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.Not(qt.Equals), second)
	c.Assert(first, qt.Matches, `fixture://localhost/lease-\d+`)
	c.Assert(devdocker.Provisioned(first), qt.IsTrue)

	releaseFirst()
	c.Assert(devdocker.Provisioned(first), qt.IsFalse)
	c.Assert(devdocker.Provisioned(second), qt.IsTrue)
	releaseSecond()
	c.Assert(devdocker.Provisioned(second), qt.IsFalse)
}

func TestProviderRefusalReachesTheOperator(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	ctx := devdocker.WithExternal(c.Context(), host.DevDatabases())

	resolved, release, err := devdocker.Resolve(ctx, "docker://fixture/none", devdocker.Options{})
	var refused *providerhost.DevDatabaseRefusedError
	c.Assert(err, qt.ErrorAs, &refused)
	c.Assert(refused.Engine, qt.Equals, "fixture")
	c.Assert(refused.Unavailable, qt.IsFalse)
	c.Assert(refused.Reason, qt.Contains, "the fixture provisions no database")
	c.Assert(resolved, qt.Equals, "")
	release()
}

func TestProviderUnavailableRuntimeIsReportedAsUnavailable(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	ctx := devdocker.WithExternal(c.Context(), host.DevDatabases())

	_, release, err := devdocker.Resolve(ctx, "docker://fixture/offline", devdocker.Options{})
	var refused *providerhost.DevDatabaseRefusedError
	c.Assert(err, qt.ErrorAs, &refused)
	c.Assert(refused.Unavailable, qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `provider engine fixture cannot provision a dev database now: the fixture runtime is not running`)
	release()
}

func TestProviderWithoutTheServiceOffersNoOwner(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "lint")
	c.Assert(host.DevDatabases(), qt.IsNil)
}
