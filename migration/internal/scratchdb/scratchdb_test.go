package scratchdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/migration/internal/scratchdb"
)

// TestCanIsolate_HappyPath pins the engines a case can get a database of its
// own on: a file on SQLite, a dev realm on YDB, and a database the server
// creates on the PostgreSQL and MySQL families.
func TestCanIsolate_HappyPath(t *testing.T) {
	for _, baseURL := range []string{
		"",
		"sqlite://file.db",
		"ydb://localhost:2136/local",
		"postgres://localhost:5432/app",
		"cockroachdb://localhost:26257/app",
		"yugabytedb://localhost:5433/app",
		"spanner://localhost:5432/app",
		"mysql://localhost:3306/app",
		"mariadb://localhost:3306/app",
	} {
		t.Run(baseURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(scratchdb.CanIsolate(c.Context(), baseURL), qt.IsNil)
		})
	}
}

// TestCanIsolate_FailurePath pins the engines that cannot isolate a case,
// which are refused rather than shared.
func TestCanIsolate_FailurePath(t *testing.T) {
	for _, baseURL := range []string{
		"clickhouse://localhost:9000/app",
		"sqlserver://localhost:1433?database=app",
		"oracle://localhost:1521/app",
	} {
		t.Run(baseURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(scratchdb.CanIsolate(c.Context(), baseURL), qt.ErrorIs, scratchdb.ErrNoIsolation)
		})
	}
}

// realmOwner stands for a provider that creates realms for the URL schemes it
// lists. It enters a realm at realm, or answers with err.
type realmOwner struct {
	schemes  []string
	realm    string
	err      error
	entered  []string
	released int
}

func (*realmOwner) DevEngines() []string { return nil }
func (*realmOwner) ProvisionDev(context.Context, string) (string, func(), error) {
	return "", nil, errors.New("the owner provisions no dev database here")
}
func (o *realmOwner) RealmSchemes() []string { return o.schemes }
func (o *realmOwner) EnterRealm(_ context.Context, rawURL string) (string, func(), error) {
	o.entered = append(o.entered, rawURL)
	if o.err != nil {
		return "", nil, o.err
	}
	return o.realm, func() { o.released++ }, nil
}

// A URL whose scheme a provider creates realms for can be isolated, whatever
// the built-in dialects know of the scheme.
func TestCanIsolateWithAProviderRealm_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &realmOwner{schemes: []string{"fixture"}}

	err := scratchdb.CanIsolate(devdocker.WithExternal(c.Context(), owner), "fixture://localhost/app.db")

	c.Assert(err, qt.IsNil)
	c.Assert(owner.entered, qt.HasLen, 0)
}

// Without the owner, the same URL names a dialect nothing here knows.
func TestCanIsolateWithAProviderRealm_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := &realmOwner{schemes: []string{"other"}}

	err := scratchdb.CanIsolate(devdocker.WithExternal(c.Context(), owner), "fixture://localhost/app.db")

	c.Assert(err, qt.ErrorMatches, `read the dialect of the scratch server URL: .*`)
}

// A case on an engine a provider serves gets a realm from the provider, which
// is asked before the engine's built-in isolation, and closing the scratch
// releases the realm once. The built-in isolation would dial the address, so
// the deadline ends that attempt rather than the test's own timeout.
func TestProvisionAProviderRealm_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &realmOwner{schemes: []string{"ydb"}, realm: "ydb://localhost:2136/local?dev_realm=one"}
	ctx, cancel := context.WithTimeout(devdocker.WithExternal(c.Context(), owner), 5*time.Second)
	defer cancel()

	scratch, err := scratchdb.Provision(ctx, "ydb://localhost:2136/local")

	c.Assert(err, qt.IsNil)
	c.Assert(scratch.URL(), qt.Equals, "ydb://localhost:2136/local?dev_realm=one")
	c.Assert(owner.entered, qt.DeepEquals, []string{"ydb://localhost:2136/local"})
	c.Assert(scratch.Close(c.Context()), qt.IsNil)
	c.Assert(scratch.Close(c.Context()), qt.IsNil)
	c.Assert(owner.released, qt.Equals, 1)
}

// A provider that creates no realm in the database the URL names returns the
// URL unchanged, and the case is refused rather than given that database to
// share. A realm the provider could not create is the provider's error.
func TestProvisionAProviderRealm_FailurePath(t *testing.T) {
	t.Run("no realm in the database", func(t *testing.T) {
		c := qt.New(t)
		owner := &realmOwner{schemes: []string{"fixture"}, realm: "fixture:///app.db"}

		scratch, err := scratchdb.Provision(devdocker.WithExternal(c.Context(), owner), "fixture:///app.db")

		c.Assert(err, qt.ErrorIs, scratchdb.ErrNoIsolation)
		c.Assert(err, qt.ErrorMatches, `this dialect has no per-case database isolation: the provider creates no realm in the database this URL names`)
		c.Assert(scratch, qt.IsNil)
		c.Assert(owner.released, qt.Equals, 1)
	})
	t.Run("a realm the provider could not create", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("the database could not take a realm")
		owner := &realmOwner{schemes: []string{"fixture"}, err: refused}

		scratch, err := scratchdb.Provision(devdocker.WithExternal(c.Context(), owner), "fixture://localhost/app.db")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `create a scratch dev realm: the database could not take a realm`)
		c.Assert(scratch, qt.IsNil)
	})
}
