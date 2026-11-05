package devdocker_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// realmOwner provisions dev databases for its engine and creates realms in a
// database of its schemes.
type realmOwner struct {
	fakeOwner
	schemes  []string
	realm    string
	entered  []string
	released int
}

func (o *realmOwner) RealmSchemes() []string { return o.schemes }

func (o *realmOwner) EnterRealm(_ context.Context, rawURL string) (string, func(), error) {
	o.entered = append(o.entered, rawURL)
	return o.realm, func() { o.released++ }, nil
}

// The `docker+<engine>://` form names the engine in its scheme, and goes to
// the owner of that engine as `docker://<engine>/...` does.
func TestResolveHandsTheDockerPlusFormToItsOwner(t *testing.T) {
	c := qt.New(t)
	owner := &fakeOwner{engines: []string{"owned"}, url: "owned://localhost/dev-1"}
	ctx := devdocker.WithExternal(c.Context(), owner)

	resolved, release, err := devdocker.Resolve(ctx, "docker+owned://_/example.org/owned:1.2/dev", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, "owned://localhost/dev-1")
	c.Assert(owner.requests, qt.DeepEquals, []string{"docker+owned://_/example.org/owned:1.2/dev"})
	c.Assert(devdocker.Provisioned(resolved), qt.IsTrue)
	release()
	c.Assert(devdocker.Provisioned(resolved), qt.IsFalse)
}

func TestDockerEngineReadsBothForms(t *testing.T) {
	for _, row := range []struct {
		url    string
		engine string
		found  bool
	}{
		{"docker://owned/1.2/dev", "owned", true},
		{"docker+owned://_/img:1/dev", "owned", true},
		{"DOCKER+Owned://_/img", "owned", true},
		{"docker+://_/img", "", false},
		{"owned://localhost/dev", "", false},
	} {
		t.Run(row.url, func(t *testing.T) {
			c := qt.New(t)
			engine, found := devdocker.DockerEngine(row.url)
			c.Assert(engine, qt.Equals, row.engine)
			c.Assert(found, qt.Equals, row.found)
		})
	}
}

// A URL of an engine whose owner creates realms becomes a realm the owner
// creates in it, which the release removes. It is not a server the run owns.
func TestResolveEntersAnOwnersRealm(t *testing.T) {
	c := qt.New(t)
	owner := &realmOwner{fakeOwner: fakeOwner{engines: []string{"owned"}}, schemes: []string{"owned"}, realm: "owned://host/db/realm-1"}
	ctx := devdocker.WithExternal(c.Context(), owner)

	resolved, release, err := devdocker.Resolve(ctx, "owned://host/db", devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, "owned://host/db/realm-1")
	c.Assert(owner.entered, qt.DeepEquals, []string{"owned://host/db"})
	c.Assert(devdocker.RunOwned(resolved), qt.IsFalse)
	release()
	release()
	c.Assert(owner.released, qt.Equals, 1)
}

// A server the operator declared disposable is the run's whole and gets no
// realm, and a URL of another scheme passes through.
func TestResolveEntersNoRealmOutsideItsSchemes(t *testing.T) {
	for _, row := range []struct {
		url        string
		disposable bool
	}{
		{"owned://host/db", true},
		{"other://host/db", false},
	} {
		t.Run(row.url, func(t *testing.T) {
			c := qt.New(t)
			owner := &realmOwner{fakeOwner: fakeOwner{engines: []string{"owned"}}, schemes: []string{"owned"}, realm: "owned://host/db/realm-1"}
			ctx := devdocker.WithExternal(c.Context(), owner)
			resolved, release, err := devdocker.Resolve(ctx, row.url, devdocker.Options{DeclaredDisposable: row.disposable})
			c.Assert(err, qt.IsNil)
			c.Cleanup(release)
			c.Assert(resolved, qt.Equals, row.url)
			c.Assert(owner.entered, qt.HasLen, 0)
		})
	}
}

// A docker URL, and a URL of an engine whose owner in the context creates
// dev realms, each get a database Resolve hands out per run, so neither is
// compared by its text.
func TestResolvedPerRun_HappyPath(t *testing.T) {
	for _, rawURL := range []string{
		"docker://postgres/16/dev",
		"docker+postgres://_/postgres:17/dev",
		"owned://host/db",
		"OWNED://host/db",
	} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			owner := &realmOwner{fakeOwner: fakeOwner{engines: []string{"owned"}}, schemes: []string{"owned"}}
			ctx := devdocker.WithExternal(c.Context(), owner)
			c.Assert(devdocker.ResolvedPerRun(ctx, rawURL), qt.IsTrue)
		})
	}
}

// Every other URL names the dev database it is: one of a built-in engine, and
// one of a scheme no owner in the context creates realms for.
func TestResolvedPerRun_FailurePath(t *testing.T) {
	for _, rawURL := range []string{"postgres://localhost/dev", "mysql://root@tcp(localhost:3306)/dev", "sqlite://dev.db", "other://host/db", ""} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			owner := &realmOwner{fakeOwner: fakeOwner{engines: []string{"owned"}}, schemes: []string{"owned"}}
			ctx := devdocker.WithExternal(c.Context(), owner)
			c.Assert(devdocker.ResolvedPerRun(ctx, rawURL), qt.IsFalse)
		})
	}
}

// Without an owner in the context no URL but a docker one is resolved per
// run: the scheme alone says nothing about realms.
func TestResolvedPerRun_NoOwnerInTheContext(t *testing.T) {
	c := qt.New(t)
	c.Assert(devdocker.ResolvedPerRun(c.Context(), "owned://host/db"), qt.IsFalse)
	c.Assert(devdocker.ResolvedPerRun(c.Context(), "docker://postgres/16/dev"), qt.IsTrue)
}
