package devdocker_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// docker://ydb starts local-ydb, which serves the one database local: a URL
// naming no database names it, and the connectable URL disables the balancer,
// since the server advertises a host name only the container resolves.
func TestParseYDB_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		rawURL    string
		wantImage string
		wantURL   string
	}{
		{name: "a tag and the database", rawURL: "docker://ydb/26.2.1.14/local",
			wantImage: "ydbplatform/local-ydb:26.2.1.14", wantURL: "ydb://127.0.0.1:12136/local?go_balancer=disable"},
		{name: "a tag alone", rawURL: "docker://ydb/25.1.4.7",
			wantImage: "ydbplatform/local-ydb:25.1.4.7", wantURL: "ydb://127.0.0.1:12136/local?go_balancer=disable"},
		{name: "no path", rawURL: "docker://ydb",
			wantImage: "ydbplatform/local-ydb:latest", wantURL: "ydb://127.0.0.1:12136/local?go_balancer=disable"},
		{name: "an operator parameter is kept", rawURL: "docker://ydb/26.2.1.14/local?go_query_mode=query",
			wantImage: "ydbplatform/local-ydb:26.2.1.14",
			wantURL:   "ydb://127.0.0.1:12136/local?go_balancer=disable&go_query_mode=query"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			spec, err := devdocker.Parse(test.rawURL)

			c.Assert(err, qt.IsNil)
			c.Assert(spec.Dialect, qt.Equals, "ydb")
			c.Assert(spec.Image, qt.Equals, test.wantImage)
			c.Assert(spec.Database, qt.Equals, "local")
			c.Assert(spec.Port(), qt.Equals, "2136")
			c.Assert(spec.Anonymous(), qt.IsTrue)
			c.Assert(spec.CreatesDatabase(), qt.IsFalse)
			c.Assert(spec.Env(testPassword), qt.DeepEquals, []string{"YDB_USE_IN_MEMORY_PDISKS=true"})
			c.Assert(spec.URL("127.0.0.1:12136", testPassword), qt.Equals, test.wantURL)
		})
	}
}

// A database local-ydb does not serve, and a parameter that would point the
// connection somewhere other than the container, are refused before anything
// starts; the engine is matched as written.
func TestParseYDB_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "another database", rawURL: "docker://ydb/26.2.1.14/dev",
			wantErr: `docker --dev-url database "dev" is not one the ydbplatform/local-ydb image serves: name local`},
		{name: "a database parameter", rawURL: "docker://ydb/26.2.1.14/local?database=/other",
			wantErr: `docker --dev-url parameter "database" would point the connection away from the container .*`},
		{name: "a dev realm", rawURL: "docker://ydb/26.2.1.14/local?dev_realm=r1",
			wantErr: `docker --dev-url parameter "dev_realm" would point the connection away from the container .*`},
		{name: "a monitoring endpoint", rawURL: "docker://ydb/26.2.1.14/local?monitoring=http://h:8765",
			wantErr: `docker --dev-url parameter "monitoring" would point the connection away from the container .*`},
		{name: "the engine in upper case", rawURL: "docker://YDB/26.2.1.14/local", wantErr: `unsupported docker image "YDB"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			spec, err := devdocker.Parse(test.rawURL)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(spec.Engine, qt.Equals, "")
			c.Assert(spec.Image, qt.Equals, "")
		})
	}
}

// A runtime on this machine starts local-ydb, published on loopback.
func TestProvisionYDB_HappyPath(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:12136"}

	instance, err := devdocker.Provision(context.Background(), "docker://ydb/26.2.1.14/local", devdocker.Options{
		Runner: runner, Ready: alwaysReady, ReadyTimeout: time.Second,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(instance.Close(), qt.IsNil) })

	c.Assert(instance.URL(), qt.Equals, "ydb://127.0.0.1:12136/local?go_balancer=disable")
	c.Assert(runner.startedImages, qt.DeepEquals, []string{"ydbplatform/local-ydb:26.2.1.14"})
	c.Assert(devdocker.RunOwned(instance.URL()), qt.IsTrue)
}

// silentRunner is a runner that cannot say where it publishes a port.
type silentRunner struct{ inner *fakeRunner }

func (r silentRunner) Available(ctx context.Context) error { return r.inner.Available(ctx) }
func (r silentRunner) Start(ctx context.Context, name, image, port string, env []string) (string, error) {
	return r.inner.Start(ctx, name, image, port, env)
}
func (r silentRunner) Remove(ctx context.Context, name string) error { return r.inner.Remove(ctx, name) }
func (r silentRunner) Stopped(ctx context.Context, name string) (string, error) {
	return r.inner.Stopped(ctx, name)
}
func (r silentRunner) Build(ctx context.Context, image string, build devdocker.Build) error {
	return r.inner.Build(ctx, image, build)
}
func (r silentRunner) RemoveImage(ctx context.Context, image string) error {
	return r.inner.RemoveImage(ctx, image)
}

// local-ydb takes every connection without a credential, so a runtime on
// another machine, which would publish it on every interface of that host,
// starts nothing, and neither does a runtime that cannot say where it runs.
func TestProvisionYDB_FailurePath(t *testing.T) {
	t.Run("a runtime on another machine", func(t *testing.T) {
		c := qt.New(t)
		runner := &fakeRunner{hostPort: "remote-dev:12136", remoteHost: "remote-dev"}

		instance, err := devdocker.Provision(context.Background(), "docker://ydb/26.2.1.14/local", devdocker.Options{
			Runner: runner, Ready: alwaysReady,
		})

		c.Assert(err, qt.ErrorMatches, `docker://ydb starts ydbplatform/local-ydb:26.2.1.14, which takes every `+
			`connection without a credential, and the container runtime runs on remote-dev, where the database `+
			`would be published on every interface for the life of the command; point DOCKER_HOST at a container `+
			`runtime on this machine, or pass a directly connectable dev database URL`)
		c.Assert(instance, qt.IsNil)
		started, _ := runner.calls()
		c.Assert(started, qt.HasLen, 0)
	})
	t.Run("a runtime that cannot say", func(t *testing.T) {
		c := qt.New(t)
		inner := &fakeRunner{hostPort: "127.0.0.1:12136"}

		instance, err := devdocker.Provision(context.Background(), "docker://ydb/26.2.1.14/local", devdocker.Options{
			Runner: silentRunner{inner: inner}, Ready: alwaysReady,
		})

		c.Assert(err, qt.ErrorMatches, `docker://ydb starts a server that takes connections without a credential, `+
			`and the container runtime cannot say whether it runs on this machine`)
		c.Assert(instance, qt.IsNil)
		started, _ := inner.calls()
		c.Assert(started, qt.HasLen, 0)
	})
}
