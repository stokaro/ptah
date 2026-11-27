package devreach_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/devreach"
)

// closedPort names a PostgreSQL server on a port nothing listens on, so the
// connection is refused at once.
//
// #nosec G101 -- a fixture password for a port nothing listens on.
const closedPort = "postgres://ptah:closed-port-secret@127.0.0.1:1/dev?sslmode=disable"

// TestServer_HappyPath answers no capability and no error for a URL that names
// no server to ask, and opens nothing.
func TestServer_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		devURL string
	}{
		{name: "no URL", devURL: ""},
		{name: "a URL of spaces", devURL: "   "},
		{name: "a docker URL whose tag no release line matches", devURL: "docker://postgres/latest/dev"},
		{name: "a docker URL with no tag", devURL: "docker://postgres"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := devreach.Server(c.Context(), "postgres", "--dev-url", test.devURL, 0)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Len(), qt.Equals, 0)
		})
	}
}

// TestServer_DockerTagNamesTheServer answers a docker URL's server from its
// image tag, and starts no container.
func TestServer_DockerTagNamesTheServer(t *testing.T) {
	c := qt.New(t)

	got, err := devreach.Server(c.Context(), "postgres", "--dev-url", "docker://postgres/18/dev", 0)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Equal(capability.ResolveServerVersion("postgres", "18").Capabilities), qt.IsTrue)
}

// TestServer_LeavesASQLiteFileUncreated answers a SQLite file without opening
// it, since opening it creates the file.
func TestServer_LeavesASQLiteFileUncreated(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "dev.db")

	got, err := devreach.Server(c.Context(), "sqlite", "--dev-url", "sqlite://"+filepath.ToSlash(path), 0)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Len(), qt.Equals, 0)
	_, statErr := os.Stat(path)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

// TestServer_FailurePath refuses a URL nothing answers on, naming it as the
// caller names its source, and a docker URL that does not parse.
func TestServer_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		source  string
		devURL  string
		wantErr string
	}{
		{
			name:    "a server nothing answers on",
			dialect: "postgres",
			source:  "--dev-url",
			devURL:  closedPort,
			// The clause after the address is the operating system's.
			wantErr: `(?s)connect to --dev-url: .*dial tcp 127\.0\.0\.1:1: .*`,
		},
		{
			name:    "the flag the caller names",
			dialect: "postgres",
			source:  "--shadow-db",
			devURL:  closedPort,
			wantErr: `(?s)connect to --shadow-db: .*`,
		},
		{
			name:    "a project file the caller names",
			dialect: "postgres",
			source:  "the dev database named in the project file",
			devURL:  closedPort,
			wantErr: `(?s)connect to the dev database named in the project file: .*`,
		},
		{
			name:    "a docker image no engine runs",
			dialect: "postgres",
			source:  "--dev-url",
			devURL:  "docker://sqlite/dev",
			wantErr: `unsupported docker image "sqlite"`,
		},
		{
			// A docker URL behind a space is no docker URL, as it is none to
			// the pinned community binary, so it is not parsed as one.
			name:    "a docker URL behind a space",
			source:  "--dev-url",
			devURL:  " docker://sqlite/dev",
			wantErr: `connect to --dev-url: .*`,
		},
		{
			// The dialect is checked before the connection, so the refusal
			// names the mismatch and not the closed port.
			name:    "a server of another dialect",
			dialect: "postgres",
			source:  "--dev-url",
			devURL:  "mysql://127.0.0.1:1/dev",
			wantErr: `--dev-url dialect "mysql" does not match --url dialect "postgres"`,
		},
		{
			name:    "a scheme no dialect has",
			dialect: "postgres",
			source:  "--dev-url",
			devURL:  "notadriver://x",
			wantErr: `unsupported --dev-url dialect "notadriver://x"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := devreach.Server(c.Context(), test.dialect, test.source, test.devURL, 0)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.Len(), qt.Equals, 0)
		})
	}
}

// refusingProvider is a provider connector whose every session fails to open.
type refusingProvider struct{}

func (refusingProvider) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return nil, errors.New("the provider refused the session")
}

// TestServer_ProviderScheme_FailurePath holds a dev URL whose scheme an
// executable provider serves to that provider: its dialect is the provider's
// target, and the connection is the provider's to open, so the refusal is the
// provider's rather than an unsupported dialect.
func TestServer_ProviderScheme_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantErr string
	}{
		{
			name:    "the provider's target",
			dialect: "fixture",
			wantErr: `connect to --dev-url: .*the provider refused the session`,
		},
		{
			name:    "another dialect",
			dialect: "postgres",
			wantErr: `--dev-url dialect "fixture" does not match --url dialect "postgres"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
				"fixture": {Target: "fixture", Connector: refusingProvider{}},
			})
			c.Assert(err, qt.IsNil)

			got, err := devreach.Server(ctx, test.dialect, "--dev-url", "fixture:///dev.db", 0)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.Len(), qt.Equals, 0)
		})
	}
}

// fixtureDevOwner is a provider that provisions the dev databases of the
// fixture engine. Nothing here asks it to.
type fixtureDevOwner struct{}

func (fixtureDevOwner) DevEngines() []string { return []string{"fixture"} }

func (fixtureDevOwner) ProvisionDev(context.Context, string) (string, func(), error) {
	return "", nil, errors.New("not provisioned here")
}

// aliasDevOwner provisions the dev databases of Spanner, an official provider
// engine, under its ID and under its catalog alias. Nothing here asks it to.
type aliasDevOwner struct{}

func (aliasDevOwner) DevEngines() []string { return []string{"cloudspanner", "spanner"} }

func (aliasDevOwner) ProvisionDev(context.Context, string) (string, func(), error) {
	return "", nil, errors.New("not provisioned here")
}

// TestServer_ProviderDocker_HappyPath leaves a docker URL whose engine a
// provider provisions to that provider, in either form: it is not parsed as an
// image Ptah knows, nothing is connected, and no capability is answered. The
// engine is compared with the target through the registry and the provider
// catalog, so an alias on either side names the same engine.
func TestServer_ProviderDocker_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		owner   devdocker.External
		dialect string
		devURL  string
	}{
		{name: "docker://", owner: fixtureDevOwner{}, dialect: "fixture", devURL: "docker://fixture/dev"},
		{name: "docker+", owner: fixtureDevOwner{}, dialect: "fixture", devURL: "docker+fixture://_/example.org/fixture:2.0"},
		{name: "an alias in the URL", owner: aliasDevOwner{}, dialect: "spanner", devURL: "docker://cloudspanner/dev"},
		{name: "an alias in the docker+ scheme", owner: aliasDevOwner{}, dialect: "spanner", devURL: "docker+cloudspanner://_/example.org/spanner:1"},
		{name: "an alias as the target", owner: aliasDevOwner{}, dialect: "cloudspanner", devURL: "docker://spanner/dev"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := devdocker.WithExternal(c.Context(), test.owner)

			got, err := devreach.Server(ctx, test.dialect, "--dev-url", test.devURL, 0)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Len(), qt.Equals, 0)
		})
	}
}

// TestServer_ProviderDocker_BuiltInImage reads a docker URL of an engine no
// provider provisions from its image tag, with a provider in ctx.
func TestServer_ProviderDocker_BuiltInImage(t *testing.T) {
	c := qt.New(t)
	ctx := devdocker.WithExternal(c.Context(), fixtureDevOwner{})

	got, err := devreach.Server(ctx, "postgres", "--dev-url", "docker://postgres/18/dev", 0)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Equal(capability.ResolveServerVersion("postgres", "18").Capabilities), qt.IsTrue)
}

// TestServer_ProviderDocker_FailurePath refuses a docker URL whose engine a
// provider provisions when the target is of another dialect, as it refuses a
// built-in docker URL of another dialect.
func TestServer_ProviderDocker_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := devdocker.WithExternal(c.Context(), fixtureDevOwner{})

	got, err := devreach.Server(ctx, "postgres", "--dev-url", "docker://fixture/dev", 0)

	c.Assert(err, qt.ErrorMatches, `--dev-url dialect "fixture" does not match --url dialect "postgres"`)
	c.Assert(got.Len(), qt.Equals, 0)
}
