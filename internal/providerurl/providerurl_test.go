package providerurl_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerselect"
	"ptah.run/internal/providerurl"
)

// unopened is a connector nothing here opens.
type unopened struct{}

func (unopened) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return nil, errors.New("not opened")
}

// servingFixture is a context on which the fixture scheme names the fixture
// target, as an explicit provider selection installs it.
func servingFixture(c *qt.C) context.Context {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"fixture": {Target: "fixture", Connector: unopened{}},
	})
	c.Assert(err, qt.IsNil)
	return ctx
}

// lazyFixture is a context carrying a lazy provider session whose manifest
// claims the fixture engine, with the alias fix, for an executable that does
// not exist: nothing here starts it.
func lazyFixture(c *qt.C) context.Context {
	c.Helper()
	config := providerconfig.Configuration{
		FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{
			Spec:    providerhost.Spec{ProviderID: "example.org/fixture", Executable: filepath.Join(c.TempDir(), "missing"), SHA256: strings.Repeat("a", 64)},
			Engines: []providerconfig.Engine{{ID: "fixture", Aliases: []string{"fix"}}},
		}},
	}
	session := providerselect.NewLazy(config, true, &providerconfig.Resolver{})
	c.Cleanup(session.Close)
	return session.With(c.Context())
}

// selections are the contexts a row runs under, by name.
var selections = map[string]func(*qt.C) context.Context{
	"explicit": servingFixture,
	"lazy":     lazyFixture,
	"none":     func(c *qt.C) context.Context { return c.Context() },
}

func TestDialect_HappyPath(t *testing.T) {
	for _, row := range []struct{ name, selection, url, want string }{
		{"a scheme an explicit selection registered", "explicit", "fixture:///tmp/x.db", "fixture"},
		{"an upper-case scheme an explicit selection registered", "explicit", "FIXTURE:///tmp/x.db", "fixture"},
		{"a scheme a lazy session would serve", "lazy", "fixture:///tmp/x.db", "fixture"},
		{"the alias of a scheme a lazy session would serve", "lazy", "fix:///tmp/x.db", "fixture"},
		{"a docker dev URL of an engine a lazy session provisions", "lazy", "docker://fixture/1/dev", "fixture"},
		{"a built-in URL beside a lazy session", "lazy", "postgres://localhost/app", "postgres"},
		{"a built-in URL with no selection", "none", "postgres://localhost/app", "postgres"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := providerurl.Dialect(selections[row.selection](c), row.url)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// A scheme only a provider serves is not a dialect of its own: without a
// selection or a session the built-in reading refuses it.
func TestDialect_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := providerurl.Dialect(c.Context(), "fixture:///tmp/x.db")

	c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "fixture:///tmp/x.db"`)
	c.Assert(got, qt.Equals, "")
}

func TestProvided(t *testing.T) {
	for _, row := range []struct {
		name, selection, url, target string
		served                       bool
	}{
		{"a registered scheme", "explicit", "fixture:///tmp/x.db", "fixture", true},
		{"a built-in scheme", "lazy", "sqlite:///tmp/x.db", "", false},
		{"a path with no scheme", "lazy", "/tmp/x.db", "", false},
		{"a URL that does not parse", "lazy", "fixture://%zz", "", false},
		{"a registered scheme with no selection", "none", "fixture:///tmp/x.db", "", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target, served := providerurl.Provided(selections[row.selection](c), row.url)
			c.Assert(served, qt.Equals, row.served)
			c.Assert(target, qt.Equals, row.target)
		})
	}
}

func TestValidateDialectMatch_HappyPath(t *testing.T) {
	for _, row := range []struct{ name, devURL, target string }{
		{"a dev URL the provider serves for the target's engine", "fixture:///tmp/dev.db", "fixture"},
		{"a built-in dev URL", "postgres://localhost/dev", "postgres"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(providerurl.ValidateDialectMatch(servingFixture(c), row.devURL, row.target), qt.IsNil)
		})
	}
}

// A dev URL the provider serves names the provider's target, which is compared
// with the target database's dialect rather than read as a dialect of its own.
func TestValidateDialectMatch_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := providerurl.ValidateDialectMatch(servingFixture(c), "fixture:///tmp/dev.db", "postgres")

	c.Assert(err, qt.ErrorMatches, `--dev-url dialect "fixture" does not match --url dialect "postgres"`)
}
