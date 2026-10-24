package atlasschema

// White-box testing required: validateDevDialect runs only once a provider
// selection is on the context and the target is connected, and a dev URL a
// provider serves for another target than the target database's needs a
// provider of two engines, which no fixture is. The decision reads the URL
// schemes on the context and nothing else, so its rows are pinned here; the
// e2e tests under integration/providercli drive the matching case.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// unopened is a connector the decision never opens.
type unopened struct{}

func (unopened) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return nil, errors.New("not opened")
}

// servingFixture is a context on which the fixture scheme names the fixture
// target, as a provider selection installs it.
func servingFixture(c *qt.C) context.Context {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"fixture": {Target: "fixture", Connector: unopened{}},
	})
	c.Assert(err, qt.IsNil)
	return ctx
}

func TestValidateDevDialect_HappyPath(t *testing.T) {
	for _, row := range []struct{ name, devURL, target string }{
		{"a dev URL the provider serves for the target's engine", "fixture:///tmp/dev.db", "fixture"},
		{"a built-in dev URL", "postgres://localhost/dev", "postgres"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateDevDialect(servingFixture(c), row.devURL, row.target), qt.IsNil)
		})
	}
}

// A dev URL the provider serves names the provider's target, which is compared
// with the target database's dialect rather than read as a dialect of its own.
func TestValidateDevDialect_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := validateDevDialect(servingFixture(c), "fixture:///tmp/dev.db", "postgres")

	c.Assert(err, qt.ErrorMatches, `--dev-url dialect "fixture" does not match --url dialect "postgres"`)
}
