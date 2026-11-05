package atlasschema

// White-box testing required: refuseInspectDevURLForm runs before an
// inspection provisions its dev database, and the public entry point that
// reaches it starts the provider that owns the URL.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
)

// providedSchemes serves the scheme of the engine "provided", as a provider
// selection does.
type providedSchemes struct{}

func (providedSchemes) NativeRegistration(context.Context, string) (dbschema.NativeRegistration, bool, error) {
	return dbschema.NativeRegistration{}, false, nil
}

func (providedSchemes) NativeSchemes() map[string]string {
	return map[string]string{"provided": "provided"}
}

// providedDev provisions the engine's dev databases, as a provider that owns
// its docker URLs does. Nothing here provisions one.
type providedDev struct{}

func (providedDev) DevEngines() []string { return []string{"provided"} }

func (providedDev) ProvisionDev(context.Context, string) (string, func(), error) {
	return "", func() {}, errors.New("not provisioned")
}

// A docker dev URL of an engine a provider provisions is the provider's to
// read, so the form check leaves it alone; without the owner the host's
// parser refuses the image it does not know.
func TestRefuseInspectDevURLForm_AProvidersDockerURL(t *testing.T) {
	c := qt.New(t)
	served := dbschema.WithNativeLookup(c.Context(), providedSchemes{})

	c.Assert(refuseInspectDevURLForm(devdocker.WithExternal(served, providedDev{}), "docker://provided/1.0/dev"), qt.IsNil)
	c.Assert(refuseInspectDevURLForm(served, "docker://provided/1.0/dev"), qt.ErrorMatches, `unsupported docker image "provided"`)
}
