package migrate

// White-box testing required: shadowDesired decides whether migrations
// generate loads its sources a second time for the shadow database, and the
// only public path to it runs a provider that binds values to a database and
// a shadow database that interprets them in another one. The fixture
// providers either bind no value or compare no attached value, so no
// generation reaches the convergence check the decision serves.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
)

// servedSchemes is a native lookup serving scheme example as the target
// example, without starting anything.
type servedSchemes struct{}

func (servedSchemes) NativeRegistration(context.Context, string) (dbschema.NativeRegistration, bool, error) {
	return dbschema.NativeRegistration{}, false, nil
}
func (servedSchemes) NativeSchemes() map[string]string {
	return map[string]string{"example": "example"}
}

// addressContexts resolves each address to the context it maps to, and
// refuses an address it does not hold.
type addressContexts map[string]schemaext.DatabaseContext

func (a addressContexts) ResolveDatabaseContext(_ context.Context, request schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
	bound, found := a[request.Address]
	if !found {
		return schemaext.DatabaseContext{}, errors.New("unknown address")
	}
	return bound, nil
}

// recordingLoader loads a schema naming the address it was bound to.
type recordingLoader struct{ addresses []string }

func (l *recordingLoader) load(_, databaseURL string) (*schemamodel.Database, error) {
	l.addresses = append(l.addresses, databaseURL)
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: databaseURL}}}, nil
}

var (
	targetContext = schemaext.DatabaseContext{Database: "/local", SourceDatabase: "/local"}
	realmContext  = schemaext.DatabaseContext{Database: "/local/.ptah/realm", SourceDatabase: "/local"}
	contexts      = addressContexts{"example://target": targetContext, "example://realm": realmContext, "example://other": targetContext}
)

// A shadow database that interprets values in another context than the
// database the plan reads gets the sources loaded again, bound for it.
func TestShadowDesired_LoadsTheSourcesForAShadowOfAnotherContext(t *testing.T) {
	c := qt.New(t)
	loader := &recordingLoader{}
	ctx := dbschema.WithNativeLookup(c.Context(), servedSchemes{})

	desired, err := shadowDesired(ctx, contexts, "example", "example://target", "example://realm", loader.load)

	c.Assert(err, qt.IsNil)
	c.Assert(desired.Tables[0].Name, qt.Equals, "example://realm")
	c.Assert(loader.addresses, qt.DeepEquals, []string{"example://realm"})
}

// The plan's own desired schema serves a shadow that shares its context, a
// shadow URL no provider serves, one another provider serves, and a run with
// no shadow database; the sources are not loaded again for any of them.
func TestShadowDesired_KeepsThePlansDesiredSchema(t *testing.T) {
	for _, test := range []struct {
		name, dialect, shadow string
	}{
		{name: "a shadow of the same context", dialect: "example", shadow: "example://other"},
		{name: "a built-in shadow URL", dialect: "example", shadow: "postgres://localhost/shadow"},
		{name: "a shadow another target serves", dialect: "another", shadow: "example://realm"},
		{name: "no shadow database", dialect: "example", shadow: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			loader := &recordingLoader{}
			ctx := dbschema.WithNativeLookup(c.Context(), servedSchemes{})

			desired, err := shadowDesired(ctx, contexts, test.dialect, "example://target", test.shadow, loader.load)

			c.Assert(err, qt.IsNil)
			c.Assert(desired, qt.IsNil)
			c.Assert(loader.addresses, qt.HasLen, 0)
		})
	}
}

func TestShadowDesired_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, planned, shadow, wantErr string
	}{
		{name: "the plan's context", planned: "example://unknown", shadow: "example://realm", wantErr: `resolve the database context of the plan: unknown address`},
		{name: "the shadow's context", planned: "example://target", shadow: "example://unknown", wantErr: `resolve the database context of --shadow-db: unknown address`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			loader := &recordingLoader{}
			ctx := dbschema.WithNativeLookup(c.Context(), servedSchemes{})

			desired, err := shadowDesired(ctx, contexts, "example", test.planned, test.shadow, loader.load)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(desired, qt.IsNil)
			c.Assert(loader.addresses, qt.HasLen, 0)
		})
	}
}
