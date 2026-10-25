package engine_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
)

func spellingProvider(service schemapreparation.Lowering) engine.Provider {
	return engine.Provider{ID: "example.org/spelling", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Spelling: service}}}
}

// spellingRequest declares a view, a role and a grant, the objects a
// spelling may rewrite the values of.
func spellingRequest() schemapreparation.LoweringRequest {
	request := loweringRequest()
	request.Desired.Views = []schemamodel.View{{Name: "recent", Body: "SELECT   id FROM events"}}
	request.Desired.Roles = []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}}
	request.Desired.Grants = []schemamodel.Grant{{Role: "reader", Privileges: []string{"select"}, OnTable: "events"}}
	return request
}

// spelledAs returns a spelling service that hands back what spell makes of a
// copy of the desired schema.
func spelledAs(spell func(*schemamodel.Database)) schemapreparation.Lowering {
	return loweringFunc(func(_ context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
		spelled := *request.Desired
		spelled.Views, spelled.Roles, spelled.Grants = slices.Clone(spelled.Views), slices.Clone(spelled.Roles), slices.Clone(spelled.Grants)
		spell(&spelled)
		return &spelled, nil
	})
}

func TestSpellDesired_HappyPath(t *testing.T) {
	t.Run("the selected target's service", func(t *testing.T) {
		c := qt.New(t)
		var received schemapreparation.LoweringRequest
		runtime := mustRuntime(c, spellingProvider(loweringFunc(func(_ context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
			received = request
			spelled := *request.Desired
			spelled.Views = []schemamodel.View{{Name: "recent", Body: "SELECT id FROM events"}}
			return &spelled, nil
		})))
		request := spellingRequest()

		got, err := runtime.SpellDesired(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(got.Views, qt.DeepEquals, []schemamodel.View{{Name: "recent", Body: "SELECT id FROM events"}})
		c.Assert(received.Target, qt.Equals, "custom")
		c.Assert(received.Desired, qt.Equals, request.Desired)
		c.Assert(request.Desired.Views[0].Body, qt.Equals, "SELECT   id FROM events")
	})

	t.Run("a target without one keeps the declaration", func(t *testing.T) {
		c := qt.New(t)
		runtime := mustRuntime(c, spellingProvider(nil))
		request := spellingRequest()

		got, err := runtime.SpellDesired(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, request.Desired)
	})

	t.Run("no desired schema reaches no service", func(t *testing.T) {
		c := qt.New(t)
		called := false
		runtime := mustRuntime(c, spellingProvider(loweringFunc(func(context.Context, schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
			called = true
			return &schemamodel.Database{}, nil
		})))
		request := spellingRequest()
		request.Desired = nil

		got, err := runtime.SpellDesired(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.IsNil)
		c.Assert(called, qt.IsFalse)
	})
}

// TestSpellDesired_FailurePath refuses a spelling that is not the declaration
// it spelled, whichever collection it changed, as well as the failures every
// target service has.
func TestSpellDesired_FailurePath(t *testing.T) {
	cases := []struct {
		name    string
		service schemapreparation.Lowering
		target  string
		wantErr error
	}{
		{name: "unregistered target", service: spelledAs(func(*schemamodel.Database) {}), target: "unregistered", wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "service error", service: loweringFunc(func(context.Context, schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
			return nil, errLoweringFailed
		}), target: "custom", wantErr: errLoweringFailed},
		{name: "nil result", service: loweringFunc(func(context.Context, schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
			return nil, nil
		}), target: "custom", wantErr: schemapreparation.ErrInvalid},
		{name: "a renamed view", service: spelledAs(func(spelled *schemamodel.Database) { spelled.Views[0].Name = "other" }), target: "custom",
			wantErr: schemapreparation.ErrInvalid},
		{name: "an added role", service: spelledAs(func(spelled *schemamodel.Database) {
			spelled.Roles = append(spelled.Roles, schemamodel.Role{Name: "writer"})
		}), target: "custom", wantErr: schemapreparation.ErrInvalid},
		{name: "a dropped grant", service: spelledAs(func(spelled *schemamodel.Database) { spelled.Grants = nil }), target: "custom",
			wantErr: schemapreparation.ErrInvalid},
		{name: "an added sequence", service: spelledAs(func(spelled *schemamodel.Database) {
			spelled.Sequences = []schemamodel.Sequence{{Name: "counter"}}
		}), target: "custom", wantErr: schemapreparation.ErrInvalid},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, spellingProvider(tt.service))
			request := spellingRequest()
			request.Target = tt.target

			got, err := runtime.SpellDesired(t.Context(), request)

			c.Assert(err, qt.ErrorIs, tt.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestSpellDesired_CanceledContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := mustRuntime(c, spellingProvider(spelledAs(func(*schemamodel.Database) { cancel() })))

	got, err := runtime.SpellDesired(ctx, spellingRequest())

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(got, qt.IsNil)
}

func TestNew_RefusesTypedNilSpelling(t *testing.T) {
	c := qt.New(t)
	var service loweringFunc

	runtime, err := engine.New(spellingProvider(service))

	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(err, qt.ErrorMatches, `.*typed-nil spelling service`)
	c.Assert(runtime, qt.IsNil)
}
