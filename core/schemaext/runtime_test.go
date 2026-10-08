package schemaext_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// Promoted methods panic if invoked. Presence validation must not dispatch to
// a provider, especially while checking a typed-nil receiver.
type untouchedRuntime struct{ schemaext.ConversionRuntime }

func TestRequireRuntimeRejectsUnavailableInputsBeforeDispatch(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		runtime schemaext.ConversionRuntime
		want    error
	}{
		{name: "no context", runtime: &untouchedRuntime{}, want: schemaext.ErrInvalidValue},
		{name: "no runtime", ctx: t.Context(), want: schemaext.ErrInvalidValue},
		{name: "typed nil", ctx: t.Context(), runtime: (*untouchedRuntime)(nil), want: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, runtime: &untouchedRuntime{}, want: context.Canceled},
		{name: "present", ctx: t.Context(), runtime: &untouchedRuntime{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaext.RequireRuntime(test.ctx, test.runtime), qt.ErrorIs, test.want)
		})
	}
}
