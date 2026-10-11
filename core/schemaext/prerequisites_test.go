package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
)

// hypertableProvider provides hypertables, as an extension installing
// TimescaleDB does.
type hypertableProvider struct{}

func (hypertableProvider) ProvidedCapabilities(caps capability.Capabilities) capability.Capabilities {
	return caps.With(capability.Hypertables, true)
}

func TestProvidedCapabilities(t *testing.T) {
	tests := []struct {
		name   string
		values []any
		want   bool
	}{
		{name: "a provider among other values", values: []any{"not a provider", nil, hypertableProvider{}}, want: true},
		{name: "no provider", values: []any{"not a provider", nil}, want: false},
		{name: "no values", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres17().With(capability.Hypertables, false)

			widened := schemaext.ProvidedCapabilities(caps, test.values...)

			c.Assert(widened.Has(capability.Hypertables), qt.Equals, test.want)
			c.Assert(caps.Has(capability.Hypertables), qt.IsFalse)
		})
	}
}
