package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

// TestNew_RegistersTheDataServicesDescriptorsAskFor pins the bundled runtime
// against every built-in descriptor: an engine whose descriptor says its owner
// writes literals or types managed rows has that service, and an engine whose
// descriptor asks for none has none. A shared package refuses an
// engine that asks without being given one, so a missing registration fails
// here rather than as that refusal in a command.
func TestNew_RegistersTheDataServicesDescriptorsAskFor(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)

	for _, described := range platform.Dialects().Dialects() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)

			services, err := runtime.DataServices(described.Name)

			c.Assert(err, qt.IsNil)
			c.Assert(services.Literals != nil || services.Columns != nil, qt.Equals, described.Literals.ByOwner)
			c.Assert(services.Rows != nil, qt.Equals, described.ManagedData.TypedRows)
		})
	}
}
