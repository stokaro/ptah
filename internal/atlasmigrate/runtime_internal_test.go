package atlasmigrate

// White-box testing required: Private adapter tests need the same explicit runtime fixture as public adapter tests.

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/engine/builtin"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}
