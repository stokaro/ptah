package atlasschema

// White-box testing required: Private inspection and scoping tests require an explicit runtime fixture in this package.

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/engine/builtin"
)

func inspectFeatureRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}
