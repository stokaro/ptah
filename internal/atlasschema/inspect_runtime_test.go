package atlasschema_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/builtintest"
)

func inspectFeatureRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return builtintest.Runtime()
}
