package compare_test

import (
	"sync"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemacomparison"
	"ptah.run/engine/builtin"
)

// bundledRuntime is the built-in runtime, assembled once for the package.
var bundledRuntime = sync.OnceValues(builtin.New)

// builtinRules returns the comparison rules the built-in runtime registers for
// dialect, so a test of an engine's index or expression reading compares with
// the rules a comparison for that engine is given. An unnamed dialect has no
// target and gets no rules, as a comparison that names none does.
func builtinRules(c *qt.C, dialect string) schemacomparison.Rules {
	c.Helper()
	if dialect == "" {
		return schemacomparison.Rules{}
	}
	runtime, err := bundledRuntime()
	c.Assert(err, qt.IsNil)
	rules, err := runtime.ComparisonRules(dialect)
	c.Assert(err, qt.IsNil)
	return rules
}
