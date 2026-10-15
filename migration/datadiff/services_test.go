package datadiff_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/builtintest"
)

// dataServices returns the data services the bundled runtime registers for
// dialect, which is what a command renders with: the YDB owner's literals on
// YDB, the Oracle owner's moment columns on Oracle, and none elsewhere.
func dataServices(c *qt.C, dialect string) dataext.Services {
	c.Helper()
	services, err := builtintest.Runtime().DataServices(dialect)
	c.Assert(err, qt.IsNil)
	return services
}
