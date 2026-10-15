package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

// TestNew_RegistersTheReadBackServicesOfTheRetiredSwitches pins which
// built-in targets answer what a read reports for a declared column or index:
// Oracle and CockroachDB for column types, and the MySQL family for index
// methods and unique-index options. These are the targets the converters
// named before they asked the runtime.
func TestNew_RegistersTheReadBackServicesOfTheRetiredSwitches(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	want := map[string]bool{platform.Oracle: true, platform.CockroachDB: true, platform.MySQL: true, platform.MariaDB: true}

	for _, described := range platform.Dialects().Dialects() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)

			readBack, err := runtime.ReadBack(described.Name)

			c.Assert(err, qt.IsNil)
			c.Assert(readBack != nil, qt.Equals, want[described.Name])
		})
	}
}
