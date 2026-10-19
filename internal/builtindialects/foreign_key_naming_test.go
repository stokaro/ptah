package builtindialects_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// TestDescriptors_ForeignKeyNamingAgreesWithTheSwitchItReplaces pins
// ForeignKeyNaming against mysqlname.NamesForeignKeys, which the SQL reader,
// the comparison and the generator's backing index cleanup asked before the
// descriptor carried the fact. It named an unnamed foreign key
// `<table>_ibfk_<n>`, and its index after the key's first column, on every
// spelling NormalizeDialect folds onto MySQL or MariaDB, in any case and with
// surrounding space, and on no other spelling, an unknown one included.
func TestDescriptors_ForeignKeyNamingAgreesWithTheSwitchItReplaces(t *testing.T) {
	switchNamed := map[string]bool{platform.MySQL: true, platform.MariaDB: true}
	var spellings []string
	for _, spelling := range platform.DialectSpellings() {
		spellings = append(spellings, spelling, strings.ToUpper(spelling), " "+spelling+" ")
	}
	spellings = append(spellings, "", "unknown", "mysql8", "maria db")
	c := qt.New(t)
	c.Assert(len(spellings) > 3*len(platform.DialectSpellings()), qt.IsTrue,
		qt.Commentf("the spellings include every accepted one, respelled, and some no dialect accepts"))
	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			described, _ := platform.Lookup(spelling)

			c.Assert(described.ForeignKeyNaming == descriptor.ForeignKeyNamingTableNumbered, qt.Equals,
				switchNamed[platform.NormalizeDialect(spelling)])
		})
	}
}
