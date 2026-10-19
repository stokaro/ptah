package builtindialects_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// TestDescriptors_SystemSchemasAgreeWithTheSwitchesTheyReplace pins
// SystemSchemas against the switches of internal/pgsystemschema it replaces,
// written out here as they stood, with changes made on purpose on Spanner.
// PostgresNonSystemSchemasPredicate left crdb_internal out of a CockroachDB
// read and spanner_sys out of a Spanner one; IsPostgresFamilySystemSchema
// added crdb_internal on CockroachDB; and IsUncreatableSchema added public and
// spanner_sys on Spanner. Spanner refuses a document that holds spanner_sys,
// as CockroachDB refuses one holding crdb_internal, so spanner_sys is reserved
// rather than implicit. Spanner also reserves the names its server refuses to
// create that no switch held: spanner and default, the spanner_ prefix, and
// six names in every other case. The Spanner descriptor carries the
// measurements. Each value holds for every spelling NormalizeDialect folds
// onto the engine, in any case and with surrounding space, and for no other
// spelling, an unknown one included.
func TestDescriptors_SystemSchemasAgreeWithTheSwitchesTheyReplace(t *testing.T) {
	switches := map[string]descriptor.SystemSchemas{
		platform.CockroachDB: {Catalog: []string{"crdb_internal"}, Reserved: []string{"crdb_internal"}},
		platform.Spanner: {
			Catalog:          []string{"spanner_sys"},
			Reserved:         []string{"spanner_sys", "spanner", "default"},
			Implicit:         []string{"public"},
			ReservedPrefixes: []string{"spanner_"},
			ReservedCaseVariants: []string{
				"default", "information_schema", "pg_catalog", "public", "spanner", "spanner_sys",
			},
		},
	}
	var spellings []string
	for _, spelling := range platform.DialectSpellings() {
		spellings = append(spellings, spelling, strings.ToUpper(spelling), " "+spelling+" ")
	}
	spellings = append(spellings, "", "unknown", "spanner-pg")
	c := qt.New(t)
	c.Assert(len(spellings) > 3*len(platform.DialectSpellings()), qt.IsTrue,
		qt.Commentf("the spellings include every accepted one, respelled, and some no dialect accepts"))
	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			described, _ := platform.Lookup(spelling)

			c.Assert(described.SystemSchemas, qt.DeepEquals, switches[platform.NormalizeDialect(spelling)])
		})
	}
}
