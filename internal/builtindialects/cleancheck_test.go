package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestCleanCheck_AgreesWithTheSwitchesItReplaced pins what each built-in
// descriptor states about the clean checks, the schema attributes and the plan
// qualifier, as the dialect switches of internal/migrateclean,
// internal/atlasschema and internal/atlasmigrate decided it: the dialects the
// `migrate apply` gate and the dev database check govern, the dialect that
// counts its tables and keeps the catalog's order (SQLite), the family whose
// refusals say "in connected schema" (PostgreSQL's), the dialect judged by
// its writer's listing (YDB), the dialect whose docker block declares a
// starting point (PostgreSQL), the dialects whose catalog query is data, the
// MySQL family's schema attributes, and the dialects the qualifier refuses.
func TestCleanCheck_AgreesWithTheSwitchesItReplaced(t *testing.T) {
	type row struct {
		check      descriptor.CleanCheck
		tables     bool
		attributes bool
		refused    bool
	}
	postgresFamily := row{check: descriptor.CleanCheck{Dev: true, ConnectedSchema: true}}
	mysqlFamily := row{check: descriptor.CleanCheck{Adoption: true, Dev: true}, attributes: true}
	want := map[string]row{
		"postgres": {check: descriptor.CleanCheck{
			Adoption: true, Dev: true, ConnectedSchema: true, StartingPoint: true,
		}},
		"cockroachdb": postgresFamily,
		"yugabytedb":  postgresFamily,
		"spanner":     {check: postgresFamily.check, refused: true},
		"mysql":       mysqlFamily,
		"mariadb":     mysqlFamily,
		"sqlite": {check: descriptor.CleanCheck{
			Adoption: true, AdoptionCounts: true, Dev: true, CatalogOrder: true,
		}, tables: true, refused: true},
		"sqlserver":  {check: descriptor.CleanCheck{Dev: true}, tables: true, refused: true},
		"clickhouse": {check: descriptor.CleanCheck{Dev: true}, tables: true, refused: true},
		"oracle":     {check: descriptor.CleanCheck{Dev: true}, tables: true},
		"ydb":        {check: descriptor.CleanCheck{Dev: true, ResetListing: true}, refused: true},
	}
	c := qt.New(t)
	c.Assert(builtindialects.Descriptors(), qt.HasLen, len(want))
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			check := dialect.CleanCheck
			c.Assert(check.Tables != "", qt.Equals, expected.tables)
			check.Tables = ""
			c.Assert(check, qt.Equals, expected.check)
			c.Assert(dialect.SchemaAttributesQuery != "", qt.Equals, expected.attributes)
			c.Assert(dialect.QualifierRefusal != "", qt.Equals, expected.refused)
		})
	}
}

// TestCleanCheck_TablesLeaveSchemaSelectingEnginesToTheirCatalog pins the
// contract CleanCheck.Tables states: an engine whose URL selects a schema is
// read through the catalog its selection names, so it states no query, and
// every other engine the dev check governs states one, or is judged by its
// writer's listing.
func TestCleanCheck_TablesLeaveSchemaSelectingEnginesToTheirCatalog(t *testing.T) {
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			selects := dialect.Schemas.Selection == descriptor.SchemaSelectionSearchPath ||
				dialect.Schemas.Selection == descriptor.SchemaSelectionDatabase
			c.Assert(dialect.CleanCheck.Tables == "", qt.Equals, selects || dialect.CleanCheck.ResetListing)
		})
	}
}
