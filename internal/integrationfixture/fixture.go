// Package integrationfixture defines deterministic metadata used by Ptah's
// integration harness without coupling its unit tests to live databases.
package integrationfixture

import (
	"maps"

	"go.5x5.cz/ptah/core/platform"
)

// RoundTrip describes one fixture-driven migration generator scenario.
type RoundTrip struct {
	Name             string
	Description      string
	Versions         []string
	BlockedByDialect map[string]string
}

var roundTrips = []RoundTrip{
	{Name: "empty_schema", Description: "empty entity package stays empty across generated up/down", Versions: []string{"024-roundtrip-empty"}},
	{Name: "single_table", Description: "single table round-trips from generated migrations", Versions: []string{"025-roundtrip-single-table"}},
	{Name: "composite_primary_key", Description: "table-level composite primary key survives apply -> introspect -> diff", Versions: []string{"026-roundtrip-composite-pk"}},
	{Name: "self_referencing_fk", Description: "self-referencing foreign key goes through the generator path", Versions: []string{"027-roundtrip-self-reference"}},
	{Name: "parent_child_fk_drop_order", Description: "parent/child tables created in one migration roll down to empty through generated down SQL", Versions: []string{"028-roundtrip-parent-child"}},
	{Name: "three_level_fk_chain", Description: "three-table foreign-key chain is generated, applied, introspected, and rolled back", Versions: []string{"034-roundtrip-fk-chain"}},
	{Name: "diamond_fk_graph", Description: "diamond-shaped foreign-key graph is generated and verified through the round-trip path", Versions: []string{"035-roundtrip-fk-diamond"}},
	{Name: "mutual_fk_cycle", Description: "mutual foreign-key cycle is generated, applied, introspected, and rolled back", Versions: []string{"029-roundtrip-mutual-cycle"}},
	{Name: "same_name_check_drift", Description: "same-name CHECK expression changes must be detected by generated migrations", Versions: []string{"030-roundtrip-check-v1", "031-roundtrip-check-v2"}},
	{Name: "same_name_unique_drift", Description: "same-name UNIQUE column-set changes must be detected by generated migrations", Versions: []string{"032-roundtrip-unique-v1", "033-roundtrip-unique-v2"}},
	{Name: "same_name_check_to_unique_drift", Description: "same-name CHECK to UNIQUE type changes must be detected by generated migrations", Versions: []string{"042-roundtrip-check-to-unique-v1", "043-roundtrip-check-to-unique-v2"}},
	{Name: "same_name_unique_to_check_drift", Description: "same-name UNIQUE to CHECK type changes must be detected by generated migrations", Versions: []string{"044-roundtrip-unique-to-check-v1", "045-roundtrip-unique-to-check-v2"}},
	{
		Name:        "composite_primary_key_add_remove",
		Description: "multi-column primary key addition and removal round-trip through generated migrations",
		Versions: []string{
			"036-roundtrip-pk-base",
			"037-roundtrip-pk-composite-added",
			"038-roundtrip-pk-composite-removed",
		},
	},
	{Name: "enum_value_add", Description: "enum value addition is generated, applied, introspected, rolled down, and re-applied", Versions: []string{"039-roundtrip-enum-v1", "040-roundtrip-enum-v2-add"}},
	{Name: "enum_value_remove", Description: "enum value removal is carried as an explicit round-trip fixture", Versions: []string{"040-roundtrip-enum-v2-add", "041-roundtrip-enum-v3-remove"}},
	{
		Name:        "foreign_key_added_to_existing_columns",
		Description: "foreign keys added to existing columns, including a self-reference, round-trip through generated migrations",
		Versions:    []string{"046-roundtrip-existing-fk-base", "047-roundtrip-existing-fk-added"},
	},
}

// RoundTrips returns an isolated copy of the fixture registry.
func RoundTrips() []RoundTrip {
	fixtures := make([]RoundTrip, len(roundTrips))
	for index, fixture := range roundTrips {
		fixture.Versions = append([]string(nil), fixture.Versions...)
		fixture.BlockedByDialect = maps.Clone(fixture.BlockedByDialect)
		fixtures[index] = fixture
	}
	return fixtures
}

// MigrationPath returns the fixture path for a dialect and migration family.
func MigrationPath(dialect, migrationType string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return "migrations/" + migrationType + "_mysql"
	case platform.ClickHouse:
		return "migrations/" + migrationType + "_clickhouse"
	case platform.SQLServer:
		return "migrations/" + migrationType + "_sqlserver"
	default:
		return "migrations/" + migrationType
	}
}
