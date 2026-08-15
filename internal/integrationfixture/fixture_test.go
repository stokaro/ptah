package integrationfixture_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/integrationfixture"
)

func TestRoundTripsCoverGeneratorEdgeCases(t *testing.T) {
	c := qt.New(t)
	fixtures := make(map[string][]string)
	for _, fixture := range integrationfixture.RoundTrips() {
		fixtures[fixture.Name] = fixture.Versions
	}

	c.Assert(fixtures, qt.DeepEquals, map[string][]string{
		"empty_schema":                          {"024-roundtrip-empty"},
		"single_table":                          {"025-roundtrip-single-table"},
		"composite_primary_key":                 {"026-roundtrip-composite-pk"},
		"self_referencing_fk":                   {"027-roundtrip-self-reference"},
		"parent_child_fk_drop_order":            {"028-roundtrip-parent-child"},
		"three_level_fk_chain":                  {"034-roundtrip-fk-chain"},
		"diamond_fk_graph":                      {"035-roundtrip-fk-diamond"},
		"mutual_fk_cycle":                       {"029-roundtrip-mutual-cycle"},
		"same_name_check_drift":                 {"030-roundtrip-check-v1", "031-roundtrip-check-v2"},
		"same_name_unique_drift":                {"032-roundtrip-unique-v1", "033-roundtrip-unique-v2"},
		"same_name_check_to_unique_drift":       {"042-roundtrip-check-to-unique-v1", "043-roundtrip-check-to-unique-v2"},
		"same_name_unique_to_check_drift":       {"044-roundtrip-unique-to-check-v1", "045-roundtrip-unique-to-check-v2"},
		"composite_primary_key_add_remove":      {"036-roundtrip-pk-base", "037-roundtrip-pk-composite-added", "038-roundtrip-pk-composite-removed"},
		"enum_value_add":                        {"039-roundtrip-enum-v1", "040-roundtrip-enum-v2-add"},
		"enum_value_remove":                     {"040-roundtrip-enum-v2-add", "041-roundtrip-enum-v3-remove"},
		"foreign_key_added_to_existing_columns": {"046-roundtrip-existing-fk-base", "047-roundtrip-existing-fk-added"},
	})
}

func TestRoundTripsReturnsIsolatedMetadata(t *testing.T) {
	c := qt.New(t)
	first := integrationfixture.RoundTrips()
	first[0].Versions[0] = "changed"
	first[0].BlockedByDialect = map[string]string{"postgres": "changed"}

	second := integrationfixture.RoundTrips()
	c.Assert(second[0].Versions, qt.DeepEquals, []string{"024-roundtrip-empty"})
	c.Assert(second[0].BlockedByDialect, qt.IsNil)
}

func TestMigrationPath(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		migrationType string
		want          string
	}{
		{name: "postgres", dialect: "postgres", migrationType: "basic", want: "migrations/basic"},
		{name: "cockroachdb", dialect: "cockroachdb", migrationType: "basic", want: "migrations/basic"},
		{name: "mysql", dialect: "mysql", migrationType: "basic", want: "migrations/basic_mysql"},
		{name: "mariadb", dialect: "mariadb", migrationType: "failing", want: "migrations/failing_mysql"},
		{name: "clickhouse", dialect: "clickhouse", migrationType: "basic", want: "migrations/basic_clickhouse"},
		{name: "sqlserver", dialect: "sqlserver", migrationType: "basic", want: "migrations/basic_sqlserver"},
		{name: "mssql", dialect: "mssql", migrationType: "failing", want: "migrations/failing_sqlserver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(integrationfixture.MigrationPath(test.dialect, test.migrationType), qt.Equals, test.want)
		})
	}
}
