package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtindialects"
)

// TestCleanup_AgreesWithTheSwitchesItReplaced pins which object kinds each
// built-in descriptor says a development database cleanup destroys, and how
// it spells the drops, as internal/schemaclean's dialect switches decided
// it. The kinds are a measurement of each writer, recorded on that package's
// coverageFor.
func TestCleanup_AgreesWithTheSwitchesItReplaced(t *testing.T) {
	// kinds are, in order: foreign keys, views, materialized views, enums,
	// domains, composites, ranges and functions.
	type row struct {
		kinds                               [8]bool
		inventory, stored, revisions, probe bool
		everywhere, listing, scoped         bool
		restrict, sync, databases, first    bool
		foreignKeyDrop                      string
	}
	postgresFamily := row{kinds: [8]bool{true, true, true, true, true, true, true, true}, revisions: true, probe: true, scoped: true, restrict: true}
	postgres := postgresFamily
	postgres.inventory = true
	mysqlFamily := row{kinds: [8]bool{true, true}, stored: true, revisions: true, probe: true, databases: true, first: true, foreignKeyDrop: "DROP FOREIGN KEY"}
	want := map[string]row{
		"postgres":    postgres,
		"cockroachdb": postgresFamily,
		"yugabytedb":  postgresFamily,
		"mysql":       mysqlFamily,
		"mariadb":     mysqlFamily,
		"sqlite":      {kinds: [8]bool{false, true}},
		"sqlserver":   {kinds: [8]bool{true}, revisions: true, probe: true},
		"clickhouse":  {kinds: [8]bool{false, true, true}, sync: true},
		"oracle":      {listing: true},
		"ydb":         {revisions: true, everywhere: true},
		"spanner":     {},
	}
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			cleanup := dialect.Cleanup
			kinds := [8]bool{
				cleanup.ForeignKeys, cleanup.Views, cleanup.MaterializedViews, cleanup.Enums,
				cleanup.Domains, cleanup.Composites, cleanup.Ranges, cleanup.Functions,
			}
			c.Assert(kinds, qt.Equals, expected.kinds)
			c.Assert(cleanup.CatalogInventory, qt.Equals, expected.inventory)
			c.Assert(cleanup.StoredObjects, qt.Equals, expected.stored)
			c.Assert(cleanup.RevisionTables, qt.Equals, expected.revisions)
			c.Assert(cleanup.RevisionProbe != "", qt.Equals, expected.probe)
			c.Assert(cleanup.RevisionTablesEverywhere, qt.Equals, expected.everywhere)
			c.Assert(cleanup.WriterListing, qt.Equals, expected.listing)
			c.Assert(cleanup.SchemaScoped, qt.Equals, expected.scoped)
			c.Assert(cleanup.Restrict, qt.Equals, expected.restrict)
			c.Assert(cleanup.Sync, qt.Equals, expected.sync)
			c.Assert(cleanup.DropsDatabases, qt.Equals, expected.databases)
			c.Assert(cleanup.ViewsFirst, qt.Equals, expected.first)
			c.Assert(cleanup.ForeignKeyDrop, qt.Equals, expected.foreignKeyDrop)
		})
	}
}
