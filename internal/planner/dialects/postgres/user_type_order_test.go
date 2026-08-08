package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// userTypeOrderSchema holds one type of each kind that names another kind, in
// both directions, so a plan that emits kind by kind gets at least one of them
// wrong whichever kind it puts first.
func userTypeOrderSchema() *goschema.Database {
	return &goschema.Database{
		Domains: []goschema.Domain{
			{Name: "d_comp", BaseType: "addr"},
			{Name: "d_range", BaseType: "myrange"},
			{Name: "d_int", BaseType: "integer", Check: "VALUE > 0"},
		},
		CompositeTypes: []goschema.CompositeType{
			{Name: "addr", Fields: []goschema.CompositeTypeField{{Name: "street", Type: "text"}, {Name: "city", Type: "text"}}},
			{Name: "measure", Fields: []goschema.CompositeTypeField{{Name: "qty", Type: "d_int"}}},
		},
		Ranges: []goschema.Range{
			{Name: "myrange", Subtype: "integer"},
			{Name: "posrange", Subtype: "d_int"},
		},
	}
}

// TestPlanner_GenerateMigrationAST_CreatesUserTypesBeforeTheTypesThatNameThem
// is the ordering the round trip depends on.
//
// A description a database gives of itself is worth nothing if it cannot be
// replayed. Emitting every domain before every composite and range put
// `CREATE DOMAIN "d_comp" AS addr;` ahead of `CREATE TYPE "addr" AS (...)`, and
// the replay stopped at `ERROR: type "addr" does not exist` -- measured on
// PostgreSQL 17.10 with psql -v ON_ERROR_STOP=1, exit 3, on the script
// ptah-compat produced for a database holding exactly these types.
func TestPlanner_GenerateMigrationAST_CreatesUserTypesBeforeTheTypesThatNameThem(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	diff := &difftypes.SchemaDiff{
		DomainsAdded:        []string{"d_comp", "d_range", "d_int"},
		CompositeTypesAdded: []string{"addr", "measure"},
		RangesAdded:         []string{"myrange", "posrange"},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, userTypeOrderSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// A domain over a composite and a domain over a range: the named type first.
	assertBefore(t, sql, "CREATE TYPE addr AS", "CREATE DOMAIN d_comp AS")
	assertBefore(t, sql, "CREATE TYPE myrange AS RANGE", "CREATE DOMAIN d_range AS")
	// The other direction, which a fixed "composites and ranges first" order
	// would break in place of the one it fixed.
	assertBefore(t, sql, "CREATE DOMAIN d_int AS", "CREATE TYPE measure AS")
	assertBefore(t, sql, "CREATE DOMAIN d_int AS", "CREATE TYPE posrange AS RANGE")
}

// TestPlanner_GenerateMigrationAST_RecreatesUserTypesInDependencyOrder covers
// the drop + recreate path a modification takes, in both directions at once.
//
// The drops are non-CASCADE on purpose, so their order is not cosmetic: a
// composite still named by a domain cannot be dropped, and the plan that
// recreates both has to take the dependent away first and put it back last.
func TestPlanner_GenerateMigrationAST_RecreatesUserTypesInDependencyOrder(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	diff := &difftypes.SchemaDiff{
		DomainsModified: []difftypes.DomainDiff{
			{DomainName: "d_comp", Changes: map[string]string{"base_type": "old -> addr"}},
			{DomainName: "d_int", Changes: map[string]string{"check": "old -> new"}},
		},
		CompositeTypesModified: []difftypes.CompositeTypeDiff{
			{TypeName: "addr", Changes: map[string]string{"fields": "old -> new"}},
			{TypeName: "measure", Changes: map[string]string{"fields": "old -> new"}},
		},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, userTypeOrderSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// Drop dependents first.
	assertBefore(t, sql, "DROP DOMAIN IF EXISTS d_comp", "DROP TYPE IF EXISTS addr")
	assertBefore(t, sql, "DROP TYPE IF EXISTS measure", "DROP DOMAIN IF EXISTS d_int")
	// Recreate them last.
	assertBefore(t, sql, "CREATE TYPE addr AS", "CREATE DOMAIN d_comp AS")
	assertBefore(t, sql, "CREATE DOMAIN d_int AS", "CREATE TYPE measure AS")
	// Every drop precedes every recreation, so a recreation never runs against
	// the shape it is replacing.
	assertBefore(t, sql, "DROP TYPE IF EXISTS addr", "CREATE TYPE addr AS")
	assertBefore(t, sql, "DROP DOMAIN IF EXISTS d_int", "CREATE DOMAIN d_int AS")
}

// TestPlanner_GenerateMigrationAST_CreatesRecreatedUserTypesBeforeNewDependents
// is the case the two paths share: a brand new domain over a composite the same
// plan is recreating. The composite does not exist when the new domain is
// created, because this plan dropped it a few statements earlier.
func TestPlanner_GenerateMigrationAST_CreatesRecreatedUserTypesBeforeNewDependents(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	diff := &difftypes.SchemaDiff{
		DomainsAdded: []string{"d_comp"},
		CompositeTypesModified: []difftypes.CompositeTypeDiff{
			{TypeName: "addr", Changes: map[string]string{"fields": "old -> new"}},
		},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, userTypeOrderSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "DROP TYPE IF EXISTS addr", "CREATE TYPE addr AS")
	assertBefore(t, sql, "CREATE TYPE addr AS", "CREATE DOMAIN d_comp AS")
}
