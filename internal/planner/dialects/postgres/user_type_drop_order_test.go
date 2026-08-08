package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPlanner_GenerateMigrationAST_DropsModifiedUserTypesAgainstTheCurrentShape
// pins the side of the diff a DROP is ordered by.
//
// The recreate path emits a non-CASCADE DROP and then creates the type again.
// The CREATE order follows the desired definitions, because that is the shape
// being built. The DROP order cannot: those statements run against the database
// as it stands, so the only references that can block them are the ones the
// CURRENT definitions carry.
//
// Here the modification is what moves the reference. The database holds
// `CREATE TYPE cc AS (f integer)` with `CREATE DOMAIN dd AS cc` over it, and
// the desired schema turns that around: `dd` becomes a plain integer domain and
// `cc` gains a field of `dd`. Ordering the drops by the desired references
// takes `cc` away first, and PostgreSQL 17.10 answers `ERROR: cannot drop type
// cc because other objects depend on it / DETAIL: type dd depends on type cc`
// (SQLSTATE 2BP01) -- measured through `ptah-compat schema apply
// --auto-approve`, exit 1, the target left on the old shape. Ordering them by
// the current references drops `dd` first and the same apply exits 0.
func TestPlanner_GenerateMigrationAST_DropsModifiedUserTypesAgainstTheCurrentShape(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	desired := &goschema.Database{
		Domains: []goschema.Domain{
			{Name: "dd", BaseType: "integer"},
		},
		CompositeTypes: []goschema.CompositeType{
			{Name: "cc", Fields: []goschema.CompositeTypeField{{Name: "f", Type: "dd"}}},
		},
	}
	diff := &difftypes.SchemaDiff{
		DomainsModified: []difftypes.DomainDiff{
			{DomainName: "dd", Changes: map[string]string{"type": "cc -> integer"}, CurrentBaseType: "cc"},
		},
		CompositeTypesModified: []difftypes.CompositeTypeDiff{
			{TypeName: "cc", Changes: map[string]string{"fields": "f integer -> f dd"}, CurrentFieldTypes: []string{"integer"}},
		},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// The current shape: dd names cc, so dd goes first.
	assertBefore(t, sql, "DROP DOMAIN IF EXISTS dd", "DROP TYPE IF EXISTS cc")
	// The desired shape: cc names dd, so dd is created first. The two orders
	// are not mirror images here, and they do not have to be.
	assertBefore(t, sql, "CREATE DOMAIN dd AS", "CREATE TYPE cc AS")
	// A recreation never runs against the shape it replaces.
	assertBefore(t, sql, "DROP TYPE IF EXISTS cc", "CREATE TYPE cc AS")
}

// TestPlanner_GenerateMigrationAST_DropsModifiedUserTypesTheDesiredShapeNoLongerNames
// is the same root cause on a shape with no flip in it: the modification simply
// stops using the domain.
//
// The database holds `CREATE DOMAIN qty AS integer CHECK (VALUE > 0)` and
// `CREATE TYPE meas AS (q qty, label text)`; the desired schema widens `qty` to
// bigint and gives `meas` a plain bigint field. Read from the desired side
// `meas` names nothing, so there is no edge to reverse and the order falls back
// to the caller's -- domains first. PostgreSQL 17.10 answers `ERROR: cannot
// drop type qty because other objects depend on it / DETAIL: column q of
// composite type meas depends on type qty`, exit 1. The current side still has
// the edge, and dropping `meas` first exits 0.
func TestPlanner_GenerateMigrationAST_DropsModifiedUserTypesTheDesiredShapeNoLongerNames(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	desired := &goschema.Database{
		Domains: []goschema.Domain{
			{Name: "qty", BaseType: "bigint", Check: "VALUE > 0"},
		},
		CompositeTypes: []goschema.CompositeType{
			{Name: "meas", Fields: []goschema.CompositeTypeField{{Name: "q", Type: "bigint"}, {Name: "label", Type: "text"}}},
		},
	}
	diff := &difftypes.SchemaDiff{
		DomainsModified: []difftypes.DomainDiff{
			{DomainName: "qty", Changes: map[string]string{"type": "integer -> bigint"}, CurrentBaseType: "integer"},
		},
		CompositeTypesModified: []difftypes.CompositeTypeDiff{
			{
				TypeName:          "meas",
				Changes:           map[string]string{"fields": "q qty, label text -> q bigint, label text"},
				CurrentFieldTypes: []string{"qty", "text"},
			},
		},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "DROP TYPE IF EXISTS meas", "DROP DOMAIN IF EXISTS qty")
	assertBefore(t, sql, "DROP DOMAIN IF EXISTS qty", "CREATE DOMAIN qty AS")
	assertBefore(t, sql, "DROP TYPE IF EXISTS meas", "CREATE TYPE meas AS")
}

// TestPlanner_GenerateMigrationAST_DropsModifiedUserTypesInCallerOrderWithoutACurrentShape
// covers a diff assembled by hand, which carries no from-side at all.
//
// There is nothing to order by then, and inventing edges out of the desired
// definitions is what this whole file exists to rule out, so the drops keep the
// caller's order. Emitting them is still right: the recreate path has to drop
// before it creates.
func TestPlanner_GenerateMigrationAST_DropsModifiedUserTypesInCallerOrderWithoutACurrentShape(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	desired := &goschema.Database{
		Domains: []goschema.Domain{
			{Name: "dd", BaseType: "integer"},
		},
		CompositeTypes: []goschema.CompositeType{
			{Name: "cc", Fields: []goschema.CompositeTypeField{{Name: "f", Type: "dd"}}},
		},
	}
	diff := &difftypes.SchemaDiff{
		DomainsModified: []difftypes.DomainDiff{
			{DomainName: "dd", Changes: map[string]string{"type": "cc -> integer"}},
		},
		CompositeTypesModified: []difftypes.CompositeTypeDiff{
			{TypeName: "cc", Changes: map[string]string{"fields": "f integer -> f dd"}},
		},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "DROP DOMAIN IF EXISTS dd", "DROP TYPE IF EXISTS cc")
	assertBefore(t, sql, "DROP TYPE IF EXISTS cc", "CREATE DOMAIN dd AS")
}
