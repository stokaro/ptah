package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// materializedViewIndexSchema declares a table, a materialized view over it,
// a unique index on the view, and an index on the table.
//
// The table index is not decoration: it is what separates "the view's index
// moved" from "every index moved".
func materializedViewIndexSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "T", Name: "t"},
			{StructName: "C", Name: "child"},
		},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "code", Type: "INT"},
			{StructName: "C", Name: "id", Type: "INT", Primary: true},
			{
				StructName: "C", Name: "t_code", Type: "INT",
				Foreign: "t(code)", ForeignKeyName: "fk_child_t",
			},
		},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "MV", Name: "mv", Body: "SELECT id FROM t",
		}},
		Indexes: []goschema.Index{
			{StructName: "MV", Name: "mv_uk", Fields: []string{"id"}, Unique: true},
			// Unique, and referenced by the foreign key above: PostgreSQL
			// accepts a unique index as a foreign key's referenced key, so this
			// one has to exist before the constraint is added. It is what makes
			// "move the view's indexes" different from "move every index".
			{StructName: "T", Name: "t_ix", Fields: []string{"code"}, Unique: true},
		},
	}
}

// TestRender_IndexOnAMaterializedViewNamesTheView pins the target of the
// statement.
//
// The index resolved to no table, and the renderer fell back to the Go STRUCT
// name -- so it emitted `ON "MV"`, and PostgreSQL 18.4 answers `relation "MV"
// does not exist`. A statement that cannot run, at exit 0
// (stokaro/ptah#1725).
func TestRender_IndexOnAMaterializedViewNamesTheView(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(materializedViewIndexSchema(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CREATE UNIQUE INDEX IF NOT EXISTS "mv_uk" ON "mv" ("id");`)
	c.Assert(sql, qt.Not(qt.Contains), `ON "MV"`)
}

// TestRender_IndexOnAMaterializedViewFollowsTheView pins the order, which is
// the engine's rule rather than a preference: the index before the view gets
// `relation "mv" does not exist`.
func TestRender_IndexOnAMaterializedViewFollowsTheView(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(materializedViewIndexSchema(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Index(sql, `CREATE MATERIALIZED VIEW "mv"`), qt.Not(qt.Equals), -1)
	c.Assert(strings.Index(sql, `"mv_uk"`) > strings.Index(sql, `CREATE MATERIALIZED VIEW "mv"`), qt.IsTrue)
}

// TestRender_ATableIndexKeepsItsPlace is the control on both rows above. Moving
// every index after the views would satisfy them and would break the ordering
// a unique index needs to back a foreign key.
func TestRender_ATableIndexKeepsItsPlace(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(materializedViewIndexSchema(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CREATE UNIQUE INDEX IF NOT EXISTS "t_ix" ON "t" ("code");`)
	c.Assert(strings.Index(sql, `"t_ix"`) > strings.Index(sql, `CREATE TABLE "t"`), qt.IsTrue)
	// The rule the position exists for: a unique index a foreign key
	// references has to be created before the constraint that references it.
	c.Assert(strings.Index(sql, `"t_ix"`) < strings.Index(sql, `fk_child_t`), qt.IsTrue)
}

// TestPlan_IndexOnAMaterializedViewIsPlannedRatherThanRefused pins the other
// surface.
//
// It did not emit a broken statement; it refused with `invalid schema diff:
// target index reference at position 0 requires a name and owning table`, which
// names a position in a slice rather than the index or the view. The two
// surfaces disagreeing about one declaration is the shape #929 and #931 are
// about.
func TestPlan_IndexOnAMaterializedViewIsPlannedRatherThanRefused(t *testing.T) {
	c := qt.New(t)
	description := materializedViewIndexSchema()

	diff := schemadiff.CompareWithDialect(description, &dbschematypes.DBSchema{}, platform.Postgres)
	nodes, err := planner.GenerateSchemaDiffAST(diff, description, platform.Postgres)

	c.Assert(err, qt.IsNil)
	rendered := make([]string, 0, len(nodes))
	for _, node := range nodes {
		sql, renderErr := renderer.RenderSQL(platform.Postgres, node)
		c.Assert(renderErr, qt.IsNil)
		rendered = append(rendered, sql)
	}
	sql := strings.Join(rendered, "\n")
	c.Assert(sql, qt.Contains, `"mv_uk" ON "mv"`)
	c.Assert(strings.Index(sql, `"mv_uk"`) > strings.Index(sql, `CREATE MATERIALIZED VIEW "mv"`), qt.IsTrue)
}

// TestRender_AnIndexNamingNothingKeepsItsStructNameFallback is the regression
// guard on the narrowness of the fix.
//
// A declaration that writes the TABLE name into StructName resolves to no
// relation at all, and the renderer's fall back to the struct name is what
// makes it work. Dropping unresolved indexes instead of moving only the view's
// would have silently deleted those -- which is what the first attempt did, and
// what the fixtures caught.
func TestRender_AnIndexNamingNothingKeepsItsStructNameFallback(t *testing.T) {
	c := qt.New(t)
	description := materializedViewIndexSchema()
	description.Indexes = append(description.Indexes,
		goschema.Index{StructName: "t", Name: "legacy_ix", Fields: []string{"id"}})

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `"legacy_ix" ON "t"`)
}

// TestRender_OneStructDeclaringBothResolvesToTheTable pins the collision the
// relation list has to decide.
//
// A struct can carry two annotations, so one StructName can declare a table and
// a materialized view at once. Tables go into the relation list first and a
// second entry for a name does not replace the first, so an index on that
// struct keeps naming the table it always named. The reverse would silently
// retarget an existing index at a view.
func TestRender_OneStructDeclaringBothResolvesToTheTable(t *testing.T) {
	c := qt.New(t)
	description := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Both", Name: "both_t"}},
		Fields: []goschema.Field{{StructName: "Both", Name: "id", Type: "INT", Primary: true}},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "Both", Name: "both_mv", Body: "SELECT id FROM both_t",
		}},
		Indexes: []goschema.Index{{StructName: "Both", Name: "both_ix", Fields: []string{"id"}}},
	}

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `"both_ix" ON "both_t"`)
	c.Assert(sql, qt.Not(qt.Contains), `"both_ix" ON "both_mv"`)
}

// TestResolveIndexTableNames_DoesNotSeeMaterializedViews pins that the
// table-only resolver stays table-only.
//
// Its callers ask a different question -- whether a TABLE column is backed by a
// unique index, which decides whether a foreign key can reference it. A
// materialized view's unique index is not an answer to that, and widening this
// resolver would make it one.
func TestResolveIndexTableNames_DoesNotSeeMaterializedViews(t *testing.T) {
	c := qt.New(t)
	indexes := []goschema.Index{{StructName: "MV", Name: "mv_uk", Fields: []string{"id"}, Unique: true}}
	views := []goschema.MaterializedView{{StructName: "MV", Name: "mv", Body: "SELECT 1"}}

	tableOnly := goschema.ResolveIndexTableNames(indexes, nil)
	withViews := goschema.ResolveIndexOwners(indexes, nil, views)

	c.Assert(tableOnly, qt.DeepEquals, []string{""})
	c.Assert(withViews, qt.DeepEquals, []string{"mv"})
}
