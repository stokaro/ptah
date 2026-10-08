package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlanner_ReleasesForeignKeysBeforeKeysAndColumns(t *testing.T) {
	c := qt.New(t)
	// Identities are deliberately absent, as in an embedder's hand-built diff.
	diff := &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "parents_pkey", TableName: "parents", Type: "PRIMARY KEY"},
			{Name: "parents_code_key", TableName: "parents", Type: "UNIQUE"},
			{Name: "parent_fk", TableName: "children", Type: "FOREIGN KEY"},
			{Name: "parent_fk", TableName: "archives", Type: "FOREIGN KEY"},
		},
		TablesModified: []difftypes.TableDiff{{
			TableName: "children", ColumnsRemoved: difftypes.ColumnChanges{{Name: "parent_id"}},
		}},
	}
	nodes, err := postgres.New().GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	for _, table := range []string{"children", "archives"} {
		drop := `ALTER TABLE "` + table + `" DROP CONSTRAINT IF EXISTS "parent_fk"`
		c.Assert(strings.Count(sql, drop), qt.Equals, 1, qt.Commentf("%s", sql))
		assertBefore(t, sql, drop, `DROP CONSTRAINT IF EXISTS "parents_pkey"`)
		assertBefore(t, sql, drop, `DROP CONSTRAINT IF EXISTS "parents_code_key"`)
		assertBefore(t, sql, drop, `DROP COLUMN "parent_id"`)
	}
}

func TestPlanner_ReleasesForeignKeyBeforeReferencedKeyReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "key_id", TableName: "public.items", Type: "PRIMARY KEY"},
			{Name: "self_fk", TableName: "public.items", Type: "FOREIGN KEY"},
		},
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "self_fk", TableName: "items", Type: "FOREIGN KEY", Columns: []string{"parent_id"}, ForeignTable: "items", ForeignColumns: []string{"id"}},
			{Name: "key_id", TableName: "items", Type: "PRIMARY KEY", Columns: []string{"id"}},
		},
	}
	nodes, err := postgres.New().GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	for _, name := range []string{"self_fk", "key_id"} {
		c.Assert(strings.Count(sql, `DROP CONSTRAINT IF EXISTS "`+name+`"`), qt.Equals, 1, qt.Commentf("%s", sql))
		c.Assert(strings.Count(sql, `ADD CONSTRAINT "`+name+`"`), qt.Equals, 1, qt.Commentf("%s", sql))
	}
	assertBefore(t, sql, `DROP CONSTRAINT IF EXISTS "self_fk"`, `DROP CONSTRAINT IF EXISTS "key_id"`)
	assertBefore(t, sql, `DROP CONSTRAINT IF EXISTS "key_id"`, `ADD CONSTRAINT "key_id"`)
	assertBefore(t, sql, `ADD CONSTRAINT "key_id"`, `ADD CONSTRAINT "self_fk"`)
}

func TestPlanner_SkippedTableDropRetainsItsForeignKeys(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "children"}},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "parent_fk", TableName: "children", Type: "FOREIGN KEY"},
			{Name: "other_fk", TableName: "archives", Type: "FOREIGN KEY"},
		},
	}
	planner := postgres.New().WithSkipChangeKinds(diffpolicy.DropTable)
	sql := renderPostgresSkip(c, planner, diff, &schemamodel.Database{})
	c.Assert(sql, qt.Not(qt.Contains), `DROP CONSTRAINT IF EXISTS "parent_fk"`)
	c.Assert(sql, qt.Contains, `DROP CONSTRAINT IF EXISTS "other_fk"`)
	c.Assert(sql, qt.Contains, "skip: drop_table")
}
