package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func skipPolicyFixture() (*difftypes.SchemaDiff, *schemamodel.Database) {
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"legacy"},
		EnumsRemoved:  []string{"legacy_status"},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_legacy", TableName: "users"},
		},
		TablesModified: []difftypes.TableDiff{
			{TableName: "users", ColumnsRemoved: []string{"middle_name"}},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
	}
	return diff, desired
}

func renderPostgresSkip(c *qt.C, planner *postgres.Planner, diff *difftypes.SchemaDiff, desired *schemamodel.Database) string {
	nodes, err := planner.GenerateMigrationASTChecked(diff, desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

func TestPlanner_SkipChangeKinds(t *testing.T) {
	diff, desired := skipPolicyFixture()

	t.Run("no policy emits every destructive statement", func(t *testing.T) {
		c := qt.New(t)
		sql := renderPostgresSkip(c, postgres.New(), diff, desired)
		c.Assert(sql, qt.Contains, "DROP TABLE IF EXISTS \"legacy\"")
		c.Assert(sql, qt.Contains, "DROP TYPE IF EXISTS \"legacy_status\"")
		c.Assert(sql, qt.Contains, "DROP INDEX IF EXISTS \"idx_legacy\"")
		c.Assert(sql, qt.Contains, "DROP COLUMN \"middle_name\"")
	})

	t.Run("skip drop_table omits the drop and comments it", func(t *testing.T) {
		c := qt.New(t)
		planner := postgres.New().WithSkipChangeKinds(diffpolicy.DropTable)
		sql := renderPostgresSkip(c, planner, diff, desired)
		// The DDL statement is gone; only the SKIP comment mentions the table.
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
		c.Assert(sql, qt.Contains, "SKIP: DROP TABLE of legacy omitted by diff policy (skip: drop_table)")
		// Other destructive statements remain.
		c.Assert(sql, qt.Contains, "DROP TYPE IF EXISTS \"legacy_status\"")
		c.Assert(sql, qt.Contains, "DROP INDEX IF EXISTS \"idx_legacy\"")
		c.Assert(sql, qt.Contains, "DROP COLUMN \"middle_name\"")
	})

	t.Run("skip all destructive kinds omits them all", func(t *testing.T) {
		c := qt.New(t)
		planner := postgres.New().WithSkipChangeKinds(
			diffpolicy.DropTable, diffpolicy.DropColumn, diffpolicy.DropIndex, diffpolicy.DropEnum,
		)
		sql := renderPostgresSkip(c, planner, diff, desired)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
		c.Assert(sql, qt.Not(qt.Contains), "DROP TYPE IF EXISTS")
		c.Assert(sql, qt.Not(qt.Contains), "DROP INDEX IF EXISTS")
		c.Assert(sql, qt.Not(qt.Contains), "DROP COLUMN \"middle_name\"")
		c.Assert(sql, qt.Contains, "skip: drop_table")
		c.Assert(sql, qt.Contains, "skip: drop_column")
		c.Assert(sql, qt.Contains, "skip: drop_index")
		c.Assert(sql, qt.Contains, "skip: drop_enum")
	})

	t.Run("WithSkipChangeKinds is immutable and no-op safe", func(t *testing.T) {
		c := qt.New(t)
		base := postgres.New()
		derived := base.WithSkipChangeKinds(diffpolicy.DropTable)
		// The base planner is unaffected by the derived policy.
		c.Assert(renderPostgresSkip(c, base, diff, desired), qt.Contains, "DROP TABLE IF EXISTS")
		c.Assert(renderPostgresSkip(c, derived, diff, desired), qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
		// Passing no kinds returns the receiver unchanged.
		c.Assert(base.WithSkipChangeKinds(), qt.Equals, base)
	})
}
