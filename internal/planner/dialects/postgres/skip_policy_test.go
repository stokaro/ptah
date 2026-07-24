package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/diffpolicy"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func skipPolicyFixture() (*types.SchemaDiff, *goschema.Database) {
	diff := &types.SchemaDiff{
		TablesRemoved:  []string{"legacy"},
		EnumsRemoved:   []string{"legacy_status"},
		IndexesRemoved: []string{"idx_legacy"},
		TablesModified: []types.TableDiff{
			{TableName: "users", ColumnsRemoved: []string{"middle_name"}},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
	}
	return diff, generated
}

func renderPostgresSkip(c *qt.C, planner *postgres.Planner, diff *types.SchemaDiff, generated *goschema.Database) string {
	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

func TestPlanner_SkipChangeKinds(t *testing.T) {
	diff, generated := skipPolicyFixture()

	t.Run("no policy emits every destructive statement", func(t *testing.T) {
		c := qt.New(t)
		sql := renderPostgresSkip(c, postgres.New(), diff, generated)
		c.Assert(sql, qt.Contains, "DROP TABLE IF EXISTS \"legacy\"")
		c.Assert(sql, qt.Contains, "DROP TYPE IF EXISTS \"legacy_status\"")
		c.Assert(sql, qt.Contains, "DROP INDEX IF EXISTS \"idx_legacy\"")
		c.Assert(sql, qt.Contains, "DROP COLUMN \"middle_name\"")
	})

	t.Run("skip drop_table omits the drop and comments it", func(t *testing.T) {
		c := qt.New(t)
		planner := postgres.New().WithSkipChangeKinds(diffpolicy.DropTable)
		sql := renderPostgresSkip(c, planner, diff, generated)
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
		sql := renderPostgresSkip(c, planner, diff, generated)
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
		c.Assert(renderPostgresSkip(c, base, diff, generated), qt.Contains, "DROP TABLE IF EXISTS")
		c.Assert(renderPostgresSkip(c, derived, diff, generated), qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
		// Passing no kinds returns the receiver unchanged.
		c.Assert(base.WithSkipChangeKinds(), qt.Equals, base)
	})
}
