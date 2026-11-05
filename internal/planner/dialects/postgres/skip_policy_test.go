package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

func skipPolicyFixture() (*difftypes.SchemaDiff, *schemamodel.Database) {
	diff := &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "legacy"}},
		EnumsRemoved:  difftypes.EnumChanges{{Name: "legacy_status"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_legacy", TableName: "users"},
		},
		TablesModified: []difftypes.TableDiff{
			{TableName: "users", ColumnsRemoved: difftypes.ColumnChanges{{Name: "middle_name"}}},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
	}
	return diff, desired
}

// renderPostgresSkip plans diff for PostgreSQL through the shared planning
// entry, which is where a skip policy is applied for every target.
func renderPostgresSkip(c *qt.C, diff *difftypes.SchemaDiff, desired *schemamodel.Database, kinds ...planext.ChangeKind) string {
	c.Helper()
	sql, err := planner.GenerateSchemaDiffSQLWithOptions(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, desired), platform.Postgres,
		planext.Options{SkipChangeKinds: kinds},
	)
	c.Assert(err, qt.IsNil)
	return sql
}

func TestPlanner_SkipChangeKinds(t *testing.T) {
	diff, desired := skipPolicyFixture()

	t.Run("no policy emits every destructive statement", func(t *testing.T) {
		c := qt.New(t)
		sql := renderPostgresSkip(c, diff, desired)
		c.Assert(sql, qt.Contains, "DROP TABLE IF EXISTS \"legacy\"")
		c.Assert(sql, qt.Contains, "DROP TYPE IF EXISTS \"legacy_status\"")
		c.Assert(sql, qt.Contains, "DROP INDEX IF EXISTS \"idx_legacy\"")
		c.Assert(sql, qt.Contains, "DROP COLUMN \"middle_name\"")
	})

	t.Run("skip drop_table omits the drop and comments it", func(t *testing.T) {
		c := qt.New(t)
		sql := renderPostgresSkip(c, diff, desired, planext.DropTable)
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
		sql := renderPostgresSkip(c, diff, desired,
			planext.DropTable, planext.DropColumn, planext.DropIndex, planext.DropEnum,
		)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
		c.Assert(sql, qt.Not(qt.Contains), "DROP TYPE IF EXISTS")
		c.Assert(sql, qt.Not(qt.Contains), "DROP INDEX IF EXISTS")
		c.Assert(sql, qt.Not(qt.Contains), "DROP COLUMN \"middle_name\"")
		c.Assert(sql, qt.Contains, "skip: drop_table")
		c.Assert(sql, qt.Contains, "skip: drop_column")
		c.Assert(sql, qt.Contains, "skip: drop_index")
		c.Assert(sql, qt.Contains, "skip: drop_enum")
	})
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
	sql := renderPostgresSkip(c, diff, &schemamodel.Database{}, planext.DropTable)
	c.Assert(sql, qt.Not(qt.Contains), `DROP CONSTRAINT IF EXISTS "parent_fk"`)
	c.Assert(sql, qt.Contains, `DROP CONSTRAINT IF EXISTS "other_fk"`)
	c.Assert(sql, qt.Contains, "skip: drop_table")
}
