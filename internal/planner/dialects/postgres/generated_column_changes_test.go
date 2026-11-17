package postgres_test

import (
	"maps"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// generatedCodeChange is a stored generated column of tasks whose expression
// changes from upper(title) to lower(title), beside the other changes given.
func generatedCodeChange(changes map[string]string, name *difftypes.NotNullConstraintNameChange) *difftypes.SchemaDiff {
	all := map[string]string{"generated": "STORED upper(title) -> STORED lower(title)"}
	maps.Copy(all, changes)
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "tasks", ColumnsModified: []difftypes.ColumnDiff{{
		ColumnName: "code",
		Changes:    all,
		Desired: schemamodel.Field{
			StructName: "Task", Name: "code", Type: "text",
			GeneratedExpression: "lower(title)", GeneratedKind: "STORED",
		},
		NotNullConstraintNameChange: name,
	}}}}}
}

// TestPlanner_AGeneratedColumnsOtherChanges_HappyPath pins the changes of a
// generated column whose expression changes in the same plan. The expression
// is a statement of its own, which carries no other change, so the column's
// NOT NULL and its name are planned beside it. Without them the plan applies
// cleanly, and the next comparison plans them on their own: the schema takes
// two migrations to converge. A target that cannot change the expression in
// place still gets them.
func TestPlanner_AGeneratedColumnsOtherChanges_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		changes map[string]string
		rename  *difftypes.NotNullConstraintNameChange
		want    string
	}{
		{
			name:    "the column gains NOT NULL",
			caps:    capability.Postgres18(),
			changes: map[string]string{"nullable": "true -> false"},
			want: `-- Add/modify columns for table: tasks
ALTER TABLE "tasks" ALTER COLUMN "code" SET EXPRESSION AS (lower(title));

-- Modify generated column tasks.code: STORED upper(title) -> STORED lower(title)
-- Modify column tasks.code: nullable: true -> false
-- POSTGRES: SET NOT NULL fails if any row of "tasks" holds NULL in "code"; the column declares no default to fill it with.
ALTER TABLE "tasks" ALTER COLUMN "code" SET NOT NULL;

`,
		},
		{
			name:   "the column's NOT NULL takes another name",
			caps:   capability.Postgres18(),
			rename: &difftypes.NotNullConstraintNameChange{Current: "old_nn", Desired: "code_nn"},
			want: `-- Add/modify columns for table: tasks
ALTER TABLE "tasks" ALTER COLUMN "code" SET EXPRESSION AS (lower(title));

-- Modify generated column tasks.code: STORED upper(title) -> STORED lower(title)
ALTER TABLE "tasks" RENAME CONSTRAINT "old_nn" TO "code_nn";

`,
		},
		{
			name:    "a target that cannot change the expression",
			caps:    capability.Postgres16(),
			changes: map[string]string{"nullable": "true -> false"},
			want: `-- Add/modify columns for table: tasks
-- WARNING: Generated column tasks.code changed, but ALTER COLUMN SET EXPRESSION requires target capability alter_generated_column_expression, unavailable on this target (PostgreSQL added it in 17); manual migration required.
-- Modify column tasks.code: nullable: true -> false
-- POSTGRES: SET NOT NULL fails if any row of "tasks" holds NULL in "code"; the column declares no default to fill it with.
ALTER TABLE "tasks" ALTER COLUMN "code" SET NOT NULL;

`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.NewWithCapabilities(test.caps).WithStatementStyle(planext.StatementStyleExact)

			nodes, err := planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), generatedCodeChange(test.changes, test.rename))
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, test.caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestPlanner_AGeneratedColumnsChangesLeaveTheDiffAsItWas plans the change and
// reads the diff back. The rollback of a migration is planned from the same
// diff, so a plan that took the expression change out of it would leave the
// rollback without the expression.
func TestPlanner_AGeneratedColumnsChangesLeaveTheDiffAsItWas(t *testing.T) {
	c := qt.New(t)
	diff := generatedCodeChange(map[string]string{"nullable": "true -> false"}, nil)

	_, err := postgres.NewWithCapabilities(capability.Postgres18()).GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{
		"generated": "STORED upper(title) -> STORED lower(title)",
		"nullable":  "true -> false",
	})
}
