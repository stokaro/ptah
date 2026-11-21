package postgres_test

import (
	"cmp"
	"maps"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/internal/planrefusal"
)

// generatedCodeChange is a stored generated column of tasks, declared with typ
// or text when typ is empty, whose expression changes from upper(title) to
// lower(title), beside the other changes given.
func generatedCodeChange(typ string, changes map[string]string, name *difftypes.NotNullConstraintNameChange) *difftypes.SchemaDiff {
	all := map[string]string{"generated": "STORED upper(title) -> STORED lower(title)"}
	maps.Copy(all, changes)
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "tasks", ColumnsModified: []difftypes.ColumnDiff{{
		ColumnName: "code",
		Changes:    all,
		Desired: schemamodel.Field{
			StructName: "Task", Name: "code", Type: cmp.Or(typ, "text"),
			GeneratedExpression: "lower(title)", GeneratedKind: "STORED",
		},
		NotNullConstraintNameChange: name,
	}}}}}
}

// TestPlanner_AGeneratedColumnsOtherChanges_HappyPath pins the changes of a
// generated column whose expression changes in the same plan. The expression
// is a statement of its own, which carries no other change, so the column's
// NOT NULL, its name and its type are planned beside it. Without them the plan applies
// cleanly, and the next comparison plans them on their own: the schema takes
// two migrations to converge. A target that cannot change the expression in
// place still gets them.
func TestPlanner_AGeneratedColumnsOtherChanges_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		typ     string
		changes map[string]string
		rename  *difftypes.NotNullConstraintNameChange
		want    string
	}{
		{
			name:    "the column gains NOT NULL",
			caps:    capability.Postgres18(),
			changes: map[string]string{"nullable": "true -> false"},
			want: `-- Add/modify columns for table: tasks
-- Modify generated column tasks.code: STORED upper(title) -> STORED lower(title)
ALTER TABLE "tasks" ALTER COLUMN "code" SET EXPRESSION AS (lower(title));

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
-- Modify generated column tasks.code: STORED upper(title) -> STORED lower(title)
ALTER TABLE "tasks" ALTER COLUMN "code" SET EXPRESSION AS (lower(title));

ALTER TABLE "tasks" RENAME CONSTRAINT "old_nn" TO "code_nn";

`,
		},
		{
			name:    "the column takes another type",
			caps:    capability.Postgres18(),
			typ:     "varchar(20)",
			changes: map[string]string{"type": "text -> varchar(20)"},
			want: `-- Add/modify columns for table: tasks
-- Modify generated column tasks.code: STORED upper(title) -> STORED lower(title)
ALTER TABLE "tasks" ALTER COLUMN "code" SET EXPRESSION AS (lower(title));

-- Modify column tasks.code: type: text -> varchar(20)
ALTER TABLE "tasks" ALTER COLUMN "code" TYPE varchar(20);

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

			nodes, err := planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), generatedCodeChange(test.typ, test.changes, test.rename))
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, test.caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestPlanner_AGeneratedColumnsChangesLeaveTheDiffAsItWas_HappyPath plans the change and
// reads the diff back. The rollback of a migration is planned from the same
// diff, so a plan that took the expression change out of it would leave the
// rollback without the expression.
func TestPlanner_AGeneratedColumnsChangesLeaveTheDiffAsItWas_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := generatedCodeChange("text", map[string]string{"nullable": "true -> false"}, nil)

	_, err := postgres.NewWithCapabilities(capability.Postgres18()).GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{
		"generated": "STORED upper(title) -> STORED lower(title)",
		"nullable":  "true -> false",
	})
}

// generatedKindChange is the generated column code of tasks, held as held and
// declared as declared, over one expression.
func generatedKindChange(held, declared string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "tasks", ColumnsModified: []difftypes.ColumnDiff{{
		ColumnName:            "code",
		Changes:               map[string]string{"generated": held + " upper(title) -> " + declared + " upper(title)"},
		PreviousGeneratedKind: held,
		Desired: schemamodel.Field{
			StructName: "Task", Name: "code", Type: "text",
			GeneratedExpression: "upper(title)", GeneratedKind: declared,
		},
	}}}}}
}

// TestPlanner_AGeneratedColumnsKind_FailurePath changes the kind of a generated
// column in either direction. ALTER COLUMN SET EXPRESSION keeps the kind, so
// the statement would run, change nothing, and be planned again on the next
// run. The plan stops and names the column.
func TestPlanner_AGeneratedColumnsKind_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		held     string
		declared string
		want     string
	}{
		{
			name: "virtual to stored", held: "VIRTUAL", declared: "STORED",
			want: `unsupported feature: generated column tasks.code is VIRTUAL and the schema declares it STORED; ` +
				`the postgres planner cannot change a generated column's kind in place, so drop the column and add it back, or declare it VIRTUAL`,
		},
		{
			name: "stored to virtual", held: "STORED", declared: "VIRTUAL",
			want: `unsupported feature: generated column tasks.code is STORED and the schema declares it VIRTUAL; ` +
				`the postgres planner cannot change a generated column's kind in place, so drop the column and add it back, or declare it STORED`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.NewWithCapabilities(capability.Postgres18()).
				GenerateMigrationAST(c.Context(), must.Must(builtin.New()), generatedKindChange(test.held, test.declared))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestPlanner_GeneratedKindRefusalNamesEveryColumn_FailurePath changes the kind
// of two generated columns in two tables, one in each direction, beside a third
// column whose expression changes under one kind. The refusal carries the two
// columns with both kinds, in the comparison's order, so a plan report can name
// each one; the third is not a kind change and stays out of it.
func TestPlanner_GeneratedKindRefusalNamesEveryColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := generatedKindChange("VIRTUAL", "STORED")
	second := generatedKindChange("STORED", "VIRTUAL").TablesModified[0]
	second.TableName = "orders"
	same := generatedKindChange("STORED", "STORED").TablesModified[0].ColumnsModified[0]
	same.ColumnName = "label"
	same.Desired.Name = "label"
	same.Desired.GeneratedExpression = "lower(title)"
	second.ColumnsModified = append(second.ColumnsModified, same)
	diff.TablesModified = append(diff.TablesModified, second)

	nodes, err := postgres.NewWithCapabilities(capability.Postgres18()).
		GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)

	var refusal *planrefusal.GeneratedKindError
	c.Assert(err, qt.ErrorAs, &refusal)
	c.Assert(refusal.Columns, qt.DeepEquals, []planrefusal.GeneratedKind{
		{Table: "tasks", Column: "code", HeldKind: "VIRTUAL", DeclaredKind: "STORED"},
		{Table: "orders", Column: "code", HeldKind: "STORED", DeclaredKind: "VIRTUAL"},
	})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: `+
		`generated column tasks\.code is VIRTUAL and the schema declares it STORED; .* or declare it VIRTUAL; `+
		`generated column orders\.code is STORED and the schema declares it VIRTUAL; .* or declare it STORED`)
	c.Assert(nodes, qt.IsNil)
}

// TestPlanner_AGeneratedColumnsExpressionOfOneKind_HappyPath is the control:
// the column keeps its kind and changes its expression, which ALTER COLUMN SET
// EXPRESSION does on a VIRTUAL column as on a STORED one.
func TestPlanner_AGeneratedColumnsExpressionOfOneKind_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := generatedKindChange("VIRTUAL", "VIRTUAL")
	diff.TablesModified[0].ColumnsModified[0].Desired.GeneratedExpression = "lower(title)"

	nodes, err := postgres.NewWithCapabilities(capability.Postgres18()).WithStatementStyle(planext.StatementStyleExact).
		GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "tasks" ALTER COLUMN "code" SET EXPRESSION AS (lower(title));`)
}
