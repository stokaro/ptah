package mssql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mssql"
)

func TestNewWithCapabilitiesUsesSQLServerDialect(t *testing.T) {
	c := qt.New(t)

	plan := mssql.New()
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "users",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "status",
				Changes: map[string]string{
					"unique": "false -> true",
				},
			}},
		}},
	}

	_, err := plan.GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner only supports type, nullability and default changes on users\.status; unsupported changes: unique.*`)
}

func TestNewWithCapabilitiesRejectsSQLServerColumnRemoval(t *testing.T) {
	c := qt.New(t)

	plan := mssql.New()
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy_id"}},
		}},
	}

	_, err := plan.GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)

	c.Assert(err, qt.ErrorMatches, `.*SQL Server planner does not support automatic DROP COLUMN for users; write an explicit migration that drops dependent constraints and indexes first.*`)
}

// TestAUniqueOverOneColumnIsAConstraintChange is a column that reads as losing or gaining
// its uniqueness because a UNIQUE over it alone goes or comes. The constraint statement
// makes the change, so the plan holds that and no column change. A key over another
// column leaves the change to the column, which this planner refuses.
func TestAUniqueOverOneColumnIsAConstraintChange(t *testing.T) {
	current := schemacapture.TableObservation{
		Table: catalog.Table{Name: "t"},
		Constraints: []catalog.Constraint{
			{Name: "x", TableName: "t", Type: "UNIQUE", ColumnNames: []string{"v"}},
			{Name: "y", TableName: "t", Type: "UNIQUE", ColumnNames: []string{"w"}},
		},
	}
	column := difftypes.ColumnDiff{ColumnName: "v", Changes: map[string]string{"unique": "true -> false"}}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "dropped",
			diff: &difftypes.SchemaDiff{
				TablesModified:     []difftypes.TableDiff{{TableName: "t", Current: current, ColumnsModified: []difftypes.ColumnDiff{column}}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "x", TableName: "t", Type: "UNIQUE"}},
			},
			want: "DROP CONSTRAINT IF EXISTS [x]",
		},
		{
			name: "added",
			diff: &difftypes.SchemaDiff{
				TablesModified:   []difftypes.TableDiff{{TableName: "t", Current: current, ColumnsModified: []difftypes.ColumnDiff{column}}},
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "z", TableName: "t", Type: "UNIQUE", Columns: []string{"v"}}},
			},
			want: "ADD CONSTRAINT [z] UNIQUE ([v])",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mssql.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), test.diff)

			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("sqlserver", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), "ALTER COLUMN")
		})
	}
}

// TestAUniqueOverAnotherColumnLeavesTheChange_FailurePath is a dropped UNIQUE that does
// not cover the column that reads as changed, and one over several columns. The column's
// change is its own, and this planner refuses it.
func TestAUniqueOverAnotherColumnLeavesTheChange_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
	}{
		{name: "over another column", columns: []string{"w"}},
		{name: "over several columns", columns: []string{"v", "w"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName: "t",
					Current: schemacapture.TableObservation{
						Table:       catalog.Table{Name: "t"},
						Constraints: []catalog.Constraint{{Name: "x", TableName: "t", Type: "UNIQUE", ColumnNames: test.columns}},
					},
					ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "v", Changes: map[string]string{"unique": "true -> false"}}},
				}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "x", TableName: "t", Type: "UNIQUE"}},
			}

			_, err := mssql.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

			c.Assert(err, qt.ErrorMatches, `.*unsupported changes: unique.*`)
		})
	}
}

// TestAddingAPrimaryKeyKeepsItsName is a key the diff names. SQL Server keeps the name,
// and a later file and the rollback have to write it.
func TestAddingAPrimaryKeyKeepsItsName(t *testing.T) {
	c := qt.New(t)
	engine := must.Must(builtin.New())
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{
		Name: "t_pk", TableName: "t", Type: "PRIMARY KEY", Columns: []string{"id", "k"},
	}}}

	nodes, err := mssql.New().GenerateMigrationAST(context.Background(), engine, diff)

	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("sqlserver", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE [t] ADD CONSTRAINT [t_pk] PRIMARY KEY ([id], [k]);")
}
