package oracle_test

import (
	"context"
	"maps"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/constraintscope"
	mssqlplanner "ptah.run/internal/planner/dialects/mssql"
	oracleplanner "ptah.run/internal/planner/dialects/oracle"
)

// migrationPlanner is what Oracle and SQL Server share.
type migrationPlanner interface {
	GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error)
}

// planFor plans diff for dialect and answers the plan's error and its ALTER
// statements, one per element.
func planFor(c *qt.C, dialect string, diff *difftypes.SchemaDiff) ([]string, error) {
	c.Helper()
	planners := map[string]migrationPlanner{"oracle": oracleplanner.New(), "sqlserver": mssqlplanner.New()}
	planned, err := planners[dialect].GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	if err != nil {
		return nil, err
	}
	sql, err := builtin.RenderSQL(dialect, planned...)
	c.Assert(err, qt.IsNil)
	var statements []string
	for _, line := range statementsOf(sql) {
		if strings.HasPrefix(line, "ALTER TABLE") {
			statements = append(statements, line)
		}
	}
	return statements, nil
}

// alterStatements is the ALTER statements of a plan that is expected to build.
func alterStatements(c *qt.C, dialect string, diff *difftypes.SchemaDiff) []string {
	c.Helper()
	statements, err := planFor(c, dialect, diff)
	c.Assert(err, qt.IsNil)
	return statements
}

// keyLoss is a diff that takes the primary key from column T.ID, which the
// read carries on the column alone, the way Oracle's does for a key it named.
// ownsNotNull states that the key is the only reason the column is NOT NULL.
func keyLoss(declared schemamodel.Field, ownsNotNull bool, changes map[string]string) *difftypes.SchemaDiff {
	declared.Name, declared.StructName, declared.Type = "ID", "T", "NUMBER(10)"
	merged := map[string]string{"primary_key": "true -> false"}
	maps.Copy(merged, changes)
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "T",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "ID", Changes: merged, Desired: declared, CurrentNotNullFromKey: ownsNotNull,
		}},
	}}}
}

// keyReplacement is a diff that replaces the table's key, which the database
// holds under PK_OLD, with one over column B that the declaration names PK_NEW.
func keyReplacement() *difftypes.SchemaDiff {
	semantics := identifier.Semantics{}
	return &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{{
			Name: "PK_OLD", TableName: "T", Type: "PRIMARY KEY", Identity: constraintscope.Identity(semantics, "T", "PK_OLD"),
		}},
		ConstraintsAdded: difftypes.ConstraintAdditions{{
			Name: "pk_new", TableName: "T", Type: "PRIMARY KEY", Columns: []string{"B"}, Identity: constraintscope.Identity(semantics, "T", "pk_new"),
		}},
	}
}

// A column whose NOT NULL comes from its key alone is nullable once the key is
// gone, so a declaration that wants NOT NULL gets it ahead of the drop. After
// it, the clause answers ORA-01442 on a column that holds its own.
func TestPlanner_OracleStatesNotNullAheadOfTheKeyThatHeldIt(t *testing.T) {
	c := qt.New(t)

	statements := alterStatements(c, "oracle", keyLoss(schemamodel.Field{}, true, nil))

	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE T MODIFY (ID NUMBER(10) NOT NULL);",
		"ALTER TABLE T DROP PRIMARY KEY;",
	})
}

// A column that holds a NOT NULL of its own needs no statement for it, and the
// key goes by name-free DROP PRIMARY KEY, which finds the key Oracle named.
func TestPlanner_OracleDropsTheKeyANamedColumnHeld(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Field
		owns     bool
		changes  map[string]string
		want     []string
	}{
		{
			name: "a column with its own NOT NULL",
			want: []string{"ALTER TABLE T DROP PRIMARY KEY;"},
		},
		{
			name:     "a nullable declaration over a column that is NOT NULL through the key",
			declared: schemamodel.Field{Nullable: true},
			owns:     true,
			changes:  map[string]string{"nullable": "false -> true"},
			want:     []string{"ALTER TABLE T DROP PRIMARY KEY;"},
		},
		{
			name:     "a nullable declaration over a column with its own NOT NULL",
			declared: schemamodel.Field{Nullable: true},
			changes:  map[string]string{"nullable": "false -> true"},
			want:     []string{"ALTER TABLE T DROP PRIMARY KEY;", "ALTER TABLE T MODIFY (ID NUMBER(10) NULL);"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := alterStatements(c, "oracle", keyLoss(test.declared, test.owns, test.changes))

			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// Oracle holds one primary key per table: ADD while the old key stands answers
// ORA-02260, so the old key goes first even though removals follow additions.
func TestPlanner_OracleDropsTheOldKeyBeforeAddingTheNewOne(t *testing.T) {
	c := qt.New(t)

	statements := alterStatements(c, "oracle", keyReplacement())

	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE T DROP CONSTRAINT PK_OLD;",
		"ALTER TABLE T ADD CONSTRAINT pk_new PRIMARY KEY (B);",
	})
}

// Nothing is dropped first when the plan only adds a key, or only removes one.
func TestPlanner_OracleLeavesAKeyAdditionAndAKeyRemovalAlone(t *testing.T) {
	added := keyReplacement()
	added.ConstraintsRemoved = nil
	removed := keyReplacement()
	removed.ConstraintsAdded = nil
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "only an addition", diff: added, want: []string{"ALTER TABLE T ADD CONSTRAINT pk_new PRIMARY KEY (B);"}},
		{name: "only a removal", diff: removed, want: []string{"ALTER TABLE T DROP CONSTRAINT PK_OLD;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(alterStatements(c, "oracle", test.diff), qt.DeepEquals, test.want)
		})
	}
}

// SQL Server shares the planner and its ordering is not measured with Oracle's:
// the addition still comes first.
func TestPlanner_OracleKeyOrderStaysOffTheSQLServerPlan_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements := alterStatements(c, "sqlserver", keyReplacement())

	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, "ADD CONSTRAINT")
	c.Assert(statements[1], qt.Contains, "DROP CONSTRAINT")
}

// A column's own key flag has no statement on SQL Server, which refuses the
// change as before, and no DROP PRIMARY KEY is written for it.
func TestPlanner_OracleKeyOrderStaysOffTheSQLServerPlan_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := planFor(c, "sqlserver", keyLoss(schemamodel.Field{}, true, nil))

	c.Assert(err, qt.ErrorMatches, `SQL Server planner only supports .* unsupported changes: primary_key`)
	c.Assert(statements, qt.IsNil)
}
