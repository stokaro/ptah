package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/engine/builtin"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/planner/dialects/mysql"
	oracleplanner "ptah.run/internal/planner/dialects/oracle"
)

// replacedForeignKey is a diff that replaces the foreign key the server named
// SYS_C009299 on CH with one the declaration names fk_ch_x_y, over the columns
// x and y and the reference to P (a, b), then drops an unrelated key.
func replacedForeignKey(foreignTable string, foreignColumns ...string) *difftypes.SchemaDiff {
	semantics := identifier.ForDialect("oracle")
	return &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{{
			Name: "fk_ch_x_y", TableName: "CH", Type: "FOREIGN KEY", Columns: []string{"x", "y"},
			ForeignTable: "P", ForeignColumns: []string{"a", "b"},
			Identity: constraintscope.Identity(semantics, "CH", "fk_ch_x_y"),
		}},
		ConstraintsRemoved: difftypes.ConstraintRemovals{{
			Name: "SYS_C009299", TableName: "CH", Type: "FOREIGN KEY",
			Identity: constraintscope.Identity(semantics, "CH", "SYS_C009299"),
		}},
		ForeignKeysRemovedWithTables: []difftypes.ForeignKeyRemovalInfo{{
			Name: "SYS_C009299", TableName: "CH", Columns: []string{"X", "Y"},
			ForeignTable: foreignTable, ForeignColumns: foreignColumns,
			Identity: constraintscope.Identity(semantics, "CH", "SYS_C009299"),
		}},
	}
}

func oracleStatements(c *qt.C, diff *difftypes.SchemaDiff) []string {
	c.Helper()
	nodes, err := oracleplanner.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("oracle", nodes...)
	c.Assert(err, qt.IsNil)
	var statements []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.TrimSpace(line) != "" {
			statements = append(statements, strings.TrimSpace(line))
		}
	}
	return statements
}

// Oracle answers ORA-02275 to a second foreign key over the same columns and
// reference while the first stands, whatever either is named, so a plan that
// replaces one drops the old key before it adds the new one.
func TestPlanner_OracleDropsAReplacedForeignKeyFirst_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "the same columns and reference under another name",
			diff: replacedForeignKey("P", "A", "B"),
			want: []string{
				"ALTER TABLE CH DROP CONSTRAINT SYS_C009299;",
				"ALTER TABLE CH ADD CONSTRAINT fk_ch_x_y FOREIGN KEY (x, y) REFERENCES P (a, b);",
			},
		},
		{
			name: "a reference spelled in the other case",
			diff: replacedForeignKey("p", "a", "b"),
			want: []string{
				"ALTER TABLE CH DROP CONSTRAINT SYS_C009299;",
				"ALTER TABLE CH ADD CONSTRAINT fk_ch_x_y FOREIGN KEY (x, y) REFERENCES P (a, b);",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(oracleStatements(c, test.diff), qt.DeepEquals, test.want)
		})
	}
}

// A removal that is not a replacement keeps the order the plan always had: the
// new key is added, and the old one is dropped with the other removals. A key
// over another table or other columns is another key, and Oracle takes both.
func TestPlanner_OracleKeepsTheOrderOfAnUnrelatedForeignKey_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "another referenced table", diff: replacedForeignKey("Q", "A", "B")},
		{name: "other referenced columns", diff: replacedForeignKey("P", "B", "A")},
		{name: "fewer referenced columns", diff: replacedForeignKey("P", "A")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(oracleStatements(c, test.diff), qt.DeepEquals, []string{
				"ALTER TABLE CH ADD CONSTRAINT fk_ch_x_y FOREIGN KEY (x, y) REFERENCES P (a, b);",
				"ALTER TABLE CH DROP CONSTRAINT SYS_C009299;",
			})
		})
	}
}

// MySQL takes a second foreign key over the same columns and reference, so
// only a target that states OneForeignKeyPerReference drops first.
func TestPlanner_MySQLKeepsTheOrderOfAReplacedForeignKey_HappyPath(t *testing.T) {
	c := qt.New(t)

	nodes, err := mysql.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()), replacedForeignKey("P", "A", "B"))
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("mysql", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals,
		"ALTER TABLE `CH` ADD CONSTRAINT `fk_ch_x_y` FOREIGN KEY (`x`, `y`) REFERENCES `P`(`a`, `b`);\n\n"+
			"ALTER TABLE `CH` DROP FOREIGN KEY `SYS_C009299`;\n\n")
}
