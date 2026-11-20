package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
)

// TestPlanner_OracleColumnKeyDrop_HappyPath plans an Oracle column, parent.code,
// that loses the key Oracle named for it, while the plan drops the foreign keys
// that reference the key. The foreign keys are dropped once each, ahead of
// `DROP UNIQUE (code)`. Left with the other constraint removals, they come
// after it, and Oracle refuses the drop of a key a foreign key references with
// ORA-02273. A foreign key over a two-column key, and one over a column that
// keeps its key, stay with the other removals.
func TestPlanner_OracleColumnKeyDrop_HappyPath(t *testing.T) {
	c := qt.New(t)
	removed := func(name, table string, foreignColumns ...string) difftypes.ForeignKeyRemovalInfo {
		return difftypes.ForeignKeyRemovalInfo{
			Name: name, TableName: table, Columns: []string{"pc"}, ForeignTable: "parent", ForeignColumns: foreignColumns,
		}
	}
	diff := &difftypes.SchemaDiff{
		DeclaredTables: []schemamodel.Table{{StructName: "Parent", Name: "parent"}},
		TablesModified: []difftypes.TableDiff{{TableName: "parent", ColumnsModified: []difftypes.ColumnDiff{
			{
				ColumnName: "code",
				Changes:    map[string]string{"unique": "true -> false"},
				Desired:    schemamodel.Field{StructName: "Parent", Name: "code", Type: "VARCHAR2(20)", Nullable: true},
			},
			{
				ColumnName: "other",
				Changes:    map[string]string{"unique": "false -> true"},
				Desired:    schemamodel.Field{StructName: "Parent", Name: "other", Type: "VARCHAR2(20)", Nullable: true, Unique: true},
			},
		}}},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{
			{Name: "CHILD_FK", TableName: "APP.CHILD", Type: "FOREIGN KEY"},
			{Name: "GONE_FK", TableName: "APP.GONE", Type: "FOREIGN KEY"},
			{Name: "PAIR_FK", TableName: "APP.PAIR", Type: "FOREIGN KEY"},
			{Name: "OTHER_FK", TableName: "APP.OTHER", Type: "FOREIGN KEY"},
		},
		ForeignKeysRemovedWithTables: []difftypes.ForeignKeyRemovalInfo{
			removed("CHILD_FK", "APP.CHILD", "CODE"),
			removed("GONE_FK", "APP.GONE", "CODE"),
			removed("PAIR_FK", "APP.PAIR", "CODE", "OTHER"),
			removed("OTHER_FK", "APP.OTHER", "OTHER"),
		},
	}

	nodes, err := mysql.NewForDialect(platform.Oracle, capability.Oracle23()).
		GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "ALTER TABLE APP.CHILD DROP CONSTRAINT CHILD_FK;\n"+
		"ALTER TABLE APP.GONE DROP CONSTRAINT GONE_FK;\n"+
		"-- Modify table: parent\n"+
		"-- Modify column parent.code: unique: true -> false\n"+
		"ALTER TABLE parent DROP UNIQUE (code);\n"+
		"-- Modify column parent.other: unique: false -> true\n"+
		"ALTER TABLE parent MODIFY (other VARCHAR2(20) UNIQUE);\n"+
		"ALTER TABLE APP.PAIR DROP CONSTRAINT PAIR_FK;\n"+
		"ALTER TABLE APP.OTHER DROP CONSTRAINT OTHER_FK;\n")
}
