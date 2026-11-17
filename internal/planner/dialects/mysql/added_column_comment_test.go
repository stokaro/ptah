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

// TestPlanner_AddedColumnComment_HappyPath pins the comment of a column the
// plan adds, on each engine this algorithm plans. MySQL and MariaDB write it
// inside the column definition. Oracle and SQL Server have no such clause, so
// there it is a statement of its own after the one that adds the column.
// Without it the column is added bare, and the next comparison plans the
// comment on its own: the schema takes two migrations to converge.
func TestPlanner_AddedColumnComment_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    string
	}{
		{
			name:    "mysql",
			dialect: platform.MySQL,
			caps:    capability.MySQL84(),
			want: "-- Modify table: tasks\n" +
				"ALTER TABLE `tasks` ADD COLUMN `note` VARCHAR(64) COMMENT 'plain note';\n\n" +
				"ALTER TABLE `tasks` ADD COLUMN `code` VARCHAR(64) GENERATED ALWAYS AS (upper(title)) STORED COMMENT 'derived code';\n\n",
		},
		{
			name:    "mariadb",
			dialect: platform.MariaDB,
			caps:    capability.MariaDB1011(),
			want: "-- Modify table: tasks\n" +
				"ALTER TABLE `tasks` ADD COLUMN `note` VARCHAR(64) COMMENT 'plain note';\n\n" +
				"ALTER TABLE `tasks` ADD COLUMN `code` VARCHAR(64) GENERATED ALWAYS AS (upper(title)) STORED COMMENT 'derived code';\n\n",
		},
		{
			name:    "oracle",
			dialect: platform.Oracle,
			caps:    capability.Oracle23(),
			want: "-- Modify table: tasks\n" +
				"ALTER TABLE tasks ADD (note VARCHAR2(64));\n" +
				"ALTER TABLE tasks ADD (code VARCHAR2(64) GENERATED ALWAYS AS (upper(title)) STORED);\n" +
				"COMMENT ON COLUMN tasks.note IS 'plain note';\n" +
				"COMMENT ON COLUMN tasks.code IS 'derived code';\n",
		},
		{
			name:    "sqlserver",
			dialect: platform.SQLServer,
			caps:    capability.SQLServer2022(),
			want: "-- Modify table: tasks\n" +
				"ALTER TABLE [tasks] ADD [note] NVARCHAR(64);\n" +
				"ALTER TABLE [tasks] ADD [code] AS (upper(title));\n" +
				"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'plain note', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'tasks', @level2type = N'COLUMN', @level2name = N'note';\n" +
				"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'derived code', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'tasks', @level2type = N'COLUMN', @level2name = N'code';\n",
		},
	}
	diff := &difftypes.SchemaDiff{
		DeclaredTables: []schemamodel.Table{{StructName: "Task", Name: "tasks"}},
		TablesModified: []difftypes.TableDiff{{TableName: "tasks", ColumnsAdded: difftypes.ColumnChanges{
			{StructName: "Task", Name: "note", Type: "VARCHAR(64)", Nullable: true, Comment: "plain note"},
			{
				StructName: "Task", Name: "code", Type: "VARCHAR(64)", Nullable: true,
				GeneratedExpression: "upper(title)", GeneratedKind: "STORED", Comment: "derived code",
			},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.NewForDialect(test.dialect, test.caps).GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
