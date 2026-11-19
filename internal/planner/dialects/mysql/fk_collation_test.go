package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// foreignKeyCollationDesired is a parent key and a foreign key that
// references it, both declared with utf8mb4_bin.
func foreignKeyCollationDesired() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "parent", StructName: "Parent"},
			{Name: "child", StructName: "Child"},
		},
		Fields: []schemamodel.Field{
			{Name: "code", Type: "VARCHAR(10)", StructName: "Parent", Primary: true, Collate: "utf8mb4_bin"},
			{Name: "code", Type: "VARCHAR(10)", StructName: "Child", Nullable: true, Foreign: "parent(code)", Collate: "utf8mb4_bin"},
		},
	}
}

// TestPlanner_ColumnCollationChange_ReleasesTheForeignKey changes the
// collation of a foreign key column, of the key it references, or of both.
// The server refuses either change while the foreign key stands, so the plan
// drops the key, changes the columns, and adds the key back after both.
func TestPlanner_ColumnCollationChange_ReleasesTheForeignKey(t *testing.T) {
	collation := map[string]string{"collation": "utf8mb4_unicode_ci -> utf8mb4_bin"}
	for _, dialect := range mysqlFamilyDialects {
		for _, row := range []struct {
			name     string
			modified []difftypes.TableDiff
			changed  []string
		}{
			{
				name:     "the referencing column",
				modified: []difftypes.TableDiff{{TableName: "child", ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "code", Changes: collation}}}},
				changed:  []string{"ALTER TABLE child MODIFY COLUMN code VARCHAR(10) COLLATE utf8mb4_bin;"},
			},
			{
				name:     "the referenced column",
				modified: []difftypes.TableDiff{{TableName: "parent", ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "code", Changes: collation}}}},
				changed:  []string{"ALTER TABLE parent MODIFY COLUMN code VARCHAR(10) COLLATE utf8mb4_bin NOT NULL;"},
			},
			{
				name: "both columns",
				modified: []difftypes.TableDiff{
					{TableName: "child", ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "code", Changes: collation}}},
					{TableName: "parent", ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "code", Changes: collation}}},
				},
				changed: []string{
					"ALTER TABLE child MODIFY COLUMN code VARCHAR(10) COLLATE utf8mb4_bin;",
					"ALTER TABLE parent MODIFY COLUMN code VARCHAR(10) COLLATE utf8mb4_bin NOT NULL;",
				},
			},
		} {
			t.Run(dialect+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)

				sql := renderMySQLFamily(c, dialect, &difftypes.SchemaDiff{TablesModified: row.modified}, foreignKeyCollationDesired())

				for _, changed := range row.changed {
					assertContainsBefore(c, sql, "ALTER TABLE child DROP FOREIGN KEY fk_child_code", changed)
					assertContainsBefore(c, sql, changed,
						"ALTER TABLE child ADD CONSTRAINT fk_child_code FOREIGN KEY (code) REFERENCES parent(code);")
				}
				c.Assert(strings.Count(sql, "DROP FOREIGN KEY fk_child_code"), qt.Equals, 1)
				c.Assert(strings.Count(sql, "ADD CONSTRAINT fk_child_code"), qt.Equals, 1)
			})
		}
	}
}

// TestPlanner_ColumnCommentChange_KeepsTheForeignKey changes the comment of a
// foreign key column, which the server accepts while the key stands, so no
// key is released.
func TestPlanner_ColumnCommentChange_KeepsTheForeignKey(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "child", ColumnsModified: []difftypes.ColumnDiff{
				{ColumnName: "code", Changes: map[string]string{"comment": " -> the parent's code"}},
			}}}}

			sql := renderMySQLFamily(c, dialect, diff, foreignKeyCollationDesired())

			c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY")
			c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT fk_child_code")
		})
	}
}
