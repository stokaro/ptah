package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// fkColumnTypeChangeInputs is the shared scenario for issue #694: posts.user_id
// widens from INTEGER to BIGINT while carrying a foreign key to users(id).
func fkColumnTypeChangeInputs() (*types.SchemaDiff, *goschema.Database) {
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName: "posts",
				ColumnsModified: []types.ColumnDiff{
					{ColumnName: "user_id", Changes: map[string]string{"type": "INTEGER -> BIGINT"}},
				},
			},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "BIGINT", StructName: "User", Primary: true},
			{Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: false, Foreign: "users(id)"},
		},
	}
	return diff, generated
}

// TestGenerateSchemaDiffSQL_ForeignKeyColumnTypeChange_MySQLFamilyBrackets checks
// that the MySQL and MariaDB planners drop, MODIFY, then recreate the foreign key
// (issue #694), while the PostgreSQL planner keeps its single ALTER COLUMN ... TYPE
// with no constraint churn.
func TestGenerateSchemaDiffSQL_ForeignKeyColumnTypeChange_MySQLFamilyBrackets(t *testing.T) {
	diff, generated := fkColumnTypeChangeInputs()

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, dialect)
			c.Assert(err, qt.IsNil)
			sql := strings.Join(statements, "\n")

			c.Assert(sql, qt.Contains, "DROP FOREIGN KEY", qt.Commentf("SQL:\n%s", sql))
			c.Assert(sql, qt.Contains, "MODIFY COLUMN", qt.Commentf("SQL:\n%s", sql))
			c.Assert(sql, qt.Contains, "ADD CONSTRAINT", qt.Commentf("SQL:\n%s", sql))

			dropIdx := strings.Index(sql, "DROP FOREIGN KEY")
			modifyIdx := strings.Index(sql, "MODIFY COLUMN")
			addIdx := strings.Index(sql, "ADD CONSTRAINT")
			c.Assert(dropIdx < modifyIdx, qt.IsTrue, qt.Commentf("drop must precede modify:\n%s", sql))
			c.Assert(modifyIdx < addIdx, qt.IsTrue, qt.Commentf("modify must precede re-add:\n%s", sql))
		})
	}
}

func TestGenerateSchemaDiffSQL_ForeignKeyColumnTypeChange_PostgresUnchanged(t *testing.T) {
	c := qt.New(t)

	diff, generated := fkColumnTypeChangeInputs()

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, platform.Postgres)
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")

	// PostgreSQL applies the type change in place; it never drops or recreates
	// the foreign key.
	c.Assert(sql, qt.Contains, "ALTER COLUMN", qt.Commentf("SQL:\n%s", sql))
	c.Assert(sql, qt.Contains, "TYPE BIGINT", qt.Commentf("SQL:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT", qt.Commentf("SQL:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP FOREIGN KEY", qt.Commentf("SQL:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT", qt.Commentf("SQL:\n%s", sql))
}
