package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
)

// autoIncrementColumn is a column as a comparison reads one that holds a counter.
func autoIncrementColumn() schemamodel.Field {
	return schemamodel.Field{Name: "id", Type: "int", AutoInc: true}
}

// holdingItsKey is a table holding the column's plain key.
func holdingItsKey(columns ...string) schemacapture.TableObservation {
	return schemacapture.TableObservation{
		Table:   catalog.Table{Name: "t"},
		Indexes: []catalog.Index{{Name: "t_id", TableName: "t", Columns: columns}},
	}
}

// TestAPlainKeyOfAnAutoIncrementColumn_HappyPath is a plan changing a column's counter and the
// plain index that is its key. MySQL and MariaDB answer 1075 to any statement leaving such a
// column without a key, so the two share a statement, and the steps that write an index on its
// own write nothing for this one.
func TestAPlainKeyOfAnAutoIncrementColumn_HappyPath(t *testing.T) {
	key := difftypes.IndexChange{Index: schemamodel.Index{Name: "t_id", Fields: []string{"id"}}, TableName: "t"}
	removal := difftypes.IndexRef{Name: "t_id", TableName: "t"}
	declared := []schemamodel.Table{{Name: "t"}}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "the column goes",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsRemoved: difftypes.ColumnChanges{autoIncrementColumn()}, Current: holdingItsKey("id")}},
				IndexesRemoved: []difftypes.IndexRef{removal},
			},
			want: "ALTER TABLE `t` DROP INDEX `t_id`, DROP COLUMN `id`;",
		},
		{
			name: "the column comes",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsAdded: difftypes.ColumnChanges{autoIncrementColumn()}}},
				IndexesAdded:   difftypes.IndexChanges{key},
				DeclaredTables: declared,
			},
			want: "ALTER TABLE `t` ADD COLUMN `id` int NOT NULL AUTO_INCREMENT, ADD INDEX `t_id` (`id`);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), test.diff)

			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("mysql", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), "DROP INDEX `t_id` ON")
			c.Assert(sql, qt.Not(qt.Contains), "CREATE INDEX")
		})
	}
}

// TestAPlainKeyOfAnotherColumnKeepsItsOwnStatement_FailurePath is a plan whose index is not the
// key of the column that changes, so nothing ties the two.
func TestAPlainKeyOfAnotherColumnKeepsItsOwnStatement_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsRemoved: difftypes.ColumnChanges{autoIncrementColumn()}, Current: holdingItsKey("v", "id")}},
		IndexesRemoved: []difftypes.IndexRef{{Name: "t_id", TableName: "t"}},
	}

	nodes, err := mysql.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DROP INDEX `t_id` ON `t`;")
	c.Assert(sql, qt.Contains, "ALTER TABLE `t` DROP COLUMN `id`;")
}
