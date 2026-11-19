package mysql_test

import (
	"cmp"
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/readscope"
)

// keyColumnUsage is one row of information_schema.KEY_COLUMN_USAGE. A primary
// or unique key's row references no table.
type keyColumnUsage struct {
	constraint, table, column         string
	referencedSchema, referencedTable any
	referencedColumn                  any
	ordinal                           int
}

// hiddenKeysCatalog answers as a MariaDB server whose database fx holds the
// keys in usage, of which visible are the foreign key rows TABLE_CONSTRAINTS
// shows the account. It applies the clauses of a KEY_COLUMN_USAGE query the
// way the server does, and only when the query has them: the referenced-table
// filter and the ordering. usage is kept out of order, so a read that relied
// on the server's natural order would see it.
type hiddenKeysCatalog struct {
	visibleCatalog
	usage   []keyColumnUsage
	visible [][]driver.Value
}

func (h hiddenKeysCatalog) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "FROM information_schema.TABLE_CONSTRAINTS tc"):
		return dbtest.QueryResult{Columns: []string{
			"CONSTRAINT_NAME", "TABLE_NAME", "CONSTRAINT_TYPE", "COLUMN_NAME", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME",
			"REFERENCED_COLUMN_NAME", "DELETE_RULE", "UPDATE_RULE", "MATCH_OPTION", "ENFORCED",
		}, Rows: h.visible}, nil
	case strings.Contains(query, "FROM information_schema.KEY_COLUMN_USAGE") && strings.Contains(query, "REFERENCED_COLUMN_NAME"):
		usage := slices.Clone(h.usage)
		if strings.Contains(query, "REFERENCED_TABLE_NAME IS NOT NULL") {
			usage = slices.DeleteFunc(usage, func(row keyColumnUsage) bool { return row.referencedTable == nil })
		}
		if strings.Contains(query, "ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION") {
			slices.SortFunc(usage, func(a, b keyColumnUsage) int {
				return cmp.Or(strings.Compare(a.table, b.table), strings.Compare(a.constraint, b.constraint), cmp.Compare(a.ordinal, b.ordinal))
			})
		}
		rows := make([][]driver.Value, 0, len(usage))
		for _, row := range usage {
			rows = append(rows, []driver.Value{
				row.constraint, row.table, row.column, coalesce(row.referencedSchema), row.referencedTable, coalesce(row.referencedColumn),
			})
		}
		return dbtest.QueryResult{Columns: []string{
			"CONSTRAINT_NAME", "TABLE_NAME", "COLUMN_NAME", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
		}, Rows: rows}, nil
	}
	return h.visibleCatalog.answer(query, args)
}

// coalesce is COALESCE(value, ”).
func coalesce(value any) any {
	return cmp.Or(value, any(""))
}

// foreignKeys describes each foreign key of database, in the order the read
// returned them, as table.name (columns) -> referenced (columns), with the
// delete rule when the read has one.
func foreignKeys(database *catalog.Database) []string {
	var described []string
	for _, constraint := range database.Constraints {
		if constraint.Type != "FOREIGN KEY" {
			continue
		}
		line := fmt.Sprintf("%s.%s (%s) -> %s (%s)", constraint.TableName, constraint.Name,
			strings.Join(constraint.ColumnNames, ", "), *constraint.ForeignTable, strings.Join(constraint.ForeignColumns, ", "))
		if constraint.DeleteRule != nil {
			line += " ON DELETE " + *constraint.DeleteRule
		}
		described = append(described, line)
	}
	return described
}

// TestReader_ReadsForeignKeysTableConstraintsHides_HappyPath reads, for a
// SELECT-only MariaDB account in a run that leaves constraints out, the
// foreign keys KEY_COLUMN_USAGE lists and TABLE_CONSTRAINTS hid. The primary
// and unique keys there are not foreign keys, the composite key keeps the
// order of its columns, and the key TABLE_CONSTRAINTS showed is read once,
// with the referential action only that read has.
func TestReader_ReadsForeignKeysTableConstraintsHides_HappyPath(t *testing.T) {
	c := qt.New(t)
	fake := hiddenKeysCatalog{
		visibleCatalog: visibleCatalog{selected: "fx", tables: []string{"child", "orders", "parent"}},
		usage: []keyColumnUsage{
			{constraint: "child_parent_fk", table: "child", column: "parent_b", referencedSchema: "fx", referencedTable: "parent", referencedColumn: "b", ordinal: 2},
			{constraint: "PRIMARY", table: "parent", column: "id", ordinal: 1},
			{constraint: "orders_parent_fk", table: "orders", column: "parent_id", referencedSchema: "fx", referencedTable: "parent", referencedColumn: "id", ordinal: 1},
			{constraint: "parent_code", table: "parent", column: "code", ordinal: 1},
			{constraint: "child_parent_fk", table: "child", column: "parent_a", referencedSchema: "fx", referencedTable: "parent", referencedColumn: "a", ordinal: 1},
		},
		visible: [][]driver.Value{
			{"orders_parent_fk", "orders", "FOREIGN KEY", "parent_id", "fx", "parent", "id", "CASCADE", "NO ACTION", "NONE", "YES"},
		},
	}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion, []string{"GRANT SELECT, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`"}, fake.answer))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Without(t.Context(), readscope.Constraints))

	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeys(got), qt.DeepEquals, []string{
		"orders.orders_parent_fk (parent_id) -> parent (id) ON DELETE CASCADE",
		"child.child_parent_fk (parent_a, parent_b) -> parent (a, b)",
	})
}

// TestReader_ReadsHiddenForeignKeysOnlyWhereTheyAreHidden holds the read to
// its two conditions with the same catalog: a run that needs constraints, and
// a MySQL server, which shows TABLE_CONSTRAINTS to the same account, read
// TABLE_CONSTRAINTS alone.
func TestReader_ReadsHiddenForeignKeysOnlyWhereTheyAreHidden(t *testing.T) {
	fake := hiddenKeysCatalog{
		visibleCatalog: visibleCatalog{selected: "fx", tables: []string{"child", "parent"}},
		usage: []keyColumnUsage{
			{constraint: "child_parent_fk", table: "child", column: "parent_id", referencedSchema: "fx", referencedTable: "parent", referencedColumn: "id", ordinal: 1},
		},
	}
	t.Run("MySQL, a run without constraints", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion,
			[]string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`", "GRANT SHOW_ROUTINE ON *.* TO `app`@`%`"}, fake.answer))

		got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Without(t.Context(), readscope.Constraints))

		c.Assert(err, qt.IsNil)
		c.Assert(foreignKeys(got), qt.HasLen, 0)
	})
	t.Run("MariaDB, a run that needs constraints and an account that sees them", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion,
			[]string{"GRANT SELECT, SHOW VIEW, TRIGGER, SHOW CREATE ROUTINE ON `fx`.* TO `app`@`%`"}, fake.answer))

		got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(foreignKeys(got), qt.HasLen, 0)
	})
}
