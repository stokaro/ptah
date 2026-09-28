package mysql_test

import (
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// foreignKeyCatalog answers a reader as a server whose database shop holds
// orders, with one key into a table of shop and one into a table of crm. The
// session selects the database selected names, or none when it is nil. Every
// question it does not model answers no rows.
func foreignKeyCatalog(selected any) dbtest.QueryHandler {
	answers := []dbtest.QueryResult{
		{Columns: []string{"DATABASE()"}, Rows: [][]driver.Value{{selected}}},
		{
			Columns: []string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"},
			Rows:    [][]driver.Value{{"shop", "utf8mb4", "utf8mb4_0900_ai_ci"}},
		},
		{
			Columns: []string{
				"CONSTRAINT_NAME", "TABLE_NAME", "CONSTRAINT_TYPE", "COLUMN_NAME",
				"REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
				"DELETE_RULE", "UPDATE_RULE",
			},
			Rows: [][]driver.Value{
				{"orders_customer", "orders", "FOREIGN KEY", "customer_id", "crm", "customers", "id", "NO ACTION", "NO ACTION"},
				{"orders_line", "orders", "FOREIGN KEY", "line_id", "shop", "lines", "id", "NO ACTION", "NO ACTION"},
			},
		},
		{},
	}
	markers := []string{"SELECT DATABASE()", "FROM information_schema.SCHEMATA", "FROM information_schema.TABLE_CONSTRAINTS tc"}
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		asked := slices.IndexFunc(markers, func(marker string) bool { return strings.Contains(query, marker) })
		return answers[(asked+len(answers))%len(answers)], nil
	}
}

// foreignTargets lists each foreign key of a read as `name -> schema.table`,
// with the schema left out where the read records none.
func foreignTargets(schema *catalog.Database) []string {
	targets := make([]string, 0, len(schema.Constraints))
	for _, constraint := range schema.Constraints {
		targets = append(targets, constraint.Name+" -> "+constraint.QualifiedForeignTableName())
	}
	return targets
}

// TestReader_KeepsTheDatabaseOfAReferencedTable_HappyPath reads a key into
// another database with that database's name (stokaro/ptah#3891).
//
// A read of one database describes objects that carry no schema, so a key into
// a table of the same database stays unqualified. A key into `crm` keeps
// `crm`: read as `customers`, it would name a table the description does not
// hold, and every command that renders the read would refuse it as unknown. A
// read of the whole server records the database of every referenced table the
// same way.
func TestReader_KeepsTheDatabaseOfAReferencedTable_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		selected  any
		databases []string
		want      []string
	}{
		{
			name:     "a session that selected the database",
			selected: "shop",
			want:     []string{"orders_customer -> crm.customers", "orders_line -> lines"},
		},
		{
			name:      "a whole-server session that lists the database",
			databases: []string{"shop"},
			want:      []string{"orders_customer -> crm.customers", "orders_line -> shop.lines"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, foreignKeyCatalog(test.selected))
			reader := mysql.NewMySQLReader(db.SQL, "")
			reader.SetSchemas(test.databases)

			got, err := reader.ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(foreignTargets(got), qt.DeepEquals, test.want)
		})
	}
}
