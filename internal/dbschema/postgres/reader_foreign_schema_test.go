package postgres_test

import (
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// foreignSchemaCatalog answers a schema read with two foreign keys on
// public.orders: orders_customer into crm.customers, and orders_line into
// public.lines. Every question it does not model answers no rows.
func foreignSchemaCatalog() dbtest.QueryHandler {
	answers := []dbtest.QueryResult{
		{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}},
		{Columns: []string{"has_table_privilege"}, Rows: [][]driver.Value{{false}}},
		{
			Columns: []string{
				"table_schema", "table_name", "constraint_name", "constraint_type", "columns",
				"foreign_schema", "foreign_table", "foreign_columns", "delete_rule", "update_rule",
				"deferrable", "deferred", "check_clause", "definition", "comment",
			},
			Rows: [][]driver.Value{
				{
					"public", "orders", "orders_customer", "FOREIGN KEY", "customer_id",
					"crm", "customers", "id", "NO ACTION", "NO ACTION", false, false, "", "", "",
				},
				{
					"public", "orders", "orders_line", "FOREIGN KEY", "line_id",
					"public", "lines", "id", "NO ACTION", "NO ACTION", false, false, "", "", "",
				},
			},
		},
		{Columns: []string{"a", "b", "c", "d", "e", "f", "g", "h"}},
	}
	markers := []string{"SELECT EXISTS", "has_table_privilege", "information_schema.table_constraints AS tc"}
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		asked := slices.IndexFunc(markers, func(marker string) bool { return strings.Contains(query, marker) })
		return answers[(asked+len(answers))%len(answers)], nil
	}
}

// foreignKeyTargets lists each foreign key of a read as
// `name -> schema.table`, with the schema left out where the read records none.
func foreignKeyTargets(schema *catalog.Database) []string {
	targets := make([]string, 0, len(schema.Constraints))
	for _, constraint := range schema.Constraints {
		targets = append(targets, constraint.Name+" -> "+constraint.QualifiedForeignTableName())
	}
	return targets
}

// TestReadSchemaContext_KeepsTheSchemaOfAReferencedTable_HappyPath reads a key
// into another schema with that schema's name (stokaro/ptah#3906).
//
// A read of public describes objects that carry no schema, so a key into a
// table of public stays unqualified. A key into crm keeps crm, whether or not
// the read was given a schema list: read as `customers`, it would name a table
// the description does not hold, and every command that renders the read would
// refuse it as unknown.
func TestReadSchemaContext_KeepsTheSchemaOfAReferencedTable_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		schemas []string
	}{
		{name: "a read of the connection's schema"},
		{name: "a read the schema list names", schemas: []string{"public"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, foreignSchemaCatalog())
			reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
			reader.SetSchemas(test.schemas)

			got, err := reader.ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(foreignKeyTargets(got), qt.DeepEquals, []string{"orders_customer -> crm.customers", "orders_line -> lines"})
		})
	}
}
