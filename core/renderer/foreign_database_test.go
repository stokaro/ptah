package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// A MySQL or MariaDB foreign key may reference a table in another database,
// and a description of one database cannot hold that table. The renderer
// writes such a key as declared and leaves the referenced table to the server,
// where it refuses a reference it cannot resolve (stokaro/ptah#3891).

// ordersIntoAnotherDatabase describes the database shop: orders, whose key
// references target. declaredOn says where the key is declared: "constraint"
// for a table-level constraint, "field" for the column's foreign attribute.
// extra is added to the description's tables.
func ordersIntoAnotherDatabase(target, declaredOn string, extra ...schemamodel.Table) *schemamodel.Database {
	fields := map[string][]schemamodel.Field{
		"constraint": {
			{StructName: "Orders", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Orders", Name: "customer_id", Type: "BIGINT"},
		},
		"field": {
			{StructName: "Orders", Name: "id", Type: "BIGINT", Primary: true},
			{
				StructName: "Orders", Name: "customer_id", Type: "BIGINT",
				Foreign: target + "(id)", ForeignKeyName: "orders_customer",
			},
		},
	}
	constraints := map[string][]schemamodel.Constraint{
		"constraint": {{
			StructName: "Orders", Table: "orders", Name: "orders_customer", Type: "FOREIGN KEY",
			Columns: []string{"customer_id"}, ForeignTable: target, ForeignColumns: []string{"id"},
		}},
	}
	return &schemamodel.Database{
		Tables:      append([]schemamodel.Table{{StructName: "Orders", Name: "orders"}}, extra...),
		Fields:      fields[declaredOn],
		Constraints: constraints[declaredOn],
	}
}

// inMySQLDatabase names database as the schema of every table of described,
// as a description written as Atlas HCL names it, and returns described.
func inMySQLDatabase(described *schemamodel.Database, database string) *schemamodel.Database {
	for i := range described.Tables {
		described.Tables[i].Schema = database
	}
	for i := range described.Constraints {
		described.Constraints[i].Table = database + "." + described.Constraints[i].Table
	}
	return described
}

// describingDatabase adds database to the databases described declares, with
// no table in it, and returns described.
func describingDatabase(described *schemamodel.Database, database string) *schemamodel.Database {
	described.Schemas = append(described.Schemas, schemamodel.Schema{Name: database})
	return described
}

// TestGetOrderedCreateStatements_KeyIntoAnotherMySQLDatabase_HappyPath renders
// a key into a table of `crm` from a description of another database, however
// the key is declared. The table holding it is still made InnoDB: the server
// refuses a foreign key on any other engine.
func TestGetOrderedCreateStatements_KeyIntoAnotherMySQLDatabase_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		declaredOn string
	}{
		{name: "mysql, a table constraint", dialect: platform.MySQL, declaredOn: "constraint"},
		{name: "mysql, a field", dialect: platform.MySQL, declaredOn: "field"},
		{name: "mariadb, a table constraint", dialect: platform.MariaDB, declaredOn: "constraint"},
		{name: "mariadb, a field", dialect: platform.MariaDB, declaredOn: "field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(
				ordersIntoAnotherDatabase("crm.customers", test.declaredOn), test.dialect)

			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, "CONSTRAINT `orders_customer` FOREIGN KEY (`customer_id`) REFERENCES `crm`.`customers`(`id`)")
			c.Assert(rendered, qt.Contains, ") ENGINE=InnoDB;")
		})
	}
}

// TestGetOrderedCreateStatements_KeyIntoAnotherMySQLDatabase_FailurePath keeps
// the refusal for a reference the description should have held.
//
// An unqualified name means a table of the described database, also where
// every table the description holds names that database. A qualified one
// naming a database the description holds a table of means a table of that
// database, and it is not there either. PostgreSQL is the control that the
// acceptance belongs to the family where a database is the schema: nothing
// here decides what a reference into an undescribed PostgreSQL schema means.
func TestGetOrderedCreateStatements_KeyIntoAnotherMySQLDatabase_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
		want     string
	}{
		{
			name:     "mysql, an unqualified table the description does not hold",
			dialect:  platform.MySQL,
			database: ordersIntoAnotherDatabase("customers", "constraint"),
			want:     `constraint "orders_customer" references unknown table "customers"`,
		},
		{
			name:     "mysql, an unqualified table beside tables that name their database",
			dialect:  platform.MySQL,
			database: inMySQLDatabase(ordersIntoAnotherDatabase("customers", "constraint"), "shop"),
			want:     `constraint "orders_customer" references unknown table "customers"`,
		},
		{
			name:    "mysql, a missing table of a described database",
			dialect: platform.MySQL,
			database: ordersIntoAnotherDatabase("crm.customers", "constraint",
				schemamodel.Table{StructName: "Accounts", Name: "accounts", Schema: "crm"}),
			want: `constraint "orders_customer" references unknown table "crm.customers"`,
		},
		{
			name:    "mariadb, a missing table of a described database, declared on the field",
			dialect: platform.MariaDB,
			database: ordersIntoAnotherDatabase("crm.customers", "field",
				schemamodel.Table{StructName: "Accounts", Name: "accounts", Schema: "crm"}),
			want: `field "customer_id" references unknown table "crm.customers"`,
		},
		{
			name:     "mysql, a table of a described database that holds none",
			dialect:  platform.MySQL,
			database: describingDatabase(ordersIntoAnotherDatabase("crm.customers", "constraint"), "crm"),
			want:     `constraint "orders_customer" references unknown table "crm.customers"`,
		},
		{
			name:     "postgres, a table of an undescribed schema",
			dialect:  platform.Postgres,
			database: ordersIntoAnotherDatabase("crm.customers", "constraint"),
			want:     `constraint "orders_customer" references unknown table "crm.customers"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(test.database, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*invalid foreign key: `+test.want+`.*`)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
