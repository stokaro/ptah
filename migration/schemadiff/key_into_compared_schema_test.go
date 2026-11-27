package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A foreign key into a schema the desired schema holds nothing of is written
// as declared, because a description of one schema cannot hold a table of
// another. A comparison against a database that holds that schema is the
// exception: the desired schema saying nothing of it asks for the referenced
// table to be dropped, so the key is refused (stokaro/ptah#3906).

// ordersIntoCRM is a desired schema holding orders, whose key orders_customer
// references crm.customers.
func ordersIntoCRM() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Orders", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Orders", Name: "customer_id", Type: "BIGINT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Orders", Table: "orders", Name: "orders_customer", Type: "FOREIGN KEY",
			Columns: []string{"customer_id"}, ForeignTable: "crm.customers", ForeignColumns: []string{"id"},
		}},
	}
}

// liveTables is a read holding tables, a table with no schema being one of the
// connection's own schema.
func liveTables(tables ...catalog.Table) *catalog.Database {
	return &catalog.Database{Tables: tables}
}

// TestCompareWithDatabaseInfo_KeyIntoAnUncomparedSchema_HappyPath compares the
// key against reads that do not cover crm: a read of public alone, and a read
// of one MySQL database. The key is planned as declared.
func TestCompareWithDatabaseInfo_KeyIntoAnUncomparedSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *catalog.Database
	}{
		{name: "postgres, a read of public", dialect: "postgres", database: liveTables(catalog.Table{Name: "accounts"})},
		{name: "cockroachdb, an empty read", dialect: "cockroachdb", database: liveTables()},
		{name: "mysql, a read of one database", dialect: "mysql", database: liveTables(catalog.Table{Name: "accounts"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), ordersIntoCRM(), test.database, catalog.ServerInfo{Dialect: test.dialect, Schema: "public"}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"orders"})
		})
	}
}

// TestCompareWithDatabaseInfo_KeyIntoAComparedSchema_FailurePath refuses the
// key where the read covers crm: a read of every schema of a PostgreSQL
// database, and a read of a whole MySQL server. The desired schema declares
// nothing of crm, so the plan would drop the table the key references.
func TestCompareWithDatabaseInfo_KeyIntoAComparedSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *catalog.Database
	}{
		{
			name:     "postgres, a read of every schema",
			dialect:  "postgres",
			database: liveTables(catalog.Table{Schema: "crm", Name: "customers"}),
		},
		{
			name:     "cockroachdb, a read holding another table of crm",
			dialect:  "cockroachdb",
			database: liveTables(catalog.Table{Schema: "crm", Name: "accounts"}),
		},
		{
			name:     "mariadb, a read of the whole server",
			dialect:  "mariadb",
			database: liveTables(catalog.Table{Schema: "crm", Name: "customers"}, catalog.Table{Schema: "shop", Name: "x"}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), ordersIntoCRM(), test.database, catalog.ServerInfo{Dialect: test.dialect, Schema: "public"}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: constraint "orders_customer" references unknown table `+
				`"crm.customers": the database compared holds schema "crm" and the desired schema declares nothing of it`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// ordersInto is a desired schema holding orders, whose key orders_customer
// references reference.
func ordersInto(reference string) *schemamodel.Database {
	database := ordersIntoCRM()
	database.Constraints[0].ForeignTable = reference
	return database
}

// ordersBesideCustomers is [ordersInto] with the table customers declared too.
func ordersBesideCustomers(reference string) *schemamodel.Database {
	database := ordersInto(reference)
	database.Tables = append(database.Tables, schemamodel.Table{StructName: "Customers", Name: "customers"})
	database.Fields = append(database.Fields, schemamodel.Field{StructName: "Customers", Name: "id", Type: "BIGINT", Primary: true})
	return database
}

// ownAccount is a read of the Oracle account APP holding customers.
func ownAccount() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "APP"}},
		Tables: []catalog.Table{{Name: "CUSTOMERS", Columns: []catalog.Column{{
			Name: "ID", DataType: "NUMBER", ColumnType: "NUMBER(19)", IsNullable: "NO", IsPrimaryKey: true,
		}}}},
	}
}

// TestCompareWithDatabaseInfo_KeyIntoTheConnectionsSchema_HappyPath compares a
// key into the schema the connection is read in with a desired schema that
// declares the table, under each spelling of the schema the dialect takes for
// it: the key names the declared table, so the comparison plans orders and
// drops nothing.
func TestCompareWithDatabaseInfo_KeyIntoTheConnectionsSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		schema    string
		reference string
		database  *catalog.Database
	}{
		{name: "oracle, lower case", dialect: "oracle", schema: "APP", reference: "app.customers", database: ownAccount()},
		{name: "oracle, upper case", dialect: "oracle", schema: "APP", reference: "APP.customers", database: ownAccount()},
		{name: "oracle, quoted", dialect: "oracle", schema: "APP", reference: `"APP".customers`, database: ownAccount()},
		{
			name: "mysql, the database read", dialect: "mysql", schema: "shop", reference: "shop.customers",
			database: liveTables(catalog.Table{Name: "customers"}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), ordersBesideCustomers(test.reference), test.database,
				catalog.ServerInfo{Dialect: test.dialect, Schema: test.schema}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"orders"})
			c.Assert(diff.TablesRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_KeyIntoTheConnectionsSchema_FailurePath refuses a
// key into the schema the connection is read in when the desired schema does
// not declare the table, however the schema is spelled: the plan would drop
// customers while adding a key that reads it. Measured on Oracle 23.26, the
// plan dropped the table and the server refused the drop with ORA-02449.
func TestCompareWithDatabaseInfo_KeyIntoTheConnectionsSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		schema    string
		reference string
		database  *catalog.Database
	}{
		{name: "oracle, lower case", dialect: "oracle", schema: "APP", reference: "app.customers", database: ownAccount()},
		{name: "oracle, upper case", dialect: "oracle", schema: "APP", reference: "APP.customers", database: ownAccount()},
		{name: "oracle, quoted", dialect: "oracle", schema: "APP", reference: `"APP".customers`, database: ownAccount()},
		{
			name: "mysql, the database read", dialect: "mysql", schema: "shop", reference: "shop.customers",
			database: liveTables(catalog.Table{Name: "customers"}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), ordersInto(test.reference), test.database,
				catalog.ServerInfo{Dialect: test.dialect, Schema: test.schema}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: constraint "orders_customer" references unknown table `+
				`".*customers": "`+test.schema+`" is the schema of the database compared, and the desired schema declares `+
				`no such table there`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_KeyIntoAComparedSchemaSpelledAnotherWay_FailurePath
// refuses a key into crm on Oracle, read as the account APP that also sees
// CRM's tables, whichever case the key spells crm in: Oracle stores the
// account CRM, and an unquoted crm names it.
func TestCompareWithDatabaseInfo_KeyIntoAComparedSchemaSpelledAnotherWay_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		reference string
	}{
		{name: "lower case", reference: "crm.customers"},
		{name: "upper case", reference: "CRM.customers"},
		{name: "quoted", reference: `"CRM".customers`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := ownAccount()
			database.Tables = append(database.Tables, catalog.Table{Schema: "CRM", Name: "CUSTOMERS"})

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), ordersInto(test.reference), database,
				catalog.ServerInfo{Dialect: "oracle", Schema: "APP"}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: constraint "orders_customer" references unknown table `+
				`".*customers": the database compared holds schema "(?i:crm)" and the desired schema declares nothing of it`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_KeyIntoAnotherAccount_HappyPath is the control:
// on Oracle a quoted lower-case "crm" is an account of its own, which the read
// does not hold, so the key is planned as declared.
func TestCompareWithDatabaseInfo_KeyIntoAnotherAccount_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := ownAccount()
	database.Tables = append(database.Tables, catalog.Table{Schema: "CRM", Name: "CUSTOMERS"})

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), ordersInto(`"crm".customers`), database,
		catalog.ServerInfo{Dialect: "oracle", Schema: "APP"}, nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"orders"})
}
