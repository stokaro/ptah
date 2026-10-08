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
