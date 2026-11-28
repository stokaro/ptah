package foreignkeyscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/foreignkeyscope"
)

// described holds orders, unqualified, and accounts in the schema audit, and
// declares the schema ledger, which holds no table.
func described() schemamodel.Database {
	return schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "ledger"}},
		Tables: []schemamodel.Table{
			{StructName: "Orders", Name: "orders"},
			{StructName: "Accounts", Name: "accounts", Schema: "audit"},
		},
	}
}

// TestOutside_HappyPath names the schema of a reference into a schema the
// description holds nothing of, on each dialect that accepts such a key.
func TestOutside_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		reference string
		schema    string
	}{
		{name: "mysql", dialect: "mysql", reference: "crm.customers", schema: "crm"},
		{name: "mariadb", dialect: "mariadb", reference: "crm.customers", schema: "crm"},
		{name: "postgres", dialect: "postgres", reference: "crm.customers", schema: "crm"},
		{name: "cockroachdb", dialect: "cockroachdb", reference: "crm.customers", schema: "crm"},
		{name: "oracle", dialect: "oracle", reference: "crm.customers", schema: "crm"},
		{name: "mysql, public is a database like any other", dialect: "mysql", reference: "public.customers", schema: "public"},
		{name: "mysql compares a database name exactly", dialect: "mysql", reference: "AUDIT.customers", schema: "AUDIT"},
		{name: "oracle, a quoted lower-case account", dialect: "oracle", reference: `"audit".customers`, schema: "audit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema, outside := foreignkeyscope.Outside(described(), test.dialect, test.reference)

			c.Assert(outside, qt.IsTrue)
			c.Assert(schema, qt.Equals, test.schema)
		})
	}
}

// TestOutside_FailurePath keeps a reference inside the description: an
// unqualified one, one into a schema the description holds a table of, one
// into public on PostgreSQL beside an unqualified table, and any reference on
// a dialect that does not accept a key out of its description.
func TestOutside_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		reference string
	}{
		{name: "unqualified", dialect: "postgres", reference: "customers"},
		{name: "a described schema", dialect: "mysql", reference: "audit.customers"},
		{name: "oracle, a described schema in upper case", dialect: "oracle", reference: "AUDIT.customers"},
		{name: "oracle, a described schema quoted", dialect: "oracle", reference: `"AUDIT".customers`},
		{name: "oracle, a declared schema in upper case", dialect: "oracle", reference: "LEDGER.entries"},
		{name: "public beside an unqualified table", dialect: "postgres", reference: "public.customers"},
		{name: "public beside an unqualified table on cockroachdb", dialect: "cockroachdb", reference: "public.customers"},
		{name: "yugabytedb", dialect: "yugabytedb", reference: "crm.customers"},
		{name: "sqlserver", dialect: "sqlserver", reference: "crm.customers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema, outside := foreignkeyscope.Outside(described(), test.dialect, test.reference)

			c.Assert(outside, qt.IsFalse)
			c.Assert(schema, qt.Equals, "")
		})
	}
}

// TestReferences_HappyPath lists a description's keys, the column-declared ones
// before the table constraints, and leaves out every other constraint.
func TestReferences_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := schemamodel.Database{
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id"},
			{StructName: "Orders", Name: "customer_id", Foreign: "crm.customers(id)"},
		},
		Constraints: []schemamodel.Constraint{
			{Name: "orders_total", Type: "CHECK"},
			{Name: "orders_line", Type: "FOREIGN KEY", ForeignTable: "lines"},
		},
	}

	c.Assert(foreignkeyscope.References(database), qt.DeepEquals, []foreignkeyscope.Reference{
		{Declaration: `field "customer_id"`, Table: "crm.customers"},
		{Declaration: `constraint "orders_line"`, Table: "lines"},
	})
}

// TestIntoSchema_HappyPath asks whether a key names a table of the Oracle
// account APP, read as the connection's schema, and whether the description
// declares that table. Every spelling Oracle takes for APP names it, and a
// quoted lower-case "app" is another account.
func TestIntoSchema_HappyPath(t *testing.T) {
	database := schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "Orders", Name: "orders"},
		{StructName: "Ledger", Name: "ledger", Schema: "APP"},
	}}
	semantics := identifier.ForDialect("oracle")
	own := foreignkeyscope.StoredSchemaKey(semantics, "oracle", "APP")
	tests := []struct {
		name      string
		reference string
		schema    string
		into      bool
		declared  bool
	}{
		{name: "lower case, declared unqualified", reference: "app.orders", schema: own, into: true, declared: true},
		{name: "upper case, declared unqualified", reference: "APP.ORDERS", schema: own, into: true, declared: true},
		{name: "quoted, declared unqualified", reference: `"APP".orders`, schema: own, into: true, declared: true},
		{name: "declared in the schema", reference: "app.ledger", schema: own, into: true, declared: true},
		{name: "not declared", reference: "APP.customers", schema: own, into: true, declared: false},
		{name: "a quoted lower-case account", reference: `"app".orders`, schema: own},
		{name: "a quoted table the fold keeps apart", reference: `APP."orders"`, schema: own, into: true, declared: false},
		{name: "another account", reference: "crm.orders", schema: own},
		{name: "unqualified", reference: "orders", schema: own},
		{name: "no connection schema", reference: "app.orders", schema: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			into, declared := foreignkeyscope.IntoSchema(database, semantics, test.reference, test.schema)

			c.Assert(into, qt.Equals, test.into)
			c.Assert(declared, qt.Equals, test.declared)
		})
	}
}

// TestReferencedSchemaKey_HappyPath keys the schema a reference names as the
// stored account of that spelling keys: on Oracle crm, CRM and "CRM" are one
// account, "crm" another, and on PostgreSQL a schema compares exactly.
func TestReferencedSchemaKey_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		reference string
		stored    string
		same      bool
	}{
		{name: "oracle, lower case", dialect: "oracle", reference: "crm.t", stored: "CRM", same: true},
		{name: "oracle, upper case", dialect: "oracle", reference: "CRM.t", stored: "CRM", same: true},
		{name: "oracle, quoted", dialect: "oracle", reference: `"CRM".t`, stored: "CRM", same: true},
		{name: "oracle, quoted lower case", dialect: "oracle", reference: `"crm".t`, stored: "CRM", same: false},
		{name: "oracle, a stored lower-case account", dialect: "oracle", reference: `"crm".t`, stored: "crm", same: true},
		{name: "postgres", dialect: "postgres", reference: "crm.t", stored: "crm", same: true},
		{name: "postgres compares exactly", dialect: "postgres", reference: "CRM.t", stored: "crm", same: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(test.dialect)

			got := foreignkeyscope.ReferencedSchemaKey(semantics, test.reference)

			c.Assert(got == foreignkeyscope.StoredSchemaKey(semantics, test.dialect, test.stored), qt.Equals, test.same)
		})
	}
}

// TestReferencedSchemaKey_FailurePath answers no schema for a reference that
// names none.
func TestReferencedSchemaKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	got := foreignkeyscope.ReferencedSchemaKey(identifier.ForDialect("oracle"), "orders")

	c.Assert(got, qt.Equals, "")
}

// TestIntoTableOf_NamesATableOutOfScope finds the table a reference names
// among the tables a read holds out of scope, with an unqualified name in the
// connection's schema.
func TestIntoTableOf_NamesATableOutOfScope(t *testing.T) {
	tests := []struct {
		name, dialect, reference, current string
		tables                            []string
	}{
		{name: "unqualified, in the connection's schema", dialect: "mysql", reference: "t1", current: "app", tables: []string{"app.t1"}},
		{name: "qualified", dialect: "mysql", reference: "app.t1", current: "app", tables: []string{"app.t1"}},
		{name: "qualified, read with no schema", dialect: "mysql", reference: "app.t1", current: "app", tables: []string{"t1"}},
		{name: "another schema the read holds", dialect: "postgres", reference: "crm.customers", current: "public", tables: []string{"crm.customers"}},
		{name: "oracle folds an unquoted name", dialect: "oracle", reference: "app.t1", current: "APP", tables: []string{"APP.T1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(foreignkeyscope.IntoTableOf(test.dialect, test.reference, test.current, test.tables), qt.IsTrue)
		})
	}
}

// TestIntoTableOf_NamesNoTableOutOfScope leaves a reference to any other table
// to the refusals, including an unqualified one on a connection that names no
// schema, where the read cannot say which schema it is in.
func TestIntoTableOf_NamesNoTableOutOfScope(t *testing.T) {
	tests := []struct {
		name, dialect, reference, current string
		tables                            []string
	}{
		{name: "nothing out of scope", dialect: "mysql", reference: "t1", current: "app"},
		{name: "another table", dialect: "mysql", reference: "t2", current: "app", tables: []string{"app.t1"}},
		{name: "another schema", dialect: "mysql", reference: "crm.t1", current: "app", tables: []string{"app.t1"}},
		{name: "unqualified on a whole server", dialect: "mysql", reference: "t1", tables: []string{"app.t1"}},
		{name: "unqualified, where neither side names a schema", dialect: "mysql", reference: "t1", tables: []string{"t1"}},
		{name: "mysql compares a table name exactly", dialect: "mysql", reference: "T1", current: "app", tables: []string{"app.t1"}},
		{name: "oracle keeps a quoted name apart", dialect: "oracle", reference: `app."t1"`, current: "APP", tables: []string{"APP.T1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(foreignkeyscope.IntoTableOf(test.dialect, test.reference, test.current, test.tables), qt.IsFalse)
		})
	}
}
