package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// tableLevelPrimaryKeyDatabase declares its primary key as a standalone
// constraint -- the `//ptah:schema:constraint type="PRIMARY KEY"` spelling --
// rather than through the table's own primary_key attribute.
//
// The two spellings reach the AST through different functions, and only the
// table attribute carried the name: the constraint spelling dropped it, so a
// declared `CONSTRAINT orders_pk PRIMARY KEY (...)` reached the server as
// `orders_pkey` with nothing reporting the difference (stokaro/ptah#2590,
// the same defect stokaro/ptah#2180 fixed for the other spelling).
func tableLevelPrimaryKeyDatabase(name string, include ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Order", Name: "id", Type: "INTEGER"},
			{StructName: "Order", Name: "label", Type: "TEXT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName:     "Order",
			Name:           name,
			Type:           "PRIMARY KEY",
			Table:          "orders",
			Columns:        []string{"tenant_id", "id"},
			IncludeColumns: include,
		}},
	}
}

// TestTableLevelPrimaryKey_HappyPath keeps the declared constraint name.
//
// The dialects listed are the ones whose renderer writes a primary key's
// constraint name at all. SQLite, the MySQL family and ClickHouse render
// `PRIMARY KEY (...)` bare whatever the node carries, which is a separate
// renderer-side gap: it is reached the same way by a table's own
// `primary_key_name`, so it predates this conversion fix and is not repaired by
// it.
func TestTableLevelPrimaryKey_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "postgres", dialect: "postgres", want: `CONSTRAINT "orders_pk" PRIMARY KEY ("tenant_id", "id")`},
		{name: "sqlserver", dialect: "sqlserver", want: `CONSTRAINT [orders_pk] PRIMARY KEY ([tenant_id], [id])`},
		{name: "oracle", dialect: "oracle", want: `CONSTRAINT orders_pk PRIMARY KEY (tenant_id, id)`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(
				tableLevelPrimaryKeyDatabase("orders_pk"), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestTableLevelPrimaryKey_IncludeColumnsAreKept covers the other field the
// conversion dropped. PostgreSQL is the dialect that has INCLUDE.
func TestTableLevelPrimaryKey_IncludeColumnsAreKept(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		tableLevelPrimaryKeyDatabase("orders_pk", "label"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains,
		`CONSTRAINT "orders_pk" PRIMARY KEY ("tenant_id", "id") INCLUDE ("label")`)
}

// TestTableLevelPrimaryKey_AnUnnamedKeyStaysUnnamed is the acceptance control:
// a conversion that invented a name would satisfy the tests above while writing
// a name no declaration asked for, and one that always wrote INCLUDE would
// satisfy the test above it.
func TestTableLevelPrimaryKey_AnUnnamedKeyStaysUnnamed(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		tableLevelPrimaryKeyDatabase(""), "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `PRIMARY KEY ("tenant_id", "id")`)
	c.Assert(sql, qt.Not(qt.Contains), "CONSTRAINT")
	c.Assert(sql, qt.Not(qt.Contains), "INCLUDE")
}
