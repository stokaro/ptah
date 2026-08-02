package mssql

// White-box testing required: SQL Server catalog normalization and aggregation
// are package-local correctness primitives with no direct exported API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
)

func TestReaderOutputSchema_DefaultSchemaUnscoped(t *testing.T) {
	c := qt.New(t)
	reader := NewSQLServerReader(nil, "")

	c.Assert(reader.outputSchema("dbo"), qt.Equals, "")
	c.Assert(reader.outputSchema("audit"), qt.Equals, "audit")
}

func TestReaderOutputSchema_CustomDefaultSchemaUnscoped(t *testing.T) {
	c := qt.New(t)
	reader := NewSQLServerReader(nil, "audit")

	c.Assert(reader.outputSchema("audit"), qt.Equals, "audit")
}

func TestReaderOutputSchema_PreservesScopedSchema(t *testing.T) {
	c := qt.New(t)
	reader := NewSQLServerReader(nil, "")
	reader.SetSchemas([]string{"dbo"})

	c.Assert(reader.outputSchema("dbo"), qt.Equals, "dbo")
}

func TestNormalizeDefault(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    string
		expected string
	}{
		{name: "numeric", input: "((0))", expected: "0"},
		{name: "string", input: "('pending')", expected: "'pending'"},
		{name: "unicode string", input: "(N'pending')", expected: "'pending'"},
		{name: "escaped unicode string", input: "(N'can''t')", expected: "'can''t'"},
		{name: "function", input: "(getdate())", expected: "getdate()"},
		{name: "nested function", input: "((sysdatetime()))", expected: "sysdatetime()"},
		{name: "not whole expression", input: "(1) + (2)", expected: "(1) + (2)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(normalizeDefault(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestReconcileColumnFlags_PreservesStructuralTableIdentity(t *testing.T) {
	c := qt.New(t)
	schema := &types.DBSchema{
		Tables: []types.DBTable{
			{Name: "tenant.data", Columns: []types.DBColumn{{Name: "id"}}},
			{Schema: "tenant", Name: "data", Columns: []types.DBColumn{{Name: "id"}}},
		},
		Constraints: []types.DBConstraint{
			{TableName: "tenant.data", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
			{Schema: "tenant", TableName: "data", Type: "UNIQUE", ColumnNames: []string{"id"}},
		},
	}

	reconcileColumnFlags(schema)

	c.Assert(schema.Tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(schema.Tables[0].Columns[0].IsUnique, qt.IsFalse)
	c.Assert(schema.Tables[1].Columns[0].IsPrimaryKey, qt.IsFalse)
	c.Assert(schema.Tables[1].Columns[0].IsUnique, qt.IsTrue)
}
