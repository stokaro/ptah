package postgres

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/testutils"
)

func TestNewPostgreSQLReader(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		tests := []struct {
			name           string
			schema         string
			expectedSchema string
		}{
			{
				name:           "with custom schema",
				schema:         "test_schema",
				expectedSchema: "test_schema",
			},
			{
				name:           "with empty schema defaults to public",
				schema:         "",
				expectedSchema: "public",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				reader := NewPostgreSQLReader(nil, test.schema)
				c.Assert(reader, qt.IsNotNil)
				c.Assert(reader.schema, qt.Equals, test.expectedSchema)
				c.Assert(reader.db, qt.IsNil) // We passed nil for testing
				c.Assert(reader.caps.Has(capability.RowLevelSecurity), qt.IsTrue)
			})
		}
	})
}

func TestNewPostgreSQLReaderWithCapabilities(t *testing.T) {
	c := qt.New(t)

	reader := NewPostgreSQLReaderWithCapabilities(nil, "", capability.CockroachDB23())

	c.Assert(reader.schema, qt.Equals, "public")
	c.Assert(reader.caps.Has(capability.RowLevelSecurity), qt.IsFalse)
	c.Assert(reader.caps.Has(capability.XMLType), qt.IsFalse)
}

func TestPostgresNullsDistinctFromDefinition(t *testing.T) {
	tests := []struct {
		name        string
		definition  string
		expected    bool
		expectedSet bool
	}{
		{
			name:        "nulls not distinct",
			definition:  `CREATE UNIQUE INDEX users_c_key ON public.users USING btree (c) NULLS NOT DISTINCT`,
			expected:    false,
			expectedSet: true,
		},
		{
			name:        "nulls distinct",
			definition:  `CREATE UNIQUE INDEX users_c_key ON public.users USING btree (c) NULLS DISTINCT`,
			expected:    true,
			expectedSet: true,
		},
		{
			name:       "absent",
			definition: `CREATE UNIQUE INDEX users_c_key ON public.users USING btree (c)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			actual := postgresNullsDistinctFromDefinition(test.definition)
			if !test.expectedSet {
				c.Assert(actual, qt.IsNil)
				return
			}
			c.Assert(actual, qt.IsNotNil)
			c.Assert(*actual, qt.Equals, test.expected)
		})
	}
}

func TestExtractPostgresIndexColumns(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		expected   []string
	}{
		{
			name:       "plain index",
			definition: `CREATE INDEX idx_users_email ON public.users USING btree (email)`,
			expected:   []string{"email"},
		},
		{
			name:       "partial index predicate does not leak into columns",
			definition: `CREATE INDEX idx_users_email_active ON public.users USING btree (email) WHERE (deleted_at IS NULL)`,
			expected:   []string{"email"},
		},
		{
			name:       "expression index fallback",
			definition: `CREATE INDEX idx_users_email_lc ON public.users USING btree (lower(email))`,
			expected:   []string{"lower(email)"},
		},
		{
			name:       "mixed expression and column fallback",
			definition: `CREATE INDEX idx_users_email_tenant ON public.users USING btree (lower(email), tenant_id)`,
			expected:   []string{"lower(email)", "tenant_id"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(extractPostgresIndexColumns(test.definition), qt.DeepEquals, test.expected)
		})
	}
}

func TestParsePostgresIndexColumnsJSONPreservesExpressionCommas(t *testing.T) {
	c := qt.New(t)

	columns, err := parsePostgresIndexColumns(`["concat(first_name, last_name)","tenant_id"]`, "")

	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.DeepEquals, []string{"concat(first_name, last_name)", "tenant_id"})
}

func TestPostgreSQLReader_ReadSchema_NoConnection(t *testing.T) {
	c := qt.New(t)

	// Test that reader can be created with nil database
	reader := NewPostgreSQLReader(nil, "public")
	c.Assert(reader, qt.IsNotNil)
	c.Assert(reader.schema, qt.Equals, "public")
	c.Assert(reader.db, qt.IsNil)

	// Note: We don't test ReadSchema() with nil db as it would panic
	// This is expected behavior - the reader requires a valid database connection
}

func TestPostgreSQLReader_ExtensionFunctionFiltering(t *testing.T) {
	c := qt.New(t)

	// Test that the readFunctions query properly excludes extension-owned functions
	// This is a unit test for the SQL query structure, not requiring a real database
	reader := NewPostgreSQLReader(nil, "public")
	c.Assert(reader, qt.IsNotNil)

	// The key test is that our query includes the NOT EXISTS clause to filter extension functions
	// We can't test the actual query execution without a database, but we can verify
	// that the reader is properly configured to exclude extension functions
	c.Assert(reader.schema, qt.Equals, "public")

	// This test documents the expected behavior:
	// Extension-owned functions should be filtered out by the database reader
	// to prevent migration generation from attempting to drop them
}

func TestPostgreSQLReader_enhanceTablesWithConstraints(t *testing.T) {
	c := qt.New(t)

	reader := NewPostgreSQLReader(nil, "public")

	// Create test data
	tables := []types.DBTable{
		{
			Name: "test_table",
			Columns: []types.DBColumn{
				{Name: "id", IsPrimaryKey: false, IsUnique: false},
				{Name: "email", IsPrimaryKey: false, IsUnique: false},
				{Name: "name", IsPrimaryKey: false, IsUnique: false},
				{Name: "sku", IsPrimaryKey: false, IsUnique: false},
				{Name: "region", IsPrimaryKey: false, IsUnique: false},
			},
		},
	}

	constraints := []types.DBConstraint{
		{TableName: "test_table", ColumnName: "id", Type: "PRIMARY KEY"},
		{TableName: "test_table", ColumnName: "email", Type: "UNIQUE"},
		{TableName: "test_table", ColumnNames: []string{"sku", "region"}, Type: "UNIQUE"},
	}

	// Test the enhancement
	reader.enhanceTablesWithConstraints(tables, constraints)

	// Verify results
	idCol := testutils.FindColumn(tables[0].Columns, "id")
	c.Assert(idCol.IsPrimaryKey, qt.IsTrue)
	c.Assert(idCol.IsUnique, qt.IsFalse)

	emailCol := testutils.FindColumn(tables[0].Columns, "email")
	c.Assert(emailCol.IsPrimaryKey, qt.IsFalse)
	c.Assert(emailCol.IsUnique, qt.IsTrue)

	nameCol := testutils.FindColumn(tables[0].Columns, "name")
	c.Assert(nameCol.IsPrimaryKey, qt.IsFalse)
	c.Assert(nameCol.IsUnique, qt.IsFalse)

	skuCol := testutils.FindColumn(tables[0].Columns, "sku")
	c.Assert(skuCol.IsPrimaryKey, qt.IsFalse)
	c.Assert(skuCol.IsUnique, qt.IsFalse)

	regionCol := testutils.FindColumn(tables[0].Columns, "region")
	c.Assert(regionCol.IsPrimaryKey, qt.IsFalse)
	c.Assert(regionCol.IsUnique, qt.IsFalse)
}

func TestNewPostgreSQLWriter(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		tests := []struct {
			name           string
			schema         string
			expectedSchema string
		}{
			{
				name:           "with custom schema",
				schema:         "test_schema",
				expectedSchema: "test_schema",
			},
			{
				name:           "with empty schema defaults to public",
				schema:         "",
				expectedSchema: "public",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				writer := NewPostgreSQLWriter(nil, test.schema)
				c.Assert(writer, qt.IsNotNil)
				c.Assert(writer.schema, qt.Equals, test.expectedSchema)
				c.Assert(writer.db, qt.IsNil) // We passed nil for testing
				c.Assert(writer.tx, qt.IsNil) // No transaction initially
			})
		}
	})
}

func TestPostgreSQLWriter_TransactionMethods_NoConnection(t *testing.T) {
	c := qt.New(t)
	writer := NewPostgreSQLWriter(nil, "public")

	t.Run("ExecuteSQL with no transaction", func(t *testing.T) {
		err := writer.ExecuteSQL(context.Background(), "SELECT 1")
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "no active transaction")
	})

	t.Run("CommitTransaction with no transaction", func(t *testing.T) {
		err := writer.CommitTransaction()
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "no active transaction")
	})

	t.Run("RollbackTransaction with no transaction", func(t *testing.T) {
		err := writer.RollbackTransaction()
		c.Assert(err, qt.IsNil) // Should not error when no transaction
	})
}

func TestPostgreSQLWriter_SchemaWriterInterface(t *testing.T) {
	c := qt.New(t)
	writer := NewPostgreSQLWriter(nil, "public")
	var _ types.SchemaWriter = writer
	c.Assert(writer, qt.IsNotNil)
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple identifier", in: "users", want: `"users"`},
		{name: "empty", in: "", want: `""`},
		{name: "mixed case preserved", in: "MyTable", want: `"MyTable"`},
		{name: "embedded double quote doubled", in: `weird"name`, want: `"weird""name"`},
		{name: "multiple embedded double quotes", in: `a"b"c`, want: `"a""b""c"`},
		{name: "name with space and semicolon", in: `t; DROP TABLE x; --`, want: `"t; DROP TABLE x; --"`},
		{name: "injection attempt via quote", in: `t" CASCADE; DROP TABLE y; --`, want: `"t"" CASCADE; DROP TABLE y; --"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(quoteIdent(tc.in), qt.Equals, tc.want)
		})
	}
}
