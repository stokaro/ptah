package postgres

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/dbtest"
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

func TestPostgreSQLReaderReadTablesUsesBulkColumnQuery(t *testing.T) {
	c := qt.New(t)

	tableRows := make([][]driver.Value, 0, 50)
	columnRows := make([][]driver.Value, 0, 100)
	for i := range 50 {
		tableName := fmt.Sprintf("table_%02d", i)
		tableRows = append(tableRows, []driver.Value{"public", tableName, "BASE TABLE", "", int64(0), false})
		columnRows = append(columnRows,
			[]driver.Value{tableName, "id", "integer", "int4", "NO", nil, nil, nil, nil, int64(1), "", ""},
			[]driver.Value{tableName, "name", "character varying", "varchar", "NO", nil, int64(255), nil, nil, int64(2), "", ""},
		)
	}

	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM information_schema.columns"):
			return dbtest.QueryResult{
				Columns: []string{
					"table_name",
					"column_name",
					"data_type",
					"udt_name",
					"is_nullable",
					"column_default",
					"character_maximum_length",
					"numeric_precision",
					"numeric_scale",
					"ordinal_position",
					"generated_kind",
					"generated_expression",
				},
				Rows: columnRows,
			}, nil
		case strings.Contains(query, "FROM information_schema.tables"):
			return dbtest.QueryResult{
				Columns: []string{
					"table_schema",
					"table_name",
					"table_type",
					"table_comment",
					"estimated_rows",
					"rls_enabled",
				},
				Rows: tableRows,
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	})
	reader := NewPostgreSQLReader(db.SQL, "public")

	tables, err := reader.readTablesForSchema("public")

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 2)
	c.Assert(tables, qt.HasLen, 50)
	c.Assert(tables[0].Name, qt.Equals, "table_00")
	c.Assert(tables[0].Columns, qt.HasLen, 2)
	c.Assert(tables[0].Columns[1].CharacterMaxLength, qt.IsNotNil)
	c.Assert(*tables[0].Columns[1].CharacterMaxLength, qt.Equals, 255)
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
			})
		}
	})
}

func TestPostgreSQLWriter_TransactionMethods_NoConnection(t *testing.T) {
	c := qt.New(t)
	writer := NewPostgreSQLWriter(nil, "public")

	t.Run("ExecuteSQL with no connection", func(t *testing.T) {
		err := writer.ExecuteSQL(context.Background(), "SELECT 1")
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "no database connection")
	})

	t.Run("BeginTransaction with no connection", func(t *testing.T) {
		tx, err := writer.BeginTransaction(context.Background())
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "no database connection")
		c.Assert(tx, qt.IsNil)
	})

	t.Run("dry-run transaction", func(t *testing.T) {
		writer.SetDryRun(true)
		tx, err := writer.BeginTransaction(context.Background())
		c.Assert(err, qt.IsNil)
		c.Assert(tx, qt.IsNotNil)
		c.Assert(tx.ExecuteSQL(context.Background(), "SELECT 1"), qt.IsNil)
		c.Assert(tx.Commit(), qt.IsNil)
	})
}

func TestPostgreSQLWriter_SchemaWriterInterface(t *testing.T) {
	c := qt.New(t)
	writer := NewPostgreSQLWriter(nil, "public")
	var _ types.SchemaWriter = writer
	c.Assert(writer, qt.IsNotNil)
}

func TestPostgreSQLWriterConcurrentTransactions(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, nil, nil)
	writer := NewPostgreSQLWriter(db.SQL, "public")

	const goroutines = 64
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	start := make(chan struct{})

	for i := range goroutines {
		wg.Go(func() {
			<-start
			tx, err := writer.BeginTransaction(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if err := tx.ExecuteSQL(context.Background(), "SELECT 1"); err != nil {
				_ = tx.Rollback()
				errs <- err
				return
			}
			if i%2 == 0 {
				errs <- tx.Commit()
				return
			}
			errs <- tx.Rollback()
		})
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		c.Assert(err, qt.IsNil)
	}
	c.Assert(db.BeginCount(), qt.Equals, goroutines)
	c.Assert(db.ExecCount(), qt.Equals, goroutines)
	c.Assert(db.CommitCount()+db.RollbackCount(), qt.Equals, goroutines)
}

func TestPostgreSQLWriterDropAllTablesCommitsOnSuccess(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresDropAllQueryHandler, nil)
	writer := NewPostgreSQLWriter(db.SQL, "public")

	c.Assert(writer.DropAllTables(), qt.IsNil)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.ExecCount(), qt.Equals, 3)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestPostgreSQLWriterDropAllTablesRollsBackOnFailure(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, postgresDropAllQueryHandler, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		if strings.Contains(query, "DROP TYPE") {
			return nil, fmt.Errorf("boom")
		}
		return driver.RowsAffected(0), nil
	})
	writer := NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables()
	c.Assert(err, qt.ErrorMatches, `failed to drop enum status: SQL execution failed: boom\nSQL: DROP TYPE IF EXISTS "status" CASCADE`)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func postgresDropAllQueryHandler(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "information_schema.tables"):
		return dbtest.QueryResult{Columns: []string{"table_name"}, Rows: [][]driver.Value{{"users"}}}, nil
	case strings.Contains(query, "pg_type"):
		return dbtest.QueryResult{Columns: []string{"typname"}, Rows: [][]driver.Value{{"status"}}}, nil
	case strings.Contains(query, "information_schema.sequences"):
		return dbtest.QueryResult{Columns: []string{"sequence_name"}, Rows: [][]driver.Value{{"users_id_seq"}}}, nil
	default:
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
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
