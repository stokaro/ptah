package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema/internal/dbtest"
	"github.com/stokaro/ptah/dbschema/types"
)

func TestNewMySQLReader(t *testing.T) {
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
				name:           "with empty schema defaults to information_schema",
				schema:         "",
				expectedSchema: "information_schema",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				reader := NewMySQLReader(nil, test.schema)
				c.Assert(reader, qt.IsNotNil)
				c.Assert(reader.schema, qt.Equals, test.expectedSchema)
				c.Assert(reader.db, qt.IsNil) // We passed nil for testing
			})
		}
	})
}

func TestMySQLReader_CheckConstraintIntrospectionErrorClassification(t *testing.T) {
	c := qt.New(t)

	c.Assert(isMissingCheckConstraintTableNameColumn(&mysqldriver.MySQLError{
		Number:  1054,
		Message: "Unknown column 'TABLE_NAME' in 'field list'",
	}), qt.IsTrue)
	c.Assert(isMissingCheckConstraintTableNameColumn(&mysqldriver.MySQLError{
		Number:  1054,
		Message: "Unknown column 'table_name' in 'field list'",
	}), qt.IsTrue)
	c.Assert(isMissingCheckConstraintTableNameColumn(&mysqldriver.MySQLError{
		Number:  1054,
		Message: "Unknown column 'CHECK_CLAUSE' in 'field list'",
	}), qt.IsFalse)

	c.Assert(isMissingCheckConstraintsTable(&mysqldriver.MySQLError{
		Number:  1109,
		Message: "Unknown table 'CHECK_CONSTRAINTS' in information_schema",
	}), qt.IsTrue)
	c.Assert(isMissingCheckConstraintsTable(&mysqldriver.MySQLError{
		Number:  1146,
		Message: "Table 'information_schema.check_constraints' doesn't exist",
	}), qt.IsTrue)
	c.Assert(isMissingCheckConstraintsTable(&mysqldriver.MySQLError{
		Number:  1142,
		Message: "SELECT command denied to user",
	}), qt.IsFalse)
}

func TestMySQLReader_parseEnumValues(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		tests := []struct {
			name         string
			columnType   string
			expectedVals []string
		}{
			{
				name:         "simple enum",
				columnType:   "enum('active','inactive')",
				expectedVals: []string{"active", "inactive"},
			},
			{
				name:         "enum with spaces",
				columnType:   "enum('value 1', 'value 2', 'value 3')",
				expectedVals: []string{"value 1", "value 2", "value 3"},
			},
			{
				name:         "enum with double quotes",
				columnType:   `enum("active","inactive")`,
				expectedVals: []string{"active", "inactive"},
			},
			{
				name:         "single value enum",
				columnType:   "enum('single')",
				expectedVals: []string{"single"},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				result := parseEnumValues(test.columnType)
				c.Assert(result, qt.DeepEquals, test.expectedVals)
			})
		}
	})

	t.Run("unhappy path", func(t *testing.T) {
		tests := []struct {
			name       string
			columnType string
		}{
			{
				name:       "not an enum",
				columnType: "varchar(255)",
			},
			{
				name:       "empty enum",
				columnType: "enum()",
			},
			{
				name:       "invalid format",
				columnType: "not_enum_format",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				result := parseEnumValues(test.columnType)
				c.Assert(result, qt.IsNil)
			})
		}
	})
}

func TestMySQLReader_ReadSchema_NoConnection(t *testing.T) {
	c := qt.New(t)

	// Test that reader can be created with nil database
	reader := NewMySQLReader(nil, "test")
	c.Assert(reader, qt.IsNotNil)
	c.Assert(reader.schema, qt.Equals, "test")
	c.Assert(reader.db, qt.IsNil)

	// Note: We don't test ReadSchema() with nil db as it would panic
	// This is expected behavior - the reader requires a valid database connection
}

func TestMySQLReaderReadTablesUsesBulkColumnQuery(t *testing.T) {
	c := qt.New(t)

	tableRows := make([][]driver.Value, 0, 50)
	columnRows := make([][]driver.Value, 0, 150)
	for i := range 50 {
		tableName := fmt.Sprintf("table_%02d", i)
		comment := ""
		if i == 0 {
			comment = "customer accounts"
		}
		tableRows = append(tableRows, []driver.Value{tableName, "BASE TABLE", comment})
		columnRows = append(columnRows,
			[]driver.Value{tableName, "id", "int", "int", "NO", nil, nil, int64(10), int64(0), int64(1), nil, nil, "auto_increment", nil},
			[]driver.Value{tableName, "email", "varchar", "varchar(255)", "NO", nil, int64(255), nil, nil, int64(2), "utf8mb4", "utf8mb4_0900_ai_ci", "", nil},
			[]driver.Value{tableName, "email_lc", "varchar", "varchar(255)", "YES", nil, int64(255), nil, nil, int64(3), "utf8mb4", "utf8mb4_0900_ai_ci", "STORED GENERATED", "lower(`email`)"},
		)
	}

	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM information_schema.COLUMNS"):
			return dbtest.QueryResult{
				Columns: []string{
					"TABLE_NAME",
					"COLUMN_NAME",
					"DATA_TYPE",
					"COLUMN_TYPE",
					"IS_NULLABLE",
					"COLUMN_DEFAULT",
					"CHARACTER_MAXIMUM_LENGTH",
					"NUMERIC_PRECISION",
					"NUMERIC_SCALE",
					"ORDINAL_POSITION",
					"CHARACTER_SET_NAME",
					"COLLATION_NAME",
					"EXTRA",
					"GENERATION_EXPRESSION",
				},
				Rows: columnRows,
			}, nil
		case strings.Contains(query, "FROM information_schema.TABLES"):
			return dbtest.QueryResult{
				Columns: []string{"TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT"},
				Rows:    tableRows,
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	})
	reader := NewMySQLReader(db.SQL, "app")

	tables, err := reader.readTables("app")

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 2)
	c.Assert(tables, qt.HasLen, 50)
	c.Assert(tables[0].Name, qt.Equals, "table_00")
	c.Assert(tables[0].Comment, qt.Equals, "customer accounts")
	c.Assert(tables[0].Columns, qt.HasLen, 3)

	id := tables[0].Columns[0]
	c.Assert(id.IsAutoIncrement, qt.IsTrue)
	c.Assert(id.NumericPrecision, qt.IsNotNil)
	c.Assert(*id.NumericPrecision, qt.Equals, 10)
	c.Assert(id.NumericScale, qt.IsNotNil)
	c.Assert(*id.NumericScale, qt.Equals, 0)

	email := tables[0].Columns[1]
	c.Assert(email.DataType, qt.Equals, "varchar(255)")
	c.Assert(email.ColumnType, qt.Equals, "varchar(255)")
	c.Assert(email.CharacterMaxLength, qt.IsNotNil)
	c.Assert(*email.CharacterMaxLength, qt.Equals, 255)
	c.Assert(email.Charset, qt.Equals, "utf8mb4")
	c.Assert(email.Collate, qt.Equals, "utf8mb4_0900_ai_ci")

	emailLC := tables[0].Columns[2]
	c.Assert(emailLC.GeneratedKind, qt.Equals, "STORED")
	c.Assert(emailLC.GeneratedExpression, qt.IsNotNil)
	c.Assert(*emailLC.GeneratedExpression, qt.Equals, "lower(`email`)")
}

func TestEnhanceTablesWithPrimaryKeys(t *testing.T) {
	c := qt.New(t)

	tables := []types.DBTable{
		{
			Name: "memberships",
			Columns: []types.DBColumn{
				{Name: "org_id"},
				{Name: "user_id"},
				{Name: "role"},
			},
		},
	}
	constraints := []types.DBConstraint{
		{
			TableName:   "memberships",
			Type:        "PRIMARY KEY",
			ColumnNames: []string{"org_id", "user_id"},
		},
	}

	enhanceTablesWithPrimaryKeys(tables, constraints)

	c.Assert(tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(tables[0].Columns[1].IsPrimaryKey, qt.IsTrue)
	c.Assert(tables[0].Columns[2].IsPrimaryKey, qt.IsFalse)
}

func TestApplyMySQLColumnMetadataKeepsGeneratedExpressionWithoutExtra(t *testing.T) {
	c := qt.New(t)

	var col types.DBColumn
	applyMySQLColumnMetadata(
		&col,
		sql.NullString{String: "default", Valid: true},
		sql.NullInt64{Int64: 255, Valid: true},
		sql.NullInt64{Int64: 10, Valid: true},
		sql.NullInt64{Int64: 2, Valid: true},
		sql.NullString{String: "utf8mb4", Valid: true},
		sql.NullString{String: "utf8mb4_0900_ai_ci", Valid: true},
		sql.NullString{},
		sql.NullString{String: "lower(`email`)", Valid: true},
	)

	c.Assert(col.GeneratedExpression, qt.IsNotNil)
	c.Assert(*col.GeneratedExpression, qt.Equals, "lower(`email`)")
	c.Assert(col.GeneratedKind, qt.Equals, "")
	c.Assert(col.ColumnDefault, qt.IsNotNil)
	c.Assert(*col.ColumnDefault, qt.Equals, "default")
}

func TestNormalizeMySQLColumnDefaultQuotesCatalogStringLiterals(t *testing.T) {
	tests := []struct {
		name         string
		columnType   string
		dataType     string
		defaultValue string
		want         string
	}{
		{
			name:         "enum value",
			columnType:   "enum('draft','active')",
			dataType:     "enum",
			defaultValue: "draft",
			want:         "'draft'",
		},
		{
			name:         "varchar value with quote",
			columnType:   "varchar(255)",
			dataType:     "varchar",
			defaultValue: "owner's draft",
			want:         "'owner''s draft'",
		},
		{
			name:         "numeric value",
			columnType:   "int",
			dataType:     "int",
			defaultValue: "42",
			want:         "42",
		},
		{
			name:         "temporal expression",
			columnType:   "timestamp",
			dataType:     "timestamp",
			defaultValue: "current_timestamp()",
			want:         "current_timestamp()",
		},
		{
			name:         "quoted value preserved",
			columnType:   "varchar(255)",
			dataType:     "varchar",
			defaultValue: "'draft'",
			want:         "'draft'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			col := &types.DBColumn{
				ColumnType: test.columnType,
				DataType:   test.dataType,
			}

			c.Assert(normalizeMySQLColumnDefault(col, test.defaultValue), qt.Equals, test.want)
		})
	}
}

func TestNewMySQLWriter(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		c := qt.New(t)
		writer := NewMySQLWriter(nil, "test_schema")
		c.Assert(writer, qt.IsNotNil)
		c.Assert(writer.schema, qt.Equals, "test_schema")
		c.Assert(writer.db, qt.IsNil) // We passed nil for testing
		c.Assert(writer.tx, qt.IsNil) // No transaction initially
	})
}

func TestMySQLWriter_TransactionMethods_NoConnection(t *testing.T) {
	c := qt.New(t)
	writer := NewMySQLWriter(nil, "test")

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

func TestMySQLWriter_UtilityMethods(t *testing.T) {
	c := qt.New(t)
	writer := NewMySQLWriter(nil, "test")

	t.Run("splitSQLStatements", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected []string
		}{
			{
				name:     "single statement",
				input:    "CREATE TABLE test (id INT)",
				expected: []string{"CREATE TABLE test (id INT)"},
			},
			{
				name:     "multiple statements",
				input:    "CREATE TABLE test (id INT); CREATE INDEX idx_test ON test (id);",
				expected: []string{"CREATE TABLE test (id INT)", "CREATE INDEX idx_test ON test (id)"},
			},
			{
				name:     "with comments",
				input:    "CREATE TABLE test (id INT); /* This is a comment; */ CREATE INDEX idx_test ON test (id);",
				expected: []string{"CREATE TABLE test (id INT)", "/* This is a comment; */ CREATE INDEX idx_test ON test (id)"},
			},
			{
				name:     "empty statements",
				input:    "CREATE TABLE test (id INT);;; CREATE INDEX idx_test ON test (id);",
				expected: []string{"CREATE TABLE test (id INT)", "CREATE INDEX idx_test ON test (id)"},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := sqlutil.SplitSQLStatements(test.input)
				c.Assert(result, qt.DeepEquals, test.expected)
			})
		}
	})

	t.Run("isCreateTableStatement", func(t *testing.T) {
		tests := []struct {
			name     string
			sql      string
			expected bool
		}{
			{
				name:     "CREATE TABLE statement",
				sql:      "CREATE TABLE test (id INT)",
				expected: true,
			},
			{
				name:     "CREATE TABLE with whitespace",
				sql:      "  CREATE TABLE test (id INT)",
				expected: true,
			},
			{
				name:     "CREATE INDEX statement",
				sql:      "CREATE INDEX idx_test ON test (id)",
				expected: false,
			},
			{
				name:     "SELECT statement",
				sql:      "SELECT * FROM test",
				expected: false,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := writer.isCreateTableStatement(test.sql)
				c.Assert(result, qt.Equals, test.expected)
			})
		}
	})

	t.Run("isCreateIndexStatement", func(t *testing.T) {
		tests := []struct {
			name     string
			sql      string
			expected bool
		}{
			{
				name:     "CREATE INDEX statement",
				sql:      "CREATE INDEX idx_test ON test (id)",
				expected: true,
			},
			{
				name:     "CREATE UNIQUE INDEX statement",
				sql:      "CREATE UNIQUE INDEX idx_test ON test (id)",
				expected: true,
			},
			{
				name:     "CREATE TABLE statement",
				sql:      "CREATE TABLE test (id INT)",
				expected: false,
			},
			{
				name:     "SELECT statement",
				sql:      "SELECT * FROM test",
				expected: false,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := writer.isCreateIndexStatement(test.sql)
				c.Assert(result, qt.Equals, test.expected)
			})
		}
	})

	t.Run("extractTableNameFromCreateTable", func(t *testing.T) {
		tests := []struct {
			name     string
			sql      string
			expected string
		}{
			{
				name:     "simple CREATE TABLE",
				sql:      "CREATE TABLE test (id INT)",
				expected: "test",
			},
			{
				name:     "CREATE TABLE with parenthesis",
				sql:      "CREATE TABLE users(",
				expected: "users",
			},
			{
				name:     "invalid statement",
				sql:      "SELECT * FROM test",
				expected: "",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := writer.extractTableNameFromCreateTable(test.sql)
				c.Assert(result, qt.Equals, test.expected)
			})
		}
	})

	t.Run("extractTableNameFromCreateIndex", func(t *testing.T) {
		tests := []struct {
			name     string
			sql      string
			expected string
		}{
			{
				name:     "simple CREATE INDEX",
				sql:      "CREATE INDEX idx_test ON test (id)",
				expected: "test",
			},
			{
				name:     "CREATE INDEX with parenthesis",
				sql:      "CREATE UNIQUE INDEX idx_users ON users(",
				expected: "users",
			},
			{
				name:     "invalid statement",
				sql:      "CREATE TABLE test (id INT)",
				expected: "",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := writer.extractTableNameFromCreateIndex(test.sql)
				c.Assert(result, qt.Equals, test.expected)
			})
		}
	})
}

func TestMySQLWriter_SchemaWriterInterface(t *testing.T) {
	c := qt.New(t)
	writer := NewMySQLWriter(nil, "test")
	var _ types.SchemaWriter = writer
	c.Assert(writer, qt.IsNotNil)
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple identifier", in: "users", want: "`users`"},
		{name: "empty", in: "", want: "``"},
		{name: "mixed case preserved", in: "MyTable", want: "`MyTable`"},
		{name: "embedded backtick doubled", in: "weird`name", want: "`weird``name`"},
		{name: "multiple embedded backticks", in: "a`b`c", want: "`a``b``c`"},
		{name: "name with space and semicolon", in: "t; DROP TABLE x; --", want: "`t; DROP TABLE x; --`"},
		{name: "injection attempt via backtick", in: "t` ; DROP TABLE y; --", want: "`t`` ; DROP TABLE y; --`"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(quoteIdent(tc.in), qt.Equals, tc.want)
		})
	}
}
