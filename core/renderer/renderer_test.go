package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
)

func TestSupportedDialects(t *testing.T) {
	c := qt.New(t)

	dialects := renderer.SupportedDialects()
	expected := []string{"postgresql", "postgres", "mysql", "mariadb", "clickhouse", "sqlite", "sqlite3", "cockroachdb", "yugabytedb", "spanner"}

	c.Assert(dialects, qt.DeepEquals, expected)
}

func TestNewRenderer_SupportedDialects(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		expected string
	}{
		{
			name:     "PostgreSQL",
			dialect:  "postgresql",
			expected: "postgres",
		},
		{
			name:     "Postgres alias",
			dialect:  "postgres",
			expected: "postgres",
		},
		{
			name:     "MySQL",
			dialect:  "mysql",
			expected: "mysql",
		},
		{
			name:     "MariaDB",
			dialect:  "mariadb",
			expected: "mariadb",
		},
		{
			name:     "ClickHouse",
			dialect:  "clickhouse",
			expected: "clickhouse",
		},
		{
			name:     "SQLite",
			dialect:  "sqlite",
			expected: "sqlite",
		},
		{
			name:     "SQLite alias",
			dialect:  "sqlite3",
			expected: "sqlite",
		},
		{
			name:     "CockroachDB",
			dialect:  "cockroachdb",
			expected: "cockroachdb",
		},
		{
			name:     "CockroachDB alias",
			dialect:  "crdb",
			expected: "cockroachdb",
		},
		{
			name:     "YugabyteDB",
			dialect:  "yugabytedb",
			expected: "yugabytedb",
		},
		{
			name:     "Spanner",
			dialect:  "spanner",
			expected: "spanner",
		},
		{
			name:     "Case insensitive PostgreSQL",
			dialect:  "POSTGRESQL",
			expected: "postgres",
		},
		{
			name:     "Case insensitive MySQL",
			dialect:  "MySQL",
			expected: "mysql",
		},
		{
			name:     "Whitespace handling",
			dialect:  "  postgresql  ",
			expected: "postgres",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(r, qt.IsNotNil)
			c.Assert(r.GetDialect(), qt.Equals, tt.expected)
		})
	}
}

func TestNewRenderer_UnsupportedDialects(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{
			name:    "Oracle",
			dialect: "oracle",
		},
		{
			name:    "SQL Server",
			dialect: "sqlserver",
		},
		{
			name:    "Empty string",
			dialect: "",
		},
		{
			name:    "Random string",
			dialect: "random",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(tt.dialect)
			c.Assert(r, qt.IsNil)
			c.Assert(err, qt.ErrorMatches, "unsupported database dialect: "+tt.dialect)
		})
	}
}

func TestPostgresFamilyRenderer_CapabilityGates(t *testing.T) {
	c := qt.New(t)

	idx := ast.NewIndex("idx_users_email", "users", "email").SetIfNotExists()
	idx.Concurrently = true

	sql, err := renderer.RenderSQL("cockroachdb", idx)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);")
	c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY",
		qt.Commentf("CockroachDB renderer must strip a stray CONCURRENTLY flag; got:\n%s", sql))

	xmlTable := ast.NewCreateTable("documents").
		AddColumn(ast.NewColumn("id", "INT8").SetPrimary()).
		AddColumn(ast.NewColumn("payload", "XML"))
	_, err = renderer.RenderSQL("cockroachdb", xmlTable)
	c.Assert(err, qt.ErrorMatches, `error rendering column payload: cockroachdb does not support XML columns; use a platform-specific type override`)
}

func TestRenderSQL_Success(t *testing.T) {
	c := qt.New(t)

	// Create a simple comment node for testing
	comment := &ast.CommentNode{Text: "Test comment"}

	sql, err := renderer.RenderSQL("postgresql", comment)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "Test comment")
}

func TestRenderSQL_UnsupportedDialect(t *testing.T) {
	c := qt.New(t)

	comment := &ast.CommentNode{Text: "Test comment"}

	sql, err := renderer.RenderSQL("sqlserver", comment)
	c.Assert(sql, qt.Equals, "")
	c.Assert(err, qt.ErrorMatches, "unsupported database dialect: sqlserver")
}

func TestRenderer_Interface(t *testing.T) {
	// Test that all dialect renderers implement the RenderVisitor interface
	dialects := []string{"postgresql", "mysql", "mariadb"}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(dialect)
			c.Assert(err, qt.IsNil)

			// Test interface methods
			c.Assert(r.GetDialect(), qt.IsNotNil)
			c.Assert(r.GetOutput(), qt.Equals, "")

			// Test Reset
			r.Reset()
			c.Assert(r.GetOutput(), qt.Equals, "")

			// Test Render with a simple node
			comment := &ast.CommentNode{Text: "Test"}
			sql, err := r.Render(comment)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.IsNotNil)
		})
	}
}

func TestRenderer_BasicRendering(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		node     ast.Node
		contains []string
	}{
		{
			name:     "PostgreSQL comment",
			dialect:  "postgresql",
			node:     &ast.CommentNode{Text: "PostgreSQL comment"},
			contains: []string{"PostgreSQL comment"},
		},
		{
			name:     "MySQL comment",
			dialect:  "mysql",
			node:     &ast.CommentNode{Text: "MySQL comment"},
			contains: []string{"MySQL comment"},
		},
		{
			name:     "MariaDB comment",
			dialect:  "mariadb",
			node:     &ast.CommentNode{Text: "MariaDB comment"},
			contains: []string{"MariaDB comment"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(tt.dialect)
			c.Assert(err, qt.IsNil)

			sql, err := r.Render(tt.node)
			c.Assert(err, qt.IsNil)

			for _, expected := range tt.contains {
				c.Assert(sql, qt.Contains, expected)
			}
		})
	}
}

func TestRenderer_CreateTable(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		contains []string
	}{
		{
			name:     "PostgreSQL CREATE TABLE",
			dialect:  "postgresql",
			contains: []string{"CREATE TABLE", "users", "POSTGRES TABLE"},
		},
		{
			name:     "MySQL CREATE TABLE",
			dialect:  "mysql",
			contains: []string{"CREATE TABLE", "users", "MYSQL TABLE"},
		},
		{
			name:     "MariaDB CREATE TABLE",
			dialect:  "mariadb",
			contains: []string{"CREATE TABLE", "users", "MARIADB TABLE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(tt.dialect)
			c.Assert(err, qt.IsNil)

			table := &ast.CreateTableNode{
				Name: "users",
				Columns: []*ast.ColumnNode{
					{
						Name:     "id",
						Type:     "INTEGER",
						Primary:  true,
						Nullable: false,
					},
					{
						Name:     "email",
						Type:     "VARCHAR(255)",
						Unique:   true,
						Nullable: false,
					},
				},
			}

			sql, err := r.Render(table)
			c.Assert(err, qt.IsNil)

			for _, expected := range tt.contains {
				c.Assert(sql, qt.Contains, expected)
			}
		})
	}
}

// TestNewVisitorMethods_UnitTests tests the new visitor methods without database dependencies
func TestNewVisitorMethods_UnitTests(t *testing.T) {
	dialects := []string{"postgresql", "mysql", "mariadb"}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			t.Run("DropIndex", func(t *testing.T) {
				c := qt.New(t)

				dropIndex := ast.NewDropIndex("test_index").
					SetTable("test_table").
					SetIfExists().
					SetComment("Test comment")

				sql, err := renderer.RenderSQL(dialect, dropIndex)
				c.Assert(err, qt.IsNil)
				sql = legacyRenderedSQL(sql)
				c.Assert(sql, qt.IsNotNil)
				c.Assert(sql, qt.Contains, "DROP INDEX")
				c.Assert(sql, qt.Contains, "test_index")

				if dialect == "postgresql" {
					c.Assert(sql, qt.Not(qt.Contains), "ON test_table")
				} else {
					c.Assert(sql, qt.Contains, "ON test_table")
				}
			})

			t.Run("CreateType", func(t *testing.T) {
				c := qt.New(t)

				enumDef := ast.NewEnumTypeDef("value1", "value2")
				createType := ast.NewCreateType("test_type", enumDef).
					SetComment("Test type")

				sql, err := renderer.RenderSQL(dialect, createType)
				c.Assert(err, qt.IsNil)
				sql = legacyRenderedSQL(sql)
				c.Assert(sql, qt.IsNotNil)

				if dialect == "postgresql" {
					c.Assert(sql, qt.Contains, "CREATE TYPE test_type AS ENUM")
					c.Assert(sql, qt.Contains, "'value1'")
					c.Assert(sql, qt.Contains, "'value2'")
				} else {
					c.Assert(sql, qt.Contains, "does not support CREATE TYPE")
				}
			})

			t.Run("AlterType", func(t *testing.T) {
				c := qt.New(t)

				alterType := ast.NewAlterType("test_type").
					AddOperation(ast.NewAddEnumValueOperation("new_value"))

				sql, err := renderer.RenderSQL(dialect, alterType)
				c.Assert(err, qt.IsNil)
				sql = legacyRenderedSQL(sql)
				c.Assert(sql, qt.IsNotNil)

				if dialect == "postgresql" {
					c.Assert(sql, qt.Contains, "ALTER TYPE test_type ADD VALUE 'new_value'")
				} else {
					c.Assert(sql, qt.Contains, "does not support ALTER TYPE")
				}
			})
		})
	}
}

func TestPlatformSpecificOverrides(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	// Test PostgreSQL (default)
	postgresStatements := renderer.GetOrderedCreateStatements(result, "postgres")
	var postgresArticlesSQL string
	for _, statement := range postgresStatements {
		statement = legacyRenderedSQL(statement)
		if strings.Contains(statement, "CREATE TABLE articles") {
			postgresArticlesSQL = statement
			break
		}
	}
	c.Assert(postgresArticlesSQL, qt.Contains, "meta_data JSONB")

	// Test MySQL (override)
	mysqlStatements := renderer.GetOrderedCreateStatements(result, "mysql")
	var mysqlArticlesSQL string
	for _, statement := range mysqlStatements {
		statement = legacyRenderedSQL(statement)
		if strings.Contains(statement, "CREATE TABLE articles") {
			mysqlArticlesSQL = statement
			break
		}
	}
	c.Assert(mysqlArticlesSQL, qt.Contains, "meta_data JSON")

	// Test MariaDB (override with check constraint)
	mariadbStatements := renderer.GetOrderedCreateStatements(result, "mariadb")
	var mariadbArticlesSQL string
	for _, statement := range mariadbStatements {
		statement = legacyRenderedSQL(statement)
		if strings.Contains(statement, "CREATE TABLE articles") {
			mariadbArticlesSQL = statement
			break
		}
	}
	c.Assert(mariadbArticlesSQL, qt.Contains, "meta_data LONGTEXT")
	c.Assert(mariadbArticlesSQL, qt.Contains, "JSON_VALID(meta_data)")
}

func TestEmbeddedFieldsInPackageParser(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	// Find the articles table statement
	statements := renderer.GetOrderedCreateStatements(result, "postgres")
	var articlesSQL string
	for _, statement := range statements {
		statement = legacyRenderedSQL(statement)
		if strings.Contains(statement, "CREATE TABLE articles") {
			articlesSQL = statement
			break
		}
	}

	c.Assert(articlesSQL, qt.Not(qt.Equals), "")

	// Verify embedded fields are included
	c.Assert(articlesSQL, qt.Contains, "created_at", qt.Commentf("Should contain created_at from Timestamps"))
	c.Assert(articlesSQL, qt.Contains, "updated_at", qt.Commentf("Should contain updated_at from Timestamps"))
	c.Assert(articlesSQL, qt.Contains, "audit_by", qt.Commentf("Should contain audit_by from AuditInfo"))
	c.Assert(articlesSQL, qt.Contains, "audit_reason", qt.Commentf("Should contain audit_reason from AuditInfo"))
	c.Assert(articlesSQL, qt.Contains, "meta_data", qt.Commentf("Should contain meta_data from Meta"))
	c.Assert(articlesSQL, qt.Contains, "author_id", qt.Commentf("Should contain author_id from User relation"))
}

func TestGetOrderedCreateStatements(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	statements := renderer.GetOrderedCreateStatements(result, "postgres")

	c.Assert(statements[0], qt.Contains, "CREATE TYPE")

	createTables := 0
	foreignKeyAlters := 0
	indexes := 0
	seenForeignKeyAlter := false
	seenIndex := false
	for _, statement := range statements[1:] {
		switch {
		case strings.Contains(statement, "CREATE TABLE"):
			c.Assert(seenForeignKeyAlter, qt.IsFalse)
			c.Assert(seenIndex, qt.IsFalse)
			c.Assert(statement, qt.Not(qt.Contains), "FOREIGN KEY")
			createTables++
		case strings.Contains(statement, "ALTER TABLE") && strings.Contains(statement, "FOREIGN KEY"):
			c.Assert(seenIndex, qt.IsFalse)
			seenForeignKeyAlter = true
			foreignKeyAlters++
		case strings.Contains(statement, "CREATE INDEX"):
			seenIndex = true
			indexes++
		}
	}

	c.Assert(createTables, qt.Equals, len(result.Tables))
	c.Assert(foreignKeyAlters > 0, qt.IsTrue)
	c.Assert(indexes, qt.Equals, 2)
}

func TestGetOrderedCreateStatements_MutualForeignKeysAreTwoPhase(t *testing.T) {
	c := qt.New(t)

	result := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "A", Name: "a"},
			{StructName: "B", Name: "b"},
		},
		Fields: []goschema.Field{
			{StructName: "A", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "A", Name: "b_id", Type: "INTEGER", Foreign: "b(id)", ForeignKeyName: "fk_a_b"},
			{StructName: "B", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "B", Name: "a_id", Type: "INTEGER", Foreign: "a(id)", ForeignKeyName: "fk_b_a"},
		},
	}

	statements := renderer.GetOrderedCreateStatements(result, "postgres")

	c.Assert(statements, qt.HasLen, 4)
	c.Assert(statements[0], qt.Contains, `CREATE TABLE "a"`)
	c.Assert(statements[0], qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(statements[1], qt.Contains, `CREATE TABLE "b"`)
	c.Assert(statements[1], qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(statements[2], qt.Contains, `ALTER TABLE "a" ADD CONSTRAINT "fk_a_b" FOREIGN KEY ("b_id") REFERENCES "b"("id")`)
	c.Assert(statements[3], qt.Contains, `ALTER TABLE "b" ADD CONSTRAINT "fk_b_a" FOREIGN KEY ("a_id") REFERENCES "a"("id")`)
}

func TestGenerateSchema_Deterministic(t *testing.T) {
	fixtureDir := "../../integration/fixtures/entities/035-roundtrip-fk-diamond"
	dialects := []string{"postgres", "mysql", "mariadb"}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			var first string
			for i := range 100 {
				result, err := goschema.ParseDir(fixtureDir)
				c.Assert(err, qt.IsNil)

				sql := strings.Join(renderer.GetOrderedCreateStatements(result, dialect), "\n")
				if i == 0 {
					first = sql
					continue
				}
				c.Assert(sql, qt.Equals, first)
			}
		})
	}
}
