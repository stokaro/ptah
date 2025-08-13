package goschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestMySQLTypeCompatibilityForEmbeddedRelations tests that embedded relation mode
// generates compatible types for MySQL/MariaDB foreign key constraints
func TestMySQLTypeCompatibilityForEmbeddedRelations(t *testing.T) {
	c := qt.New(t)

	// Create a schema with embedded relation that should generate compatible types
	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "articles", StructName: "Article"},
		},
		Fields: []goschema.Field{
			// User table with SERIAL id (becomes INT AUTO_INCREMENT in MySQL)
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{
				StructName:       "Article",
				EmbeddedTypeName: "User",
				Mode:             "relation",
				Field:            "author_id",
				Ref:              "users(id)",
			},
		},
		Dependencies:               make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Process embedded fields to generate the author_id field
	processEmbeddedFieldsTest(db)

	// Find the generated author_id field
	var authorField *goschema.Field
	for _, field := range db.Fields {
		if field.Name == "author_id" {
			authorField = &field
			break
		}
	}

	c.Assert(authorField, qt.IsNotNil, qt.Commentf("author_id field should be generated"))
	c.Assert(authorField.Type, qt.Equals, "INTEGER")
	c.Assert(authorField.Foreign, qt.Equals, "users(id)")

	// Verify platform overrides are set for MySQL/MariaDB
	c.Assert(authorField.Overrides, qt.IsNotNil)
	c.Assert(authorField.Overrides["mysql"], qt.IsNotNil)
	c.Assert(authorField.Overrides["mysql"]["type"], qt.Equals, "INT")
	c.Assert(authorField.Overrides["mariadb"], qt.IsNotNil)
	c.Assert(authorField.Overrides["mariadb"]["type"], qt.Equals, "INT")

	// Test that the field conversion works correctly by creating columns
	// This tests the platform override functionality indirectly
	mysqlColumn := fromschema.FromField(*authorField, nil, "mysql")
	c.Assert(mysqlColumn.Type, qt.Equals, "INT")

	mariadbColumn := fromschema.FromField(*authorField, nil, "mariadb")
	c.Assert(mariadbColumn.Type, qt.Equals, "INT")

	postgresColumn := fromschema.FromField(*authorField, nil, "postgres")
	c.Assert(postgresColumn.Type, qt.Equals, "INTEGER")
}

// TestMySQLMigrationGeneratesCompatibleTypes tests that the full migration generation
// produces compatible types for MySQL foreign key constraints
func TestMySQLMigrationGeneratesCompatibleTypes(t *testing.T) {
	c := qt.New(t)

	// Create a schema similar to the failing integration tests
	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "articles", StructName: "Article"},
		},
		Fields: []goschema.Field{
			// User table with SERIAL id (AutoInc should be true for SERIAL)
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			// Article table with basic fields
			{StructName: "Article", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
			{StructName: "Article", Name: "title", Type: "VARCHAR(255)"},
			// Author ID field with platform overrides (simulating embedded relation)
			{
				StructName:     "Article",
				Name:           "author_id",
				Type:           "INTEGER",
				Foreign:        "users(id)",
				ForeignKeyName: "fk_article_author_id",
				Overrides: map[string]map[string]string{
					"mysql":   {"type": "INT"},
					"mariadb": {"type": "INT"},
				},
			},
		},
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Create schema diff for adding both tables
	diff := &types.SchemaDiff{
		TablesAdded: []string{"users", "articles"},
	}

	// Generate migration using MySQL planner
	planner := &mysql.Planner{}
	nodes := planner.GenerateMigrationAST(diff, db)

	// Render to SQL
	r := renderer.NewRenderer("mysql")
	var sqlStatements []string
	for _, node := range nodes {
		sql, err := r.Render(node)
		c.Assert(err, qt.IsNil)
		sqlStatements = append(sqlStatements, sql)
	}

	// Find the CREATE TABLE users statement
	var usersCreateSQL string
	var articlesCreateSQL string
	var authorFKSQL string

	for _, sql := range sqlStatements {
		if strings.Contains(sql, "CREATE TABLE users") {
			usersCreateSQL = sql
		}
		if strings.Contains(sql, "CREATE TABLE articles") {
			articlesCreateSQL = sql
		}
		if strings.Contains(sql, "fk_article_author_id") {
			authorFKSQL = sql
		}
	}

	c.Assert(usersCreateSQL, qt.Not(qt.Equals), "")
	c.Assert(articlesCreateSQL, qt.Not(qt.Equals), "")
	c.Assert(authorFKSQL, qt.Not(qt.Equals), "")

	// Verify that users.id is created as INT AUTO_INCREMENT
	c.Assert(usersCreateSQL, qt.Contains, "id INT")
	c.Assert(usersCreateSQL, qt.Contains, "AUTO_INCREMENT")
	c.Assert(usersCreateSQL, qt.Contains, "PRIMARY KEY")

	// Verify that articles.author_id is created as INT (not INTEGER)
	c.Assert(articlesCreateSQL, qt.Contains, "author_id INT")

	// Verify that the foreign key constraint references the correct types
	c.Assert(authorFKSQL, qt.Contains, "FOREIGN KEY (author_id)")
	c.Assert(authorFKSQL, qt.Contains, "REFERENCES users(id)")

	// The key test: both should use INT type, making them compatible
	// This should prevent the "incompatible column types" error in MySQL
}

// Helper function to process embedded fields (simplified version for testing)
func processEmbeddedFieldsTest(db *goschema.Database) {
	for _, embedded := range db.EmbeddedFields {
		if embedded.Mode == "relation" && embedded.Field != "" && embedded.Ref != "" {
			// Create platform-specific overrides for MySQL/MariaDB compatibility
			overrides := make(map[string]map[string]string)
			overrides["mysql"] = map[string]string{"type": "INT"}
			overrides["mariadb"] = map[string]string{"type": "INT"}

			// Generate the foreign key field
			db.Fields = append(db.Fields, goschema.Field{
				StructName:     embedded.StructName,
				FieldName:      embedded.EmbeddedTypeName,
				Name:           embedded.Field,
				Type:           "INTEGER",
				Foreign:        embedded.Ref,
				ForeignKeyName: "fk_" + strings.ToLower(embedded.StructName) + "_" + strings.ToLower(embedded.Field),
				Overrides:      overrides,
			})
		}
	}
}
