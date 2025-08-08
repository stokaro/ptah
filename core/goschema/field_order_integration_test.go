package goschema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestFieldOrderConsistencyInMigrationGeneration tests that field order is preserved
// and consistent across multiple migration generation runs.
func TestFieldOrderConsistencyInMigrationGeneration(t *testing.T) {
	c := qt.New(t)

	// Create a test Go file with multiple fields in a specific order
	testContent := `package test

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default="CURRENT_TIMESTAMP"
	UpdatedAt time.Time

	//migrator:schema:field name="status" type="VARCHAR(20)" not_null="true" default="'active'"
	Status string
}`

	// Create temporary directory and file
	tmpDir := c.TempDir()
	testFile := filepath.Join(tmpDir, "user.go")
	err := os.WriteFile(testFile, []byte(testContent), 0600)
	c.Assert(err, qt.IsNil)

	// Generate migration SQL multiple times and verify consistency
	var previousSQL string
	for i := 0; i < 5; i++ {
		// Parse the directory
		database, err := goschema.ParseDir(tmpDir)
		c.Assert(err, qt.IsNil)

		// Create a schema diff for table creation
		diff := &types.SchemaDiff{
			TablesAdded: []string{"users"},
		}

		// Generate migration AST
		planner := postgres.New()
		astNodes := planner.GenerateMigrationAST(diff, database)
		c.Assert(len(astNodes), qt.Equals, 1)

		// Render to SQL
		sql, err := renderer.RenderSQL("postgresql", astNodes[0])
		c.Assert(err, qt.IsNil)

		if i == 0 {
			previousSQL = sql
		} else {
			// Verify that the SQL is identical across runs
			c.Assert(sql, qt.Equals, previousSQL, qt.Commentf("Migration SQL should be identical across runs (run %d)", i))
		}

		// Verify that fields appear in the expected order
		expectedFieldOrder := []string{"id", "email", "name", "created_at", "updated_at", "status"}
		for j, fieldName := range expectedFieldOrder {
			// Look for field name followed by space or newline
			fieldIndex := strings.Index(sql, fieldName+" ")
			if fieldIndex == -1 {
				fieldIndex = strings.Index(sql, fieldName+"\n")
			}
			if fieldIndex == -1 {
				fieldIndex = strings.Index(sql, "  "+fieldName+" ") // with indentation
			}
			c.Assert(fieldIndex, qt.Not(qt.Equals), -1, qt.Commentf("Field %s should be present in SQL", fieldName))

			// Verify that fields appear in the correct order relative to each other
			if j > 0 {
				prevFieldName := expectedFieldOrder[j-1]
				prevFieldIndex := strings.Index(sql, prevFieldName+" ")
				if prevFieldIndex == -1 {
					prevFieldIndex = strings.Index(sql, prevFieldName+"\n")
				}
				if prevFieldIndex == -1 {
					prevFieldIndex = strings.Index(sql, "  "+prevFieldName+" ")
				}
				c.Assert(fieldIndex > prevFieldIndex, qt.IsTrue,
					qt.Commentf("Field %s should appear after %s in SQL", fieldName, prevFieldName))
			}
		}
	}

	// Verify the final SQL structure
	c.Assert(previousSQL, qt.Contains, "CREATE TABLE users")
	c.Assert(previousSQL, qt.Contains, "id SERIAL PRIMARY KEY")
	c.Assert(previousSQL, qt.Contains, "email VARCHAR(255) UNIQUE")
	c.Assert(previousSQL, qt.Contains, "name VARCHAR(255)")
	c.Assert(previousSQL, qt.Contains, "created_at TIMESTAMP")
	c.Assert(previousSQL, qt.Contains, "updated_at TIMESTAMP")
	c.Assert(previousSQL, qt.Contains, "status VARCHAR(20)")

	// Debug: Print the actual SQL to understand the format
	t.Logf("Generated SQL:\n%s", previousSQL)
}

// TestFieldOrderWithEmbeddedFields tests that embedded fields maintain their position
// in the field order during migration generation.
func TestFieldOrderWithEmbeddedFields(t *testing.T) {
	c := qt.New(t)

	// Create test content with embedded fields
	testContent := `package test

type BaseEntity struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default="CURRENT_TIMESTAMP"
	CreatedAt time.Time
}

//migrator:schema:table name="posts"
type Post struct {
	//migrator:embedded mode="inline"
	BaseEntity

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="content" type="TEXT" not_null="true"
	Content string

	//migrator:schema:field name="published" type="BOOLEAN" not_null="true" default="false"
	Published bool
}`

	// Create temporary directory and file
	tmpDir := c.TempDir()
	testFile := filepath.Join(tmpDir, "post.go")
	err := os.WriteFile(testFile, []byte(testContent), 0600)
	c.Assert(err, qt.IsNil)

	// Test multiple runs for consistency
	var previousSQL string
	for i := 0; i < 3; i++ {
		// Parse the directory
		database, err := goschema.ParseDir(tmpDir)
		c.Assert(err, qt.IsNil)

		// Create a schema diff for table creation
		diff := &types.SchemaDiff{
			TablesAdded: []string{"posts"},
		}

		// Generate migration AST
		planner := postgres.New()
		astNodes := planner.GenerateMigrationAST(diff, database)
		c.Assert(len(astNodes), qt.Equals, 1)

		// Render to SQL
		sql, err := renderer.RenderSQL("postgresql", astNodes[0])
		c.Assert(err, qt.IsNil)

		if i == 0 {
			previousSQL = sql
		} else {
			c.Assert(sql, qt.Equals, previousSQL, qt.Commentf("Migration SQL should be identical across runs (run %d)", i))
		}
	}

	// Debug: Print the actual SQL to understand the format
	t.Logf("Generated SQL for embedded fields:\n%s", previousSQL)

	// Verify field order: regular fields appear first, then embedded fields are processed
	// Expected order: title, content, published, id (from BaseEntity), created_at (from BaseEntity)
	expectedFieldOrder := []string{"title", "content", "published", "id", "created_at"}
	for j, fieldName := range expectedFieldOrder {
		fieldIndex := strings.Index(previousSQL, fieldName+" ")
		if fieldIndex == -1 {
			fieldIndex = strings.Index(previousSQL, "  "+fieldName+" ") // with indentation
		}
		c.Assert(fieldIndex, qt.Not(qt.Equals), -1, qt.Commentf("Field %s should be present in SQL", fieldName))

		if j > 0 {
			prevFieldName := expectedFieldOrder[j-1]
			prevFieldIndex := strings.Index(previousSQL, prevFieldName+" ")
			if prevFieldIndex == -1 {
				prevFieldIndex = strings.Index(previousSQL, "  "+prevFieldName+" ")
			}
			c.Assert(fieldIndex > prevFieldIndex, qt.IsTrue,
				qt.Commentf("Field %s should appear after %s in SQL", fieldName, prevFieldName))
		}
	}
}

// TestFieldOrderWithMultipleTables tests field order consistency when multiple tables
// are involved in migration generation.
func TestFieldOrderWithMultipleTables(t *testing.T) {
	c := qt.New(t)

	testContent := `package test

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}

//migrator:schema:table name="posts"
type Post struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="content" type="TEXT" not_null="true"
	Content string

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64
}`

	// Create temporary directory and file
	tmpDir := c.TempDir()
	testFile := filepath.Join(tmpDir, "entities.go")
	err := os.WriteFile(testFile, []byte(testContent), 0600)
	c.Assert(err, qt.IsNil)

	// Test multiple runs for consistency
	var previousSQL []string
	for i := 0; i < 3; i++ {
		// Parse the directory
		database, err := goschema.ParseDir(tmpDir)
		c.Assert(err, qt.IsNil)

		// Create a schema diff for table creation
		diff := &types.SchemaDiff{
			TablesAdded: []string{"users", "posts"},
		}

		// Generate migration AST
		planner := postgres.New()
		astNodes := planner.GenerateMigrationAST(diff, database)
		c.Assert(len(astNodes), qt.Equals, 2) // Should have 2 CREATE TABLE statements

		// Render to SQL
		var sqlStatements []string
		for _, node := range astNodes {
			sql, err := renderer.RenderSQL("postgresql", node)
			c.Assert(err, qt.IsNil)
			sqlStatements = append(sqlStatements, sql)
		}

		if i == 0 {
			previousSQL = sqlStatements
		} else {
			c.Assert(len(sqlStatements), qt.Equals, len(previousSQL))
			for j, sql := range sqlStatements {
				c.Assert(sql, qt.Equals, previousSQL[j], 
					qt.Commentf("Migration SQL for statement %d should be identical across runs (run %d)", j, i))
			}
		}
	}

	// Verify field order in each table
	usersSQL := ""
	postsSQL := ""
	for _, sql := range previousSQL {
		if strings.Contains(sql, "CREATE TABLE users") {
			usersSQL = sql
		} else if strings.Contains(sql, "CREATE TABLE posts") {
			postsSQL = sql
		}
	}

	c.Assert(usersSQL, qt.Not(qt.Equals), "")
	c.Assert(postsSQL, qt.Not(qt.Equals), "")

	// Verify Users table field order: id, email, name
	userFields := []string{"id", "email", "name"}
	for j, fieldName := range userFields {
		fieldIndex := strings.Index(usersSQL, fieldName+" ")
		c.Assert(fieldIndex, qt.Not(qt.Equals), -1, qt.Commentf("Field %s should be present in users table", fieldName))

		if j > 0 {
			prevFieldName := userFields[j-1]
			prevFieldIndex := strings.Index(usersSQL, prevFieldName+" ")
			c.Assert(fieldIndex > prevFieldIndex, qt.IsTrue,
				qt.Commentf("Field %s should appear after %s in users table", fieldName, prevFieldName))
		}
	}

	// Verify Posts table field order: id, title, content, user_id
	postFields := []string{"id", "title", "content", "user_id"}
	for j, fieldName := range postFields {
		fieldIndex := strings.Index(postsSQL, fieldName+" ")
		c.Assert(fieldIndex, qt.Not(qt.Equals), -1, qt.Commentf("Field %s should be present in posts table", fieldName))

		if j > 0 {
			prevFieldName := postFields[j-1]
			prevFieldIndex := strings.Index(postsSQL, prevFieldName+" ")
			c.Assert(fieldIndex > prevFieldIndex, qt.IsTrue,
				qt.Commentf("Field %s should appear after %s in posts table", fieldName, prevFieldName))
		}
	}
}
