package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
)

func TestPointerEmbeddedFields_EndToEnd(t *testing.T) {
	c := qt.New(t)

	// Create a temporary directory with all the necessary files
	tmpDir := t.TempDir()

	// Create base_id.go
	baseIDContent := `package entities

// BaseID represents a common ID structure that can be embedded in other entities
type BaseID struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
	err := os.WriteFile(filepath.Join(tmpDir, "base_id.go"), []byte(baseIDContent), 0644)
	c.Assert(err, qt.IsNil)

	// Create timestamps.go
	timestampsContent := `package entities

import "time"

// Timestamps represents common timestamp fields that can be embedded in other entities
type Timestamps struct {
	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

// AuditInfo represents audit information that can be embedded with prefix
type AuditInfo struct {
	//migrator:schema:field name="by" type="VARCHAR(255)"
	By string

	//migrator:schema:field name="reason" type="TEXT"
	Reason string
}

// Metadata represents metadata that can be embedded as JSON
type Metadata struct {
	//migrator:schema:field name="author" type="VARCHAR(255)"
	Author string

	//migrator:schema:field name="source" type="VARCHAR(255)"
	Source string
}

// SkippedInfo represents information that should be skipped in embedding
type SkippedInfo struct {
	//migrator:schema:field name="internal_data" type="TEXT"
	InternalData string
}
`
	err = os.WriteFile(filepath.Join(tmpDir, "timestamps.go"), []byte(timestampsContent), 0644)
	c.Assert(err, qt.IsNil)

	// Create user.go
	userContent := `package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:embedded mode="inline"
	BaseID

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}
`
	err = os.WriteFile(filepath.Join(tmpDir, "user.go"), []byte(userContent), 0644)
	c.Assert(err, qt.IsNil)

	// Create blog_post.go with pointer embedded fields
	blogPostContent := `package entities

// BlogPost demonstrates all embedding modes using pointer types
//migrator:schema:table name="blog_posts"
type BlogPost struct {
	// Mode 1: inline with pointer
	//migrator:embedded mode="inline"
	*BaseID

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="content" type="TEXT" not_null="true"
	Content string

	// Mode 1: inline with pointer
	//migrator:embedded mode="inline"
	*Timestamps

	// Mode 2: inline with prefix and pointer
	//migrator:embedded mode="inline" prefix="audit_"
	*AuditInfo

	// Mode 3: json with pointer
	//migrator:embedded mode="json" name="meta_data" type="JSONB"
	*Metadata

	// Mode 4: relation with pointer
	//migrator:embedded mode="relation" field="author_id" ref="users(id)" on_delete="CASCADE"
	*User

	// Mode 5: skip with pointer
	//migrator:embedded mode="skip"
	*SkippedInfo

	//migrator:schema:field name="published" type="BOOLEAN" not_null="true" default="false"
	Published bool
}
`
	err = os.WriteFile(filepath.Join(tmpDir, "blog_post.go"), []byte(blogPostContent), 0644)
	c.Assert(err, qt.IsNil)

	// Parse the directory
	database, err := goschema.ParseDir(tmpDir)
	c.Assert(err, qt.IsNil)

	// Should have 2 tables: users and blog_posts
	c.Assert(len(database.Tables), qt.Equals, 2)

	// Find the BlogPost table
	var blogPostTable *goschema.Table
	for _, table := range database.Tables {
		if table.StructName == "BlogPost" {
			blogPostTable = &table
			break
		}
	}
	c.Assert(blogPostTable, qt.IsNotNil)
	c.Assert(blogPostTable.Name, qt.Equals, "blog_posts")

	// Should have embedded fields for BlogPost
	blogPostEmbedded := 0
	for _, embedded := range database.EmbeddedFields {
		if embedded.StructName == "BlogPost" {
			blogPostEmbedded++
		}
	}
	c.Assert(blogPostEmbedded, qt.Equals, 6)

	// Generate schema
	statements := fromschema.FromDatabase(*database, "postgresql")

	// Find the BlogPost CREATE TABLE statement
	var blogPostSQL string
	for _, stmt := range statements.Statements {
		sql, err := renderer.RenderSQL("postgresql", stmt)
		c.Assert(err, qt.IsNil)
		if containsSubstr(sql, "CREATE TABLE blog_posts") {
			blogPostSQL = sql
			break
		}
	}

	c.Assert(blogPostSQL, qt.Not(qt.Equals), "")

	// Verify all embedded fields are present
	expectedColumns := []string{
		"id",           // from *BaseID
		"created_at",   // from *Timestamps
		"updated_at",   // from *Timestamps
		"audit_by",     // from *AuditInfo with prefix
		"audit_reason", // from *AuditInfo with prefix
		"meta_data",    // from *Metadata as JSON
		"author_id",    // from *User as relation
		"title",        // regular field
		"content",      // regular field
		"published",    // regular field
	}

	for _, column := range expectedColumns {
		c.Assert(containsSubstr(blogPostSQL, column), qt.IsTrue,
			qt.Commentf("BlogPost table should contain column %s. SQL: %s", column, blogPostSQL))
	}

	// Verify skipped fields are NOT present
	skippedColumns := []string{"internal_data"}
	for _, column := range skippedColumns {
		c.Assert(containsSubstr(blogPostSQL, column), qt.IsFalse,
			qt.Commentf("BlogPost table should NOT contain skipped column %s", column))
	}

	// Verify foreign key constraint is present
	c.Assert(containsSubstr(blogPostSQL, "FOREIGN KEY"), qt.IsTrue)
	c.Assert(containsSubstr(blogPostSQL, "REFERENCES users(id)"), qt.IsTrue)

	t.Logf("Generated BlogPost SQL:\n%s", blogPostSQL)
}

// containsSubstr checks if a string contains a substring
func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
