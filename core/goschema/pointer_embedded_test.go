package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestParseFile_PointerEmbeddedFields(t *testing.T) {
	c := qt.New(t)

	// Create a test file with pointer embedded fields
	content := `package entities

// BaseID represents a common ID structure that can be embedded in other entities
type BaseID struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

// Timestamps represents common timestamp fields that can be embedded in other entities
type Timestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

// BlogPost demonstrates pointer embedded fields
//ptah:schema:table name="blog_posts"
type BlogPost struct {
	// Pointer embedded field - should be parsed correctly
	//ptah:embedded mode="inline"
	*BaseID

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	// Another pointer embedded field
	//ptah:embedded mode="inline"
	*Timestamps
}

// RegularPost demonstrates value embedded fields for comparison
//ptah:schema:table name="regular_posts"
type RegularPost struct {
	// Value embedded field - should be parsed correctly
	//ptah:embedded mode="inline"
	BaseID

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	// Another value embedded field
	//ptah:embedded mode="inline"
	Timestamps
}
`

	// Write to temporary file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "pointer_embedded.go")
	err := os.WriteFile(testFile, []byte(content), 0600)
	c.Assert(err, qt.IsNil)

	// Parse the file
	database := mustParseFile(c, testFile)

	// Should have 2 tables
	c.Assert(database.Tables, qt.HasLen, 2)

	// Should have embedded fields for both tables
	c.Assert(database.EmbeddedFields, qt.HasLen, 4)

	// Check that pointer embedded fields are parsed correctly
	var blogPostEmbedded []goschema.EmbeddedField
	var regularPostEmbedded []goschema.EmbeddedField

	for _, embedded := range database.EmbeddedFields {
		switch embedded.StructName {
		case "BlogPost":
			blogPostEmbedded = append(blogPostEmbedded, embedded)
		case "RegularPost":
			regularPostEmbedded = append(regularPostEmbedded, embedded)
		}
	}

	// Both should have 2 embedded fields each
	c.Assert(blogPostEmbedded, qt.HasLen, 2)
	c.Assert(regularPostEmbedded, qt.HasLen, 2)

	// Check that pointer embedded fields have correct EmbeddedTypeName
	for _, embedded := range blogPostEmbedded {
		switch embedded.EmbeddedTypeName {
		case "BaseID":
			c.Assert(embedded.Mode, qt.Equals, "inline")
		case "Timestamps":
			c.Assert(embedded.Mode, qt.Equals, "inline")
		default:
			t.Errorf("Unexpected embedded type name: %s", embedded.EmbeddedTypeName)
		}
	}

	// Check that value embedded fields have correct EmbeddedTypeName
	for _, embedded := range regularPostEmbedded {
		switch embedded.EmbeddedTypeName {
		case "BaseID":
			c.Assert(embedded.Mode, qt.Equals, "inline")
		case "Timestamps":
			c.Assert(embedded.Mode, qt.Equals, "inline")
		default:
			t.Errorf("Unexpected embedded type name: %s", embedded.EmbeddedTypeName)
		}
	}

	// Both pointer and value embedded fields should generate the same EmbeddedTypeName
	// This ensures they will be processed identically by the schema generator
	blogPostTypes := make(map[string]bool)
	regularPostTypes := make(map[string]bool)

	for _, embedded := range blogPostEmbedded {
		blogPostTypes[embedded.EmbeddedTypeName] = true
	}

	for _, embedded := range regularPostEmbedded {
		regularPostTypes[embedded.EmbeddedTypeName] = true
	}

	// Should have the same embedded type names
	c.Assert(blogPostTypes, qt.DeepEquals, regularPostTypes)
}
