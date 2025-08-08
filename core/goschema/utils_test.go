package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestDeduplicate_FieldOrderPreservation(t *testing.T) {
	c := qt.New(t)

	// Test multiple runs to ensure consistent ordering

	for i := 0; i < 10; i++ {
		// Create a fresh copy for each test
		testDB := &goschema.Database{
			Tables: []goschema.Table{
				{Name: "users", StructName: "User"},
			},
			Fields: []goschema.Field{
				{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
				{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false},
				{StructName: "User", Name: "name", Type: "VARCHAR(255)", Nullable: false},
				{StructName: "User", Name: "created_at", Type: "TIMESTAMP", Nullable: false},
				// Add duplicate fields
				{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
				{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false},
			},
		}

		// Apply deduplication
		goschema.Deduplicate(testDB)

		// Verify field order is preserved
		userFields := make([]string, 0)
		for _, field := range testDB.Fields {
			if field.StructName == "User" {
				userFields = append(userFields, field.Name)
			}
		}

		// Should have exactly 4 unique fields
		c.Assert(len(userFields), qt.Equals, 4)

		// Order should be preserved: id, email, name, created_at
		expectedOrder := []string{"id", "email", "name", "created_at"}
		c.Assert(userFields, qt.DeepEquals, expectedOrder)
	}
}

func TestDeduplicate_MultipleStructsFieldOrder(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
		},
		Fields: []goschema.Field{
			// User fields
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
			// Post fields
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "title", Type: "VARCHAR(255)"},
			{StructName: "Post", Name: "content", Type: "TEXT"},
			{StructName: "Post", Name: "user_id", Type: "INTEGER"},
			// More User fields (to test interleaving)
			{StructName: "User", Name: "created_at", Type: "TIMESTAMP"},
			// Duplicates
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true}, // duplicate
			{StructName: "Post", Name: "title", Type: "VARCHAR(255)"},       // duplicate
		},
	}

	goschema.Deduplicate(db)

	// Extract fields by struct
	userFields := make([]string, 0)
	postFields := make([]string, 0)

	for _, field := range db.Fields {
		switch field.StructName {
		case "User":
			userFields = append(userFields, field.Name)
		case "Post":
			postFields = append(postFields, field.Name)
		}
	}

	// Verify User fields maintain order: id, email, name, created_at
	expectedUserOrder := []string{"id", "email", "name", "created_at"}
	c.Assert(userFields, qt.DeepEquals, expectedUserOrder)

	// Verify Post fields maintain order: id, title, content, user_id
	expectedPostOrder := []string{"id", "title", "content", "user_id"}
	c.Assert(postFields, qt.DeepEquals, expectedPostOrder)
}

func TestDeduplicate_IndexOrderPreservation(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Indexes: []goschema.Index{
			{StructName: "User", Name: "idx_email", Fields: []string{"email"}},
			{StructName: "User", Name: "idx_name", Fields: []string{"name"}},
			{StructName: "Post", Name: "idx_title", Fields: []string{"title"}},
			{StructName: "User", Name: "idx_created_at", Fields: []string{"created_at"}},
			// Duplicates
			{StructName: "User", Name: "idx_email", Fields: []string{"email"}},
			{StructName: "Post", Name: "idx_title", Fields: []string{"title"}},
		},
	}

	goschema.Deduplicate(db)

	// Extract index names in order
	indexNames := make([]string, 0)
	for _, index := range db.Indexes {
		indexNames = append(indexNames, index.Name)
	}

	// Should preserve original order and remove duplicates
	expectedOrder := []string{"idx_email", "idx_name", "idx_title", "idx_created_at"}
	c.Assert(indexNames, qt.DeepEquals, expectedOrder)
	c.Assert(len(db.Indexes), qt.Equals, 4) // Should have 4 unique indexes
}

func TestDeduplicate_EnumOrderPreservation(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Enums: []goschema.Enum{
			{Name: "user_status", Values: []string{"active", "inactive"}},
			{Name: "post_status", Values: []string{"draft", "published"}},
			{Name: "priority", Values: []string{"low", "medium", "high"}},
			// Duplicates
			{Name: "user_status", Values: []string{"active", "inactive"}},
			{Name: "priority", Values: []string{"low", "medium", "high"}},
		},
	}

	goschema.Deduplicate(db)

	// Extract enum names in order
	enumNames := make([]string, 0)
	for _, enum := range db.Enums {
		enumNames = append(enumNames, enum.Name)
	}

	// Should preserve original order and remove duplicates
	expectedOrder := []string{"user_status", "post_status", "priority"}
	c.Assert(enumNames, qt.DeepEquals, expectedOrder)
	c.Assert(len(db.Enums), qt.Equals, 3) // Should have 3 unique enums
}

func TestDeduplicate_TableOrderPreservation(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
			{Name: "comments", StructName: "Comment"},
			{Name: "categories", StructName: "Category"},
			// Duplicates
			{Name: "users", StructName: "User"},
			{Name: "comments", StructName: "Comment"},
		},
	}

	goschema.Deduplicate(db)

	// Extract table names in order
	tableNames := make([]string, 0)
	for _, table := range db.Tables {
		tableNames = append(tableNames, table.Name)
	}

	// Should preserve original order and remove duplicates
	expectedOrder := []string{"users", "posts", "comments", "categories"}
	c.Assert(tableNames, qt.DeepEquals, expectedOrder)
	c.Assert(len(db.Tables), qt.Equals, 4) // Should have 4 unique tables
}

func TestDeduplicate_EmbeddedFieldOrderPreservation(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "BaseEntity", Mode: "inline"},
			{StructName: "User", EmbeddedTypeName: "Timestamps", Mode: "inline"},
			{StructName: "Post", EmbeddedTypeName: "BaseEntity", Mode: "inline"},
			{StructName: "User", EmbeddedTypeName: "SoftDelete", Mode: "inline"},
			// Duplicates
			{StructName: "User", EmbeddedTypeName: "BaseEntity", Mode: "inline"},
			{StructName: "Post", EmbeddedTypeName: "BaseEntity", Mode: "inline"},
		},
	}

	goschema.Deduplicate(db)

	// Extract embedded field names in order
	embeddedKeys := make([]string, 0)
	for _, embedded := range db.EmbeddedFields {
		key := embedded.StructName + "." + embedded.EmbeddedTypeName
		embeddedKeys = append(embeddedKeys, key)
	}

	// Should preserve original order and remove duplicates
	expectedOrder := []string{"User.BaseEntity", "User.Timestamps", "Post.BaseEntity", "User.SoftDelete"}
	c.Assert(embeddedKeys, qt.DeepEquals, expectedOrder)
	c.Assert(len(db.EmbeddedFields), qt.Equals, 4) // Should have 4 unique embedded fields
}

func TestDeduplicate_ComplexScenarioWithAllTypes(t *testing.T) {
	c := qt.New(t)

	// Test a complex scenario with all types of entities and duplicates
	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
			{Name: "users", StructName: "User"}, // duplicate
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "Post", Name: "title", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
			{StructName: "Post", Name: "content", Type: "TEXT"},
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true}, // duplicate
		},
		Indexes: []goschema.Index{
			{StructName: "User", Name: "idx_email", Fields: []string{"email"}},
			{StructName: "Post", Name: "idx_title", Fields: []string{"title"}},
			{StructName: "User", Name: "idx_name", Fields: []string{"name"}},
			{StructName: "User", Name: "idx_email", Fields: []string{"email"}}, // duplicate
		},
		Enums: []goschema.Enum{
			{Name: "status", Values: []string{"active", "inactive"}},
			{Name: "priority", Values: []string{"low", "high"}},
			{Name: "status", Values: []string{"active", "inactive"}}, // duplicate
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "BaseEntity", Mode: "inline"},
			{StructName: "Post", EmbeddedTypeName: "BaseEntity", Mode: "inline"},
			{StructName: "User", EmbeddedTypeName: "BaseEntity", Mode: "inline"}, // duplicate
		},
	}

	goschema.Deduplicate(db)

	// Verify all collections maintain order and remove duplicates

	// Tables
	tableNames := make([]string, 0)
	for _, table := range db.Tables {
		tableNames = append(tableNames, table.Name)
	}
	c.Assert(tableNames, qt.DeepEquals, []string{"users", "posts"})
	c.Assert(len(db.Tables), qt.Equals, 2)

	// Fields - should maintain interleaved order
	fieldKeys := make([]string, 0)
	for _, field := range db.Fields {
		key := field.StructName + "." + field.Name
		fieldKeys = append(fieldKeys, key)
	}
	expectedFieldOrder := []string{"User.id", "Post.id", "User.email", "Post.title", "User.name", "Post.content"}
	c.Assert(fieldKeys, qt.DeepEquals, expectedFieldOrder)
	c.Assert(len(db.Fields), qt.Equals, 6)

	// Indexes
	indexNames := make([]string, 0)
	for _, index := range db.Indexes {
		indexNames = append(indexNames, index.Name)
	}
	c.Assert(indexNames, qt.DeepEquals, []string{"idx_email", "idx_title", "idx_name"})
	c.Assert(len(db.Indexes), qt.Equals, 3)

	// Enums
	enumNames := make([]string, 0)
	for _, enum := range db.Enums {
		enumNames = append(enumNames, enum.Name)
	}
	c.Assert(enumNames, qt.DeepEquals, []string{"status", "priority"})
	c.Assert(len(db.Enums), qt.Equals, 2)

	// Embedded Fields
	embeddedKeys := make([]string, 0)
	for _, embedded := range db.EmbeddedFields {
		key := embedded.StructName + "." + embedded.EmbeddedTypeName
		embeddedKeys = append(embeddedKeys, key)
	}
	c.Assert(embeddedKeys, qt.DeepEquals, []string{"User.BaseEntity", "Post.BaseEntity"})
	c.Assert(len(db.EmbeddedFields), qt.Equals, 2)
}
