package goschema_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestParseExtensionComment(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected goschema.Extension
	}{
		{
			name:    "basic extension",
			comment: "//migrator:schema:extension name=\"pg_trgm\"",
			expected: goschema.Extension{
				Name:        "pg_trgm",
				IfNotExists: false,
				Version:     "",
				Comment:     "",
			},
		},
		{
			name:    "extension with if_not_exists",
			comment: "//migrator:schema:extension name=\"pg_trgm\" if_not_exists=\"true\"",
			expected: goschema.Extension{
				Name:        "pg_trgm",
				IfNotExists: true,
				Version:     "",
				Comment:     "",
			},
		},
		{
			name:    "extension with version",
			comment: "//migrator:schema:extension name=\"postgis\" version=\"3.0\" if_not_exists=\"true\"",
			expected: goschema.Extension{
				Name:        "postgis",
				IfNotExists: true,
				Version:     "3.0",
				Comment:     "",
			},
		},
		{
			name:    "extension with comment",
			comment: "//migrator:schema:extension name=\"btree_gin\" comment=\"Enable GIN indexes on btree types\"",
			expected: goschema.Extension{
				Name:        "btree_gin",
				IfNotExists: false,
				Version:     "",
				Comment:     "Enable GIN indexes on btree types",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create a temporary Go file with the extension annotation
			content := `package test

` + tt.comment + `
type TestExtensions struct{}
`

			// Write to a temporary file and parse it
			database := parseStringAsGoFile(c, content)

			c.Assert(len(database.Extensions), qt.Equals, 1)
			ext := database.Extensions[0]
			c.Assert(ext.Name, qt.Equals, tt.expected.Name)
			c.Assert(ext.IfNotExists, qt.Equals, tt.expected.IfNotExists)
			c.Assert(ext.Version, qt.Equals, tt.expected.Version)
			c.Assert(ext.Comment, qt.Equals, tt.expected.Comment)
		})
	}
}

func TestParseIndexWithPostgreSQLFeatures(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected goschema.Index
	}{
		{
			name:    "GIN index",
			comment: "//migrator:schema:index name=\"idx_tags\" fields=\"tags\" type=\"GIN\"",
			expected: goschema.Index{
				Name:      "idx_tags",
				Fields:    []string{"tags"},
				Type:      "GIN",
				Condition: "",
				Operator:  "",
				TableName: "",
			},
		},
		{
			name:    "partial index",
			comment: "//migrator:schema:index name=\"idx_active\" fields=\"status\" condition=\"deleted_at IS NULL\"",
			expected: goschema.Index{
				Name:      "idx_active",
				Fields:    []string{"status"},
				Type:      "",
				Condition: "deleted_at IS NULL",
				Operator:  "",
				TableName: "",
			},
		},
		{
			name:    "trigram index",
			comment: "//migrator:schema:index name=\"idx_name_trgm\" fields=\"name\" type=\"GIN\" ops=\"gin_trgm_ops\"",
			expected: goschema.Index{
				Name:      "idx_name_trgm",
				Fields:    []string{"name"},
				Type:      "GIN",
				Condition: "",
				Operator:  "gin_trgm_ops",
				TableName: "",
			},
		},
		{
			name:    "cross-table index",
			comment: "//migrator:schema:index name=\"idx_external\" fields=\"name,status\" table=\"products\"",
			expected: goschema.Index{
				Name:      "idx_external",
				Fields:    []string{"name", "status"},
				Type:      "",
				Condition: "",
				Operator:  "",
				TableName: "products",
			},
		},
		{
			name:    "complex index with all features",
			comment: "//migrator:schema:index name=\"idx_complex\" fields=\"name,tags\" type=\"GIN\" condition=\"status = 'active'\" table=\"products\"",
			expected: goschema.Index{
				Name:      "idx_complex",
				Fields:    []string{"name", "tags"},
				Type:      "GIN",
				Condition: "status = 'active'",
				Operator:  "",
				TableName: "products",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create a temporary Go file with the index annotation
			content := `package test

type TestEntity struct {
	` + tt.comment + `
	_ int
}
`

			// Write to a temporary file and parse it
			database := parseStringAsGoFile(c, content)

			c.Assert(len(database.Indexes), qt.Equals, 1)
			idx := database.Indexes[0]
			c.Assert(idx.Name, qt.Equals, tt.expected.Name)
			c.Assert(idx.Fields, qt.DeepEquals, tt.expected.Fields)
			c.Assert(idx.Type, qt.Equals, tt.expected.Type)
			c.Assert(idx.Condition, qt.Equals, tt.expected.Condition)
			c.Assert(idx.Operator, qt.Equals, tt.expected.Operator)
			c.Assert(idx.TableName, qt.Equals, tt.expected.TableName)
		})
	}
}

func TestParseMultipleExtensions(t *testing.T) {
	c := qt.New(t)

	content := `package test

//migrator:schema:extension name="pg_trgm" if_not_exists="true"
//migrator:schema:extension name="btree_gin" if_not_exists="true"
//migrator:schema:extension name="postgis" version="3.0"
type DatabaseExtensions struct{}
`

	database := parseStringAsGoFile(c, content)

	c.Assert(len(database.Extensions), qt.Equals, 3)

	// Check pg_trgm
	c.Assert(database.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(database.Extensions[0].IfNotExists, qt.IsTrue)

	// Check btree_gin
	c.Assert(database.Extensions[1].Name, qt.Equals, "btree_gin")
	c.Assert(database.Extensions[1].IfNotExists, qt.IsTrue)

	// Check postgis
	c.Assert(database.Extensions[2].Name, qt.Equals, "postgis")
	c.Assert(database.Extensions[2].Version, qt.Equals, "3.0")
}

func TestParseCompletePostgreSQLSchema(t *testing.T) {
	c := qt.New(t)

	content := `package test

//migrator:schema:extension name="pg_trgm" if_not_exists="true"
//migrator:schema:extension name="btree_gin" if_not_exists="true"
type DatabaseExtensions struct{}

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="tags" type="JSONB"
	Tags []string

	//migrator:schema:field name="status" type="VARCHAR(50)"
	Status string

	// GIN index for JSONB field
	//migrator:schema:index name="idx_product_tags" fields="tags" type="GIN"
	_ int

	// Partial index with condition
	//migrator:schema:index name="idx_active_products" fields="status" condition="deleted_at IS NULL"
	_ int

	// Trigram similarity index
	//migrator:schema:index name="idx_product_name_trgm" fields="name" type="GIN" ops="gin_trgm_ops"
	_ int
}
`

	database := parseStringAsGoFile(c, content)

	// Check extensions
	c.Assert(len(database.Extensions), qt.Equals, 2)
	c.Assert(database.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(database.Extensions[1].Name, qt.Equals, "btree_gin")

	// Check tables
	c.Assert(len(database.Tables), qt.Equals, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "products")
	c.Assert(database.Tables[0].StructName, qt.Equals, "Product")

	// Check fields
	c.Assert(len(database.Fields), qt.Equals, 4)

	// Check indexes
	c.Assert(len(database.Indexes), qt.Equals, 3)

	// Check GIN index
	ginIndex := database.Indexes[0]
	c.Assert(ginIndex.Name, qt.Equals, "idx_product_tags")
	c.Assert(ginIndex.Type, qt.Equals, "GIN")
	c.Assert(ginIndex.Fields, qt.DeepEquals, []string{"tags"})

	// Check partial index
	partialIndex := database.Indexes[1]
	c.Assert(partialIndex.Name, qt.Equals, "idx_active_products")
	c.Assert(partialIndex.Condition, qt.Equals, "deleted_at IS NULL")

	// Check trigram index
	trigramIndex := database.Indexes[2]
	c.Assert(trigramIndex.Name, qt.Equals, "idx_product_name_trgm")
	c.Assert(trigramIndex.Type, qt.Equals, "GIN")
	c.Assert(trigramIndex.Operator, qt.Equals, "gin_trgm_ops")
}

// Helper function to parse a string as a Go file
func parseStringAsGoFile(c *qt.C, content string) goschema.Database {
	// Write content to a temporary file
	tmpFile := c.TempDir() + "/test.go"
	err := writeFile(tmpFile, content)
	c.Assert(err, qt.IsNil)

	// Parse the file
	return goschema.ParseFile(tmpFile)
}

// Helper function to write content to a file
func writeFile(filename, content string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}
