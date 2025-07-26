package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/postgres"
)

func TestPostgreSQLRenderer_VisitIndex_PostgreSQLFeatures(t *testing.T) {
	tests := []struct {
		name     string
		index    *ast.IndexNode
		expected string
	}{
		{
			name: "basic BTREE index",
			index: &ast.IndexNode{
				Name:    "idx_users_email",
				Table:   "users",
				Columns: []string{"email"},
				Unique:  false,
			},
			expected: "CREATE INDEX idx_users_email ON users (email);\n",
		},
		{
			name: "unique index",
			index: &ast.IndexNode{
				Name:    "idx_users_username",
				Table:   "users",
				Columns: []string{"username"},
				Unique:  true,
			},
			expected: "CREATE UNIQUE INDEX idx_users_username ON users (username);\n",
		},
		{
			name: "GIN index",
			index: &ast.IndexNode{
				Name:    "idx_products_tags",
				Table:   "products",
				Columns: []string{"tags"},
				Type:    "GIN",
			},
			expected: "CREATE INDEX idx_products_tags ON products USING GIN (tags);\n",
		},
		{
			name: "partial index",
			index: &ast.IndexNode{
				Name:      "idx_active_users",
				Table:     "users",
				Columns:   []string{"status"},
				Condition: "deleted_at IS NULL",
			},
			expected: "CREATE INDEX idx_active_users ON users (status) WHERE deleted_at IS NULL;\n",
		},
		{
			name: "trigram index",
			index: &ast.IndexNode{
				Name:     "idx_users_name_trgm",
				Table:    "users",
				Columns:  []string{"name"},
				Type:     "GIN",
				Operator: "gin_trgm_ops",
			},
			expected: "CREATE INDEX idx_users_name_trgm ON users USING GIN (name gin_trgm_ops);\n",
		},
		{
			name: "composite GIN index with condition",
			index: &ast.IndexNode{
				Name:      "idx_products_search",
				Table:     "products",
				Columns:   []string{"name", "tags"},
				Type:      "GIN",
				Condition: "status = 'active'",
			},
			expected: "CREATE INDEX idx_products_search ON products USING GIN (name, tags) WHERE status = 'active';\n",
		},
		{
			name: "complex index with all features",
			index: &ast.IndexNode{
				Name:      "idx_complex",
				Table:     "products",
				Columns:   []string{"name", "description"},
				Type:      "GIN",
				Operator:  "gin_trgm_ops",
				Condition: "status = 'published' AND deleted_at IS NULL",
				Unique:    false,
			},
			expected: "CREATE INDEX idx_complex ON products USING GIN (name gin_trgm_ops, description gin_trgm_ops) WHERE status = 'published' AND deleted_at IS NULL;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.index)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_VisitExtension(t *testing.T) {
	tests := []struct {
		name      string
		extension *ast.ExtensionNode
		expected  string
	}{
		{
			name: "basic extension",
			extension: &ast.ExtensionNode{
				Name: "pg_trgm",
			},
			expected: "CREATE EXTENSION pg_trgm;\n",
		},
		{
			name: "extension with IF NOT EXISTS",
			extension: &ast.ExtensionNode{
				Name:        "pg_trgm",
				IfNotExists: true,
			},
			expected: "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n",
		},
		{
			name: "extension with version",
			extension: &ast.ExtensionNode{
				Name:    "postgis",
				Version: "3.0",
			},
			expected: "CREATE EXTENSION postgis VERSION '3.0';\n",
		},
		{
			name: "extension with comment",
			extension: &ast.ExtensionNode{
				Name:    "btree_gin",
				Comment: "Enable GIN indexes on btree types",
			},
			expected: "-- Enable GIN indexes on btree types\nCREATE EXTENSION btree_gin;\n",
		},
		{
			name: "extension with all features",
			extension: &ast.ExtensionNode{
				Name:        "postgis",
				IfNotExists: true,
				Version:     "3.0",
				Comment:     "Geographic data support",
			},
			expected: "-- Geographic data support\nCREATE EXTENSION IF NOT EXISTS postgis VERSION '3.0';\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			sql, err := renderer.Render(tt.extension)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.expected)
		})
	}
}

func TestPostgreSQLRenderer_CompleteSchema(t *testing.T) {
	c := qt.New(t)

	// Create a complete schema with extensions, tables, and indexes
	renderer := postgres.New()

	// Extension
	extension := &ast.ExtensionNode{
		Name:        "pg_trgm",
		IfNotExists: true,
		Comment:     "Enable trigram similarity search",
	}

	// Table
	table := ast.NewCreateTable("products").
		AddColumn(
			ast.NewColumn("id", "SERIAL").
				SetPrimary().
				SetNotNull(),
		).
		AddColumn(
			ast.NewColumn("name", "VARCHAR(255)").
				SetNotNull(),
		).
		AddColumn(
			ast.NewColumn("tags", "JSONB"),
		).
		AddColumn(
			ast.NewColumn("status", "VARCHAR(50)"),
		).
		AddColumn(
			ast.NewColumn("deleted_at", "TIMESTAMP"),
		)

	// Indexes
	ginIndex := &ast.IndexNode{
		Name:    "idx_products_tags",
		Table:   "products",
		Columns: []string{"tags"},
		Type:    "GIN",
	}

	partialIndex := &ast.IndexNode{
		Name:      "idx_active_products",
		Table:     "products",
		Columns:   []string{"status"},
		Condition: "deleted_at IS NULL",
	}

	trigramIndex := &ast.IndexNode{
		Name:     "idx_products_name_trgm",
		Table:    "products",
		Columns:  []string{"name"},
		Type:     "GIN",
		Operator: "gin_trgm_ops",
	}

	// Render each component
	extensionSQL, err := renderer.Render(extension)
	c.Assert(err, qt.IsNil)

	tableSQL, err := renderer.Render(table)
	c.Assert(err, qt.IsNil)

	ginIndexSQL, err := renderer.Render(ginIndex)
	c.Assert(err, qt.IsNil)

	partialIndexSQL, err := renderer.Render(partialIndex)
	c.Assert(err, qt.IsNil)

	trigramIndexSQL, err := renderer.Render(trigramIndex)
	c.Assert(err, qt.IsNil)

	// Verify the generated SQL
	c.Assert(extensionSQL, qt.Contains, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	c.Assert(extensionSQL, qt.Contains, "-- Enable trigram similarity search")

	c.Assert(tableSQL, qt.Contains, "CREATE TABLE products")
	c.Assert(tableSQL, qt.Contains, "id SERIAL PRIMARY KEY NOT NULL")
	c.Assert(tableSQL, qt.Contains, "tags JSONB")

	c.Assert(ginIndexSQL, qt.Equals, "CREATE INDEX idx_products_tags ON products USING GIN (tags);\n")
	c.Assert(partialIndexSQL, qt.Equals, "CREATE INDEX idx_active_products ON products (status) WHERE deleted_at IS NULL;\n")
	c.Assert(trigramIndexSQL, qt.Equals, "CREATE INDEX idx_products_name_trgm ON products USING GIN (name gin_trgm_ops);\n")
}
