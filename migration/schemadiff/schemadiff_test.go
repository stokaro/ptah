package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestCompare_DefaultBehavior(t *testing.T) {
	c := qt.New(t)

	// Setup test data with plpgsql in database but not in generated schema
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
		},
	}

	// Test default behavior (should ignore plpgsql)
	diff := schemadiff.Compare(generated, database)

	// plpgsql should be ignored by default, so no extensions should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
}

func TestCompareWithDialect_MySQLFamilyInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{
				Tables: []goschema.Table{{
					Name:       "products",
					StructName: "Product",
				}},
				Fields: []goschema.Field{
					{StructName: "Product", Name: "id", Type: "int", Primary: true},
					{
						StructName: "Product",
						Name:       "status",
						Type:       "enum_product_status",
						Enum:       []string{"draft", "active"},
						Nullable:   false,
					},
				},
				Enums: []goschema.Enum{{
					Name:   "enum_product_status",
					Values: []string{"draft", "active"},
				}},
			}
			database := &types.DBSchema{
				Tables: []types.DBTable{{
					Name: "products",
					Type: "TABLE",
					Columns: []types.DBColumn{
						{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
						{Name: "status", DataType: "enum('draft','active')", IsNullable: "NO"},
					},
				}},
				Enums: []types.DBEnum{{
					Name:   "enum_draft_active",
					Values: []string{"draft", "active"},
				}},
			}

			diff := schemadiff.CompareWithDialect(generated, database, dialect)
			c.Assert(diff.EnumsAdded, qt.HasLen, 0)
			c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_GeneratedColumnDefaultKindMatchesDialect(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		databaseKind string
	}{
		{name: "postgres", dialect: "postgres", databaseKind: "STORED"},
		{name: "mysql", dialect: "mysql", databaseKind: "VIRTUAL"},
		{name: "mariadb", dialect: "mariadb", databaseKind: "VIRTUAL"},
		{name: "sqlite", dialect: "sqlite", databaseKind: "VIRTUAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			expression := "lower(email)"
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "users", StructName: "User"}},
				Fields: []goschema.Field{
					{StructName: "User", Name: "email_lc", Type: "TEXT", Nullable: true, GeneratedExpression: expression},
				},
			}
			database := &types.DBSchema{
				Tables: []types.DBTable{{
					Name: "users",
					Type: "TABLE",
					Columns: []types.DBColumn{{
						Name:                "email_lc",
						DataType:            "TEXT",
						IsNullable:          "YES",
						GeneratedExpression: &expression,
						GeneratedKind:       tt.databaseKind,
					}},
				}},
			}

			diff := schemadiff.CompareWithDialect(generated, database, tt.dialect)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

func TestCompareWithDialect_SQLiteInlineEnumsMatchGeneratedEnumFields(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			Name:       "products",
			StructName: "Product",
		}},
		Fields: []goschema.Field{
			{StructName: "Product", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Product",
				Name:       "status",
				Type:       "enum_product_status",
				Enum:       []string{"draft", "active"},
				Nullable:   false,
			},
		},
		Enums: []goschema.Enum{{
			Name:   "enum_product_status",
			Values: []string{"draft", "active"},
		}},
	}
	check := "status IN ('draft', 'active')"
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "products",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "status", DataType: "TEXT", IsNullable: "NO"},
			},
		}},
		Constraints: []types.DBConstraint{{
			Name:        "products_status_check",
			TableName:   "products",
			Type:        "CHECK",
			CheckClause: &check,
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "sqlite")
	c.Assert(diff.EnumsAdded, qt.HasLen, 0)
	c.Assert(diff.EnumsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func TestCompareWithOptions_CustomIgnoreList(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test with custom ignore list (ignore adminpack but not plpgsql)
	opts := config.WithIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// adminpack should be ignored, plpgsql should be marked for removal
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"plpgsql"})
}

func TestCompareWithOptions_NoIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test with no ignored extensions (manage all extensions)
	opts := config.WithIgnoredExtensions() // Empty list
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// All database extensions should be marked for removal
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"adminpack", "plpgsql"})
}

func TestCompareWithOptions_AdditionalIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
			{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
		},
	}

	// Test with additional ignored extensions (default + adminpack)
	opts := config.WithAdditionalIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// plpgsql and adminpack should be ignored, only pg_stat_statements should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pg_stat_statements"})
}

func TestCompareWithOptions_NilOptions(t *testing.T) {
	c := qt.New(t)

	// Setup test data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
		},
	}

	// Test with nil options (should use defaults)
	diff := schemadiff.CompareWithOptions(generated, database, nil)

	// Should behave the same as Compare() - ignore plpgsql by default
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
}

func TestLibraryUsageExamples(t *testing.T) {
	c := qt.New(t)

	// Example data
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
			{Name: "btree_gin", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}

	t.Run("simple usage with defaults", func(t *testing.T) {
		// Most common usage - just compare with defaults
		diff := schemadiff.Compare(generated, database)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{}) // plpgsql ignored
	})

	t.Run("custom ignore list", func(t *testing.T) {
		// User wants to ignore specific extensions
		opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
	})

	t.Run("manage all extensions", func(t *testing.T) {
		// User wants to manage all extensions (no ignoring)
		opts := config.WithIgnoredExtensions()
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"plpgsql"})
	})

	t.Run("add to default ignore list", func(t *testing.T) {
		// User wants defaults plus additional ignored extensions
		opts := config.WithAdditionalIgnoredExtensions("uuid-ossp")
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"btree_gin"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{}) // plpgsql still ignored
	})
}
