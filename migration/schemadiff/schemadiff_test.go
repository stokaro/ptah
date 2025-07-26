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
