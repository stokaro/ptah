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

	// Setup test data - use an extension that's not in the default ignore list
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "uuid-ossp", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "btree_gin", Version: "1.3", Schema: "public"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}

	// Test default behavior (should ignore plpgsql, btree_gin, pg_trgm)
	diff := schemadiff.Compare(generated, database)

	// Default ignored extensions should not be removed, uuid-ossp should be added
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"uuid-ossp"})
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

	// Setup test data - use an extension that's not in the default ignore list
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "uuid-ossp", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "btree_gin", Version: "1.3", Schema: "public"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
			{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
		},
	}

	// Test with additional ignored extensions (default + adminpack)
	opts := config.WithAdditionalIgnoredExtensions("adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, opts)

	// plpgsql, btree_gin, pg_trgm, and adminpack should be ignored, only pg_stat_statements should be removed
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"uuid-ossp"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pg_stat_statements"})
}

func TestCompareWithOptions_NilOptions(t *testing.T) {
	c := qt.New(t)

	// Setup test data - use an extension that's not in the default ignore list
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "uuid-ossp", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "btree_gin", Version: "1.3", Schema: "public"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}

	// Test with nil options (should use defaults)
	diff := schemadiff.CompareWithOptions(generated, database, nil)

	// Should behave the same as Compare() - ignore default extensions
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"uuid-ossp"})
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{})
}

func TestLibraryUsageExamples(t *testing.T) {
	c := qt.New(t)

	// Example data - use extensions that are not in the default ignore list
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "uuid-ossp", IfNotExists: true},
			{Name: "hstore", IfNotExists: true},
		},
	}
	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "btree_gin", Version: "1.3", Schema: "public"},
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
			{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
		},
	}

	t.Run("simple usage with defaults", func(t *testing.T) {
		// Most common usage - just compare with defaults
		diff := schemadiff.Compare(generated, database)

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"hstore"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{}) // default extensions ignored
	})

	t.Run("custom ignore list", func(t *testing.T) {
		// User wants to ignore specific extensions only (not the defaults)
		opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		// hstore should be added, btree_gin and pg_trgm should be removed (not ignored in custom list)
		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"hstore"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"btree_gin", "pg_trgm"})
	})

	t.Run("manage all extensions", func(t *testing.T) {
		// User wants to manage all extensions (no ignoring)
		opts := config.WithIgnoredExtensions()
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		// hstore should be added, btree_gin and pg_trgm should be removed, plpgsql should be removed
		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"hstore"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"btree_gin", "pg_trgm", "plpgsql"})
	})

	t.Run("add to default ignore list", func(t *testing.T) {
		// User wants defaults plus additional ignored extensions
		opts := config.WithAdditionalIgnoredExtensions("uuid-ossp")
		diff := schemadiff.CompareWithOptions(generated, database, opts)

		// hstore should be added, uuid-ossp is now ignored so it won't be removed
		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"hstore"})
		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{}) // all extensions in database are ignored
	})
}
