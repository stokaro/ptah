//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/dbschema/postgres"
)

// TestPostgreSQLReader_ExtensionFunctionFiltering_Integration tests that extension-owned functions
// are properly filtered out when reading the database schema. This prevents migration generation
// from attempting to drop extension functions, which would cause failures.
func TestPostgreSQLReader_ExtensionFunctionFiltering_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// Create extensions that contain functions
	_, err = db.Exec(`CREATE EXTENSION IF NOT EXISTS btree_gin`)
	c.Assert(err, qt.IsNil)

	_, err = db.Exec(`CREATE EXTENSION IF NOT EXISTS pg_trgm`)
	c.Assert(err, qt.IsNil)

	// Create a custom user function for comparison
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION test_user_function(input_text TEXT) 
		RETURNS TEXT AS $$
		BEGIN
			RETURN 'processed: ' || input_text;
		END;
		$$ LANGUAGE plpgsql;
	`)
	c.Assert(err, qt.IsNil)

	// Clean up after test
	defer func() {
		_, _ = db.Exec("DROP FUNCTION IF EXISTS test_user_function(TEXT)")
		// Note: We don't drop extensions as they might be used by other tests
		// and dropping them would also drop their functions
	}()

	// Read schema using our reader
	reader := postgres.NewPostgreSQLReader(db, "public")
	schema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema, qt.IsNotNil)

	// Verify that extension functions are NOT included in the schema
	functionNames := make(map[string]bool)
	for _, fn := range schema.Functions {
		functionNames[fn.Name] = true
	}

	// Our custom function should be included
	c.Assert(functionNames["test_user_function"], qt.IsTrue,
		qt.Commentf("User-defined function should be included in schema"))

	// Extension functions should NOT be included
	extensionFunctions := []string{
		"gin_btree_consistent",
		"gin_extract_value_text",
		"gin_extract_query_text",
		"similarity",
		"word_similarity",
		"gin_trgm_consistent",
		"show_trgm",
	}

	for _, extFunc := range extensionFunctions {
		c.Assert(functionNames[extFunc], qt.IsFalse,
			qt.Commentf("Extension function %s should NOT be included in schema", extFunc))
	}

	// Verify that we can query extension functions directly (they exist in the database)
	// but they're filtered out by our reader
	var count int
	err = db.QueryRow(`
		SELECT COUNT(*) 
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_depend d ON d.objid = p.oid
		JOIN pg_extension e ON e.oid = d.refobjid
		WHERE n.nspname = 'public' 
		AND p.prokind = 'f'
		AND d.deptype = 'e'
		AND e.extname IN ('btree_gin', 'pg_trgm')
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Not(qt.Equals), 0,
		qt.Commentf("Extension functions should exist in database but be filtered by reader"))
}

// TestPostgreSQLReader_CustomFunctionIncluded_Integration verifies that user-defined functions
// are still properly included in the schema when extension function filtering is active.
func TestPostgreSQLReader_CustomFunctionIncluded_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// Create a custom function with various properties
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION calculate_discount(
			base_price NUMERIC,
			discount_percent NUMERIC DEFAULT 0
		) 
		RETURNS NUMERIC AS $$
		BEGIN
			RETURN base_price * (1 - discount_percent / 100);
		END;
		$$ LANGUAGE plpgsql IMMUTABLE;
	`)
	c.Assert(err, qt.IsNil)

	// Clean up after test
	defer func() {
		_, _ = db.Exec("DROP FUNCTION IF EXISTS calculate_discount(NUMERIC, NUMERIC)")
	}()

	// Read schema
	reader := postgres.NewPostgreSQLReader(db, "public")
	schema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	// Find our function
	var foundFunction bool
	for _, fn := range schema.Functions {
		if fn.Name == "calculate_discount" {
			foundFunction = true
			c.Assert(fn.Language, qt.Equals, "plpgsql")
			c.Assert(fn.Volatility, qt.Equals, "IMMUTABLE")
			c.Assert(fn.Parameters, qt.Contains, "base_price")
			c.Assert(fn.Parameters, qt.Contains, "discount_percent")
			break
		}
	}

	c.Assert(foundFunction, qt.IsTrue,
		qt.Commentf("User-defined function should be included in schema"))
}
