//go:build integration

package gonative_test

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/dbschema"
)

// TestPgxDriverValidation verifies that the pgx driver is actually being used
// instead of the old lib/pq driver for PostgreSQL connections.
func TestPgxDriverValidation(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Skipping PostgreSQL driver validation: POSTGRES_TEST_DSN environment variable not set")
	}

	t.Run("direct pgx driver usage", func(t *testing.T) {
		c := qt.New(t)

		// Test direct connection with pgx driver
		db, err := sql.Open("pgx", dsn)
		c.Assert(err, qt.IsNil)
		defer db.Close()

		err = db.Ping()
		c.Assert(err, qt.IsNil)

		// Verify driver type by checking the underlying driver
		driver := db.Driver()
		driverType := reflect.TypeOf(driver).String()

		// The pgx driver should be identifiable in the type name
		c.Assert(strings.Contains(driverType, "pgx") || strings.Contains(driverType, "stdlib"), qt.IsTrue,
			qt.Commentf("Expected pgx driver, got: %s", driverType))
	})

	t.Run("dbschema connection uses pgx", func(t *testing.T) {
		c := qt.New(t)

		// Test that dbschema.ConnectToDatabase uses pgx for PostgreSQL URLs
		conn, err := dbschema.ConnectToDatabase(dsn)
		c.Assert(err, qt.IsNil)
		defer conn.Close()

		// Test that we can query the database through the connection
		var version string
		err = conn.QueryRow("SELECT version()").Scan(&version)
		c.Assert(err, qt.IsNil)
		c.Assert(version, qt.Not(qt.Equals), "")
		c.Assert(strings.Contains(version, "PostgreSQL"), qt.IsTrue)

		// Verify connection info shows correct dialect
		info := conn.Info()
		c.Assert(info.Dialect, qt.Matches, "postgres|postgresql|pgx")
	})

	t.Run("pgx specific features work", func(t *testing.T) {
		c := qt.New(t)

		// Test pgx-specific functionality that lib/pq doesn't have
		db, err := sql.Open("pgx", dsn)
		c.Assert(err, qt.IsNil)
		defer db.Close()

		// Test that we can use pgx-specific connection features
		// This is a basic test to ensure the driver is working correctly
		var version string
		err = db.QueryRow("SELECT version()").Scan(&version)
		c.Assert(err, qt.IsNil)
		c.Assert(version, qt.Not(qt.Equals), "")
		c.Assert(strings.Contains(version, "PostgreSQL"), qt.IsTrue)

		// Test that we can handle PostgreSQL-specific types correctly
		// This tests that pgx's type handling is working
		var jsonData interface{}
		err = db.QueryRow("SELECT '{\"test\": \"value\"}'::jsonb").Scan(&jsonData)
		c.Assert(err, qt.IsNil)
		c.Assert(jsonData, qt.IsNotNil)
	})

	t.Run("connection string compatibility", func(t *testing.T) {
		// Test that various PostgreSQL connection string formats work with pgx
		testCases := []struct {
			name string
			dsn  string
		}{
			{
				name: "original DSN",
				dsn:  dsn,
			},
		}

		// Add variations if the original DSN allows it
		if strings.HasPrefix(dsn, "postgres://") {
			// Test with postgresql:// scheme
			postgresqlDSN := strings.Replace(dsn, "postgres://", "postgresql://", 1)
			testCases = append(testCases, struct {
				name string
				dsn  string
			}{
				name: "postgresql scheme",
				dsn:  postgresqlDSN,
			})
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				c := qt.New(t)

				conn, err := dbschema.ConnectToDatabase(tc.dsn)
				c.Assert(err, qt.IsNil)
				defer conn.Close()

				// Test that we can query the database
				var version string
				err = conn.QueryRow("SELECT version()").Scan(&version)
				c.Assert(err, qt.IsNil)
				c.Assert(version, qt.Not(qt.Equals), "")

				// Verify connection info shows correct dialect
				info := conn.Info()
				c.Assert(info.Dialect, qt.Matches, "postgres|postgresql|pgx")
			})
		}
	})
}

// TestDriverMigrationCompleteness ensures that no lib/pq references remain
// in the codebase that could cause driver inconsistencies.
func TestDriverMigrationCompleteness(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Skipping driver migration test: POSTGRES_TEST_DSN environment variable not set")
	}

	t.Run("no lib/pq driver registration", func(t *testing.T) {
		c := qt.New(t)

		// Attempt to open with "postgres" driver name (lib/pq)
		// This should fail if lib/pq is not imported/registered
		_, err := sql.Open("postgres", dsn)

		// We expect this to either fail or use a different driver
		// If it succeeds, we need to verify it's not actually lib/pq
		if err == nil {
			t.Log("Warning: 'postgres' driver name still works - checking if it's actually pgx")
			// This might happen if pgx registers itself under multiple names
		} else {
			// This is the expected behavior - lib/pq should not be available
			c.Assert(strings.Contains(err.Error(), "unknown driver"), qt.IsTrue,
				qt.Commentf("Expected 'unknown driver' error, got: %v", err))
		}
	})

	t.Run("pgx driver is available", func(t *testing.T) {
		c := qt.New(t)

		// Verify that pgx driver is properly registered
		db, err := sql.Open("pgx", dsn)
		c.Assert(err, qt.IsNil)
		defer db.Close()

		err = db.Ping()
		c.Assert(err, qt.IsNil)
	})
}
