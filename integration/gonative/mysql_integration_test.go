//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/mysql"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// skipIfNoMySQL skips only when MYSQL_TEST_DSN is absent; a bad configured DSN fails.
func skipIfNoMySQL(t *testing.T) string {
	t.Helper()
	return requireReachableEngine(t, dbtarget.MySQL, "mysql", "MySQL")
}

// skipIfNoMariaDB skips only when MARIADB_TEST_DSN is absent; a bad configured DSN fails.
func skipIfNoMariaDB(t *testing.T) string {
	t.Helper()
	return requireReachableEngine(t, dbtarget.MariaDB, "mysql", "MariaDB")
}

// Helper function to find a column by name
func findColumn(columns []types.DBColumn, name string) *types.DBColumn {
	for i := range columns {
		if columns[i].Name == name {
			return &columns[i]
		}
	}
	return nil
}

func tableExists(db *sql.DB, tableName string) bool {
	var exists bool
	checkSQL := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ?
		)`

	err := db.QueryRow(checkSQL, tableName).Scan(&exists)
	return err == nil && exists
}

func TestMySQLReader_ReadSchema_Integration(t *testing.T) {
	dsn := skipIfNoMySQL(t)
	testMySQLCompatibleReaderReadSchema(t, dsn)
}

func TestMariaDBReader_ReadSchema_Integration(t *testing.T) {
	dsn := skipIfNoMariaDB(t)
	testMySQLCompatibleReaderReadSchema(t, dsn)
}

func testMySQLCompatibleReaderReadSchema(t *testing.T, dsn string) {
	t.Helper()
	c := qt.New(t)

	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// Create a test table with various column types
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_table (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			name_lc VARCHAR(255) GENERATED ALWAYS AS (lower(name)) STORED,
			status ENUM('active', 'inactive') DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE KEY unique_name (name)
		)
	`)
	c.Assert(err, qt.IsNil)

	// Clean up after test
	defer func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS test_table")
	}()

	reader := mysql.NewMySQLReader(db, "")
	schema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema, qt.IsNotNil)
	c.Assert(schema.Tables, qt.Not(qt.HasLen), 0)

	// Find our test table
	var testTable *types.DBTable
	for i := range schema.Tables {
		if schema.Tables[i].Name == "test_table" {
			testTable = &schema.Tables[i]
			break
		}
	}
	c.Assert(testTable, qt.IsNotNil)
	c.Assert(testTable.Columns, qt.HasLen, 5)

	// Verify column properties
	idCol := findColumn(testTable.Columns, "id")
	c.Assert(idCol, qt.IsNotNil)
	c.Assert(idCol.IsAutoIncrement, qt.IsTrue)
	c.Assert(idCol.IsPrimaryKey, qt.IsTrue)

	nameCol := findColumn(testTable.Columns, "name")
	c.Assert(nameCol, qt.IsNotNil)
	c.Assert(nameCol.IsNullable, qt.Equals, "NO")
	c.Assert(nameCol.IsUnique, qt.IsTrue)

	nameLCCol := findColumn(testTable.Columns, "name_lc")
	c.Assert(nameLCCol, qt.IsNotNil)
	c.Assert(nameLCCol.GeneratedKind, qt.Equals, "STORED")
	c.Assert(nameLCCol.GeneratedExpression, qt.IsNotNil)
	c.Assert(*nameLCCol.GeneratedExpression, qt.Contains, "name")

	statusCol := findColumn(testTable.Columns, "status")
	c.Assert(statusCol, qt.IsNotNil)
	c.Assert(statusCol.DataType, qt.Equals, "enum('active','inactive')")
}

func TestMySQLWriter_Integration(t *testing.T) {
	dsn := skipIfNoMySQL(t)
	c := qt.New(t)

	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	writer := mysql.NewMySQLWriter(db, "", "mysql")

	t.Run("transaction lifecycle", func(t *testing.T) {
		c := qt.New(t)
		// Test successful transaction
		tx, err := writer.BeginTransaction(t.Context())
		c.Assert(err, qt.IsNil)

		err = tx.ExecuteSQL(t.Context(), "SELECT 1")
		c.Assert(err, qt.IsNil)

		err = tx.Commit()
		c.Assert(err, qt.IsNil)

		// Test rollback transaction
		tx, err = writer.BeginTransaction(t.Context())
		c.Assert(err, qt.IsNil)

		err = tx.Rollback()
		c.Assert(err, qt.IsNil)
	})

	t.Run("DropAllTables", func(t *testing.T) {
		c := qt.New(t)
		cleanupDSN := requireReachableTestDSN(
			t,
			"MYSQL_CLEANUP_TEST_DSN",
			"mysql",
			"MySQL cleanup",
		)
		cleanupDB, err := sql.Open("mysql", cleanupDSN)
		c.Assert(err, qt.IsNil)
		defer cleanupDB.Close()
		cleanupWriter := mysql.NewMySQLWriter(cleanupDB, "", "mysql")

		// Create a test table first
		_, err = cleanupDB.Exec("CREATE TABLE IF NOT EXISTS temp_test_table (id INT AUTO_INCREMENT PRIMARY KEY)")
		c.Assert(err, qt.IsNil)

		err = cleanupWriter.DropAllTables(t.Context())
		c.Assert(err, qt.IsNil)

		// Verify table was dropped
		exists := tableExists(cleanupDB, "temp_test_table")
		c.Assert(exists, qt.IsFalse)
	})
}
