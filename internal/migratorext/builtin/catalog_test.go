package builtin_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	// The SQLite driver stands in for a server's information_schema, which an
	// attached in-memory database of that name provides.
	_ "modernc.org/sqlite"

	"ptah.run/core/platform"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
)

// informationSchema opens a database whose information_schema.columns holds
// one column, state, of the table schema_migrations in schema ptah_test.
func informationSchema(c *qt.C) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"ATTACH DATABASE ':memory:' AS information_schema",
		"CREATE TABLE information_schema.columns (table_schema TEXT, table_name TEXT, column_name TEXT)",
		"INSERT INTO information_schema.columns VALUES ('ptah_test', 'schema_migrations', 'state')",
	} {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return db
}

// The MySQL family and Spanner ask information_schema in the configured
// schema, or in the schema the connection carries, trimmed, when none is
// configured.
func TestCatalog_ColumnExistsAsksTheResolvedSchema(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		configured string
		connection string
		want       bool
	}{
		{name: "mysql reads the connected database", dialect: platform.MySQL, connection: " ptah_test ", want: true},
		{name: "mariadb reads the connected database", dialect: platform.MariaDB, connection: "ptah_test", want: true},
		{name: "spanner reads the connection's schema", dialect: platform.Spanner, connection: "ptah_test", want: true},
		{name: "a configured schema wins", dialect: platform.MySQL, configured: "other", connection: "ptah_test", want: false},
		{name: "spanner honors a configured schema", dialect: platform.Spanner, configured: "ptah_test", connection: "other", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := informationSchema(c)
			table := migratorext.Table{ConfiguredSchema: test.configured, ConnectionSchema: test.connection, Name: "schema_migrations"}

			found, err := builtin.Lookup(test.dialect).Catalog.ColumnExists(c.Context(), db, table, "state")

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.Equals, test.want)
		})
	}
}
