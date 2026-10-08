package atlasschema_test

import (
	"slices"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

func connectSQLite(c *qt.C, dbPath string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	return conn
}

func sqliteTableExists(c *qt.C, dbPath, table string) bool {
	c.Helper()
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)

	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
	c.Assert(err, qt.IsNil)
	return slices.ContainsFunc(schema.Tables, func(dbTable catalog.Table) bool {
		return dbTable.Name == table
	})
}

// sqliteObjectCount counts the catalog entries of any type named name.
func sqliteObjectCount(c *qt.C, dbPath, name string) int {
	c.Helper()
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM sqlite_master WHERE name = ?", name).Scan(&count), qt.IsNil)
	return count
}
