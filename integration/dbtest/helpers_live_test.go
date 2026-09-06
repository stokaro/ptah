//go:build integration

package dbtest_test

import (
	"context"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// scratchDatabaseCount counts the disposable databases a run would have left
// behind on the server.
//
// It matches on the prefix the provisioner uses rather than on an exact name,
// because the names carry random bytes and the point is how many exist rather
// than which.
func scratchDatabaseCount(c *qt.C, serverURL string) int {
	connection, err := dbschema.ConnectToDatabase(context.Background(), serverURL)
	c.Assert(err, qt.IsNil)
	defer connection.Close()

	var count int
	row := connection.QueryRow(
		"SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'ptah_scratch_%'")
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}
