//go:build integration

package ydb_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

func TestYDBDesiredYQL_ChangefeedsAndConsumers(t *testing.T) {
	const table = "CREATE TABLE events (id Int64 NOT NULL, PRIMARY KEY (id));"
	const feed = "ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON', retention_period=Interval('PT12H'));"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connect(c, enterRealm(c, line))
			path := filepath.Join(c.TempDir(), "schema.sql")
			for _, source := range []string{
				table + feed + "ALTER TOPIC `events/updates` ADD CONSUMER worker;",
				table + feed + "ALTER TOPIC `events/updates` ADD CONSUMER audit WITH (important=TRUE);",
				table + feed,
				table,
			} {
				c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				statements := planAgainst(c, conn, desired, nil)
				c.Assert(statements, qt.Not(qt.HasLen), 0)
				apply(c, conn, statements)
				c.Assert(planAgainst(c, conn, desired, nil), qt.HasLen, 0)
			}
		})
	}
}
