//go:build integration

package ydb_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

func desiredSerialYQL(c *qt.C, root string, increment int) *schemamodel.Database {
	c.Helper()
	source := fmt.Sprintf("CREATE TABLE `ptah_ydb_serials/orders` (id BigSerial NOT NULL,note Utf8,PRIMARY KEY(id)); ALTER SEQUENCE `%s/ptah_ydb_serials/orders/_serial_column_id` START WITH 100 INCREMENT BY 5; CREATE TABLE `ptah_ydb_serials/counters` (id BigSerial NOT NULL,note Utf8,PRIMARY KEY(id)); ALTER SEQUENCE `%s/ptah_ydb_serials/counters/_serial_column_id` INCREMENT %d;", root, root, increment)
	document := sqlschema.NewDocument(nil)
	document.YDBDatabasePath = root
	database, _, err := sqlschema.ReadOnto([]byte(source), "ydb", document)
	c.Assert(err, qt.IsNil)
	return &database
}

func TestYDBDesiredYQL_SerialSequences(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, serialSchemas)
			c.Cleanup(func() { dropTables(c, conn, serialSchemas) })
			root := readScoped(c, conn, serialSchemas).DatabasePath
			desired := desiredSerialYQL(c, root, 3)
			apply(c, conn, planAgainst(c, conn, desired, serialSchemas))
			c.Assert(planAgainst(c, conn, desired, serialSchemas), qt.HasLen, 0)
			c.Assert(serialIDs(c, conn, "orders", "a", "b"), qt.DeepEquals, []int64{100, 105})
			c.Assert(serialIDs(c, conn, "counters", "a", "b"), qt.DeepEquals, []int64{1, 4})
			changed := desiredSerialYQL(c, root, 7)
			changes := planAgainst(c, conn, changed, serialSchemas)
			c.Assert(changes, qt.DeepEquals, []string{"ALTER SEQUENCE `" + root + "/ptah_ydb_serials/counters/_serial_column_id` START WITH 1 INCREMENT BY 7"})
			apply(c, conn, changes)
			c.Assert(planAgainst(c, conn, changed, serialSchemas), qt.HasLen, 0)
			c.Assert(serialIDs(c, conn, "counters", "c", "d"), qt.DeepEquals, []int64{1, 4, 7, 14})
		})
	}
}
