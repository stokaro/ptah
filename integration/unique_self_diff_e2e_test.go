//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// uniqueKeysOverOneColumn are databases holding two UNIQUE keys over one
// column, one under the name the server gives a column's key. Measured on
// MySQL 8.4, MariaDB 11.8 and PostgreSQL 18.6, the first row builds `a` and
// `a_2`, and `g2_a_key` and `g2_a_key1`. Described as the second key alone, a
// diff of the database with itself plans to drop the first
// (stokaro/ptah#3819).
var uniqueKeysOverOneColumn = []struct {
	name string
	sql  string
}{
	{name: "ALTER TABLE adds a second key", sql: `CREATE TABLE g2 (a int, UNIQUE (a));
ALTER TABLE g2 ADD UNIQUE (a);`},
	{name: "ALTER TABLE adds a named key", sql: `CREATE TABLE g3 (a int UNIQUE);
ALTER TABLE g3 ADD CONSTRAINT uq_g3 UNIQUE (a);`},
}

// TestSchemaDiffFromADatabaseWithTwoKeysOverAColumnToItselfIsSyncedE2E diffs
// each database with itself on every engine that shares the comparison.
func TestSchemaDiffFromADatabaseWithTwoKeysOverAColumnToItselfIsSyncedE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		for _, test := range uniqueKeysOverOneColumn {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				source := engine.builtDB(c, test.sql)

				out := runPtahNative(c, "schema", "diff", "--from", source, "--to", source)

				c.Assert(out, qt.Contains, "Schemas are synced")
			})
		}
	}
}
