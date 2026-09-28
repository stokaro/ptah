//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// foreignKeysOverOneColumn are databases holding two foreign keys over one
// column. Measured on PostgreSQL 18.6 and MySQL 8.4.11, each server builds
// both keys. Described with one foreign key per column, a diff of the
// database with itself plans to drop one of them (stokaro/ptah#3873).
var foreignKeysOverOneColumn = []struct {
	name string
	sql  string
}{
	{name: "unnamed keys, one with an action", sql: `CREATE TABLE op (id int PRIMARY KEY);
CREATE TABLE ord (c int, FOREIGN KEY (c) REFERENCES op (id) ON DELETE CASCADE, FOREIGN KEY (c) REFERENCES op (id));`},
	{name: "named keys, the later name with an action", sql: `CREATE TABLE op (id int PRIMARY KEY);
CREATE TABLE ord (c int, CONSTRAINT fk_a FOREIGN KEY (c) REFERENCES op (id),
  CONSTRAINT fk_b FOREIGN KEY (c) REFERENCES op (id) ON DELETE CASCADE);`},
}

// TestSchemaDiffFromADatabaseWithTwoForeignKeysOverAColumnToItselfIsSyncedE2E
// diffs each database with itself on every engine that shares the comparison.
func TestSchemaDiffFromADatabaseWithTwoForeignKeysOverAColumnToItselfIsSyncedE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		for _, test := range foreignKeysOverOneColumn {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				source := engine.builtDB(c, test.sql)

				out := runPtahNative(c, "schema", "diff", "--from", source, "--to", source)

				c.Assert(out, qt.Contains, "Schemas are synced")
			})
		}
	}
}

// TestSchemaDiffToADatabaseWithASecondForeignKeyOverAColumnPlansItE2E is the
// control: from a database holding one of the keys to one holding both, the
// diff adds the other.
func TestSchemaDiffToADatabaseWithASecondForeignKeyOverAColumnPlansItE2E(t *testing.T) {
	for _, engine := range uniqueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			source := engine.builtDB(c, `CREATE TABLE op (id int PRIMARY KEY);
CREATE TABLE ord (c int, CONSTRAINT fk_a FOREIGN KEY (c) REFERENCES op (id));`)
			target := engine.builtDB(c, foreignKeysOverOneColumn[1].sql)

			out := runPtahNative(c, "schema", "diff", "--from", source, "--to", target)

			c.Assert(out, qt.Contains, "fk_b")
			c.Assert(out, qt.Contains, "CASCADE")
		})
	}
}
