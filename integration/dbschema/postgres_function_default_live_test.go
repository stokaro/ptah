//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A default that calls a function with arguments reads, applies, and plans
// nothing again (stokaro/ptah#3611). The first row is the shape pg_dump writes
// for every serial column: a sequence, and a nextval default naming it as a
// regclass.
func TestPostgresLiveSQLDocumentFunctionDefaultsConverge(t *testing.T) {
	tests := []struct {
		name     string
		document string
	}{
		{
			name: "a serial column as pg_dump writes it",
			document: `CREATE SEQUENCE "%[1]s".counted_id_seq;
CREATE TABLE "%[1]s".counted (
    id integer NOT NULL DEFAULT nextval('"%[1]s".counted_id_seq'::regclass) PRIMARY KEY
);`,
		},
		{
			name: "a sequence named without the regclass cast",
			document: `CREATE SEQUENCE "%[1]s".counted_id_seq;
CREATE TABLE "%[1]s".counted (id integer NOT NULL DEFAULT nextval('"%[1]s".counted_id_seq') PRIMARY KEY);`,
		},
		{
			name:     "string arguments and a nested call",
			document: `CREATE TABLE "%[1]s".counted (id integer PRIMARY KEY, label text DEFAULT concat('a', lower('B'), 'c'));`,
		},
		{
			name:     "a qualified function and a cast after the call",
			document: `CREATE TABLE "%[1]s".counted (id integer PRIMARY KEY, at timestamp DEFAULT pg_catalog.now()::timestamp);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)

			live := settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": test.document})

			c.Assert(liveColumnNames(c, live, "counted"), qt.Not(qt.HasLen), 0)
		})
	}
}

// The control: a default that calls the function with another argument plans
// the change.
func TestPostgresLiveSQLDocumentFunctionDefaultChangePlans(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `CREATE TABLE "%[1]s".counted (id integer PRIMARY KEY, label text DEFAULT lower('A'));`})

	statements := planFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `CREATE TABLE "%[1]s".counted (id integer PRIMARY KEY, label text DEFAULT lower('B'));`})

	c.Assert(statements, qt.Not(qt.HasLen), 0)
}
