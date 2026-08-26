package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/internal/dbschema/sqlite"
)

// onlyForeignKey returns the one FOREIGN KEY constraint in the schema. The
// primary keys of both tables are constraints too, and they are not what any
// case here is about.
func onlyForeignKey(c *qt.C, constraints []catalog.Constraint) catalog.Constraint {
	c.Helper()
	var found []catalog.Constraint
	for _, constraint := range constraints {
		if constraint.Type == "FOREIGN KEY" {
			found = append(found, constraint)
		}
	}
	c.Assert(found, qt.HasLen, 1)
	return found[0]
}

// TestReader_RecordsAForeignKeysDeferral pins the property PRAGMA
// foreign_key_list does not report.
//
// SQLite records deferral only in the CREATE TABLE text: foreign_key_list
// answers with the columns, the referenced table, the actions and the match
// clause and says nothing about DEFERRABLE. The reader had nothing to read, so
// every key was described as immediate -- a deferred key became immediate on a
// replay, and a declaration asking for DEFERRABLE against a database without it
// was reported as synced, which is the sentence stokaro/ptah#1624 added a
// comparison to prevent (stokaro/ptah#2202).
func TestReader_RecordsAForeignKeysDeferral(t *testing.T) {
	tests := []struct {
		name       string
		ddl        string
		deferrable bool
		initially  string
	}{
		{
			// An UNNAMED inline key. It had no entry at all in the DDL walk,
			// which only recorded keys that carried a CONSTRAINT name, so this
			// is the case a name-keyed lookup cannot see.
			name:       "an unnamed inline key",
			ddl:        `pid INTEGER NOT NULL REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED`,
			deferrable: true,
			initially:  "DEFERRED",
		},
		{
			name:       "a named inline key",
			ddl:        `pid INTEGER NOT NULL CONSTRAINT fk_named REFERENCES parent(id) DEFERRABLE INITIALLY IMMEDIATE`,
			deferrable: true,
			initially:  "IMMEDIATE",
		},
		{
			name:       "DEFERRABLE with no timing",
			ddl:        `pid INTEGER NOT NULL REFERENCES parent(id) DEFERRABLE`,
			deferrable: true,
			initially:  "",
		},
		{
			// The control. Most keys carry nothing, and a reader that reported
			// every key as deferrable would satisfy every row above.
			name:       "no deferral at all",
			ddl:        `pid INTEGER NOT NULL REFERENCES parent(id)`,
			deferrable: false,
			initially:  "",
		},
		{
			// SQLite parses this and behaves immediately. Reporting the timing
			// would describe a deferral that never happens; reporting neither
			// makes the replay behave the way the source does.
			name:       "NOT DEFERRABLE with a timing",
			ddl:        `pid INTEGER NOT NULL REFERENCES parent(id) NOT DEFERRABLE INITIALLY DEFERRED`,
			deferrable: false,
			initially:  "",
		},
		{
			name:       "a table-level key",
			ddl:        `pid INTEGER NOT NULL, CONSTRAINT fk_tbl FOREIGN KEY (pid) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED`,
			deferrable: true,
			initially:  "DEFERRED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMemoryDB(t)
			execSQL(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
			execSQL(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, `+tt.ddl+`)`)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			foreignKey := onlyForeignKey(c, schema.Constraints)
			c.Assert(foreignKey.Deferrable, qt.Equals, tt.deferrable)
			c.Assert(foreignKey.Initially, qt.Equals, tt.initially)
		})
	}
}
