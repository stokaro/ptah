package sqlschema_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// deferredKey is one key of the model and how it defers its check.
type deferredKey struct {
	Table      string
	Type       string
	Name       string
	Columns    []string
	Deferrable bool
	Initially  string
}

// deferredKeysOf lists the primary keys the tables carry and the UNIQUE and
// EXCLUDE constraints of the model, in that order.
func deferredKeysOf(database schemamodel.Database) []deferredKey {
	var keys []deferredKey
	for _, table := range database.Tables {
		if len(table.PrimaryKey) > 0 {
			keys = append(keys, deferredKey{
				Table: table.Name, Type: "PRIMARY KEY", Name: table.PrimaryKeyName, Columns: table.PrimaryKey,
				Deferrable: table.PrimaryKeyDeferrable, Initially: table.PrimaryKeyInitially,
			})
		}
	}
	for _, constraint := range database.Constraints {
		if slices.Contains([]string{"UNIQUE", "EXCLUDE"}, constraint.Type) {
			keys = append(keys, deferredKey{
				Table: constraint.Table, Type: constraint.Type, Name: constraint.Name, Columns: constraint.Columns,
				Deferrable: constraint.Deferrable, Initially: constraint.Initially,
			})
		}
	}
	return keys
}

// TestRead_KeyDeferral carries the deferral a SQL file writes on a key into
// the model. Each key was created by PostgreSQL 18.6 from the same statement,
// with condeferrable and condeferred as the row says; the names are the ones
// pg_constraint holds. A column's deferrable key is the table's key over the
// column, which is how the server reports it.
func TestRead_KeyDeferral(t *testing.T) {
	const sql = `CREATE TABLE u1 (x int, UNIQUE (x) DEFERRABLE);
CREATE TABLE u2 (x int, CONSTRAINT u2_named UNIQUE (x) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE u3 (x int UNIQUE DEFERRABLE INITIALLY IMMEDIATE, y int UNIQUE NOT DEFERRABLE);
CREATE TABLE p1 (x int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE p2 (x int, y int, PRIMARY KEY (x, y) DEFERRABLE);
CREATE TABLE e1 (r int, EXCLUDE USING btree (r WITH =) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE u4 (x int);
ALTER TABLE u4 ADD CONSTRAINT u4_k UNIQUE (x) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE p3 (x int);
ALTER TABLE p3 ADD PRIMARY KEY (x) DEFERRABLE;`
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(sql), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(deferredKeysOf(database), qt.DeepEquals, []deferredKey{
		{Table: "p1", Type: "PRIMARY KEY", Columns: []string{"x"}, Deferrable: true, Initially: "deferred"},
		{Table: "p2", Type: "PRIMARY KEY", Columns: []string{"x", "y"}, Deferrable: true},
		{Table: "p3", Type: "PRIMARY KEY", Columns: []string{"x"}, Deferrable: true},
		{Table: "u1", Type: "UNIQUE", Name: "u1_x_key", Columns: []string{"x"}, Deferrable: true},
		{Table: "u2", Type: "UNIQUE", Name: "u2_named", Columns: []string{"x"}, Deferrable: true, Initially: "deferred"},
		{Table: "u3", Type: "UNIQUE", Name: "u3_x_key", Columns: []string{"x"}, Deferrable: true, Initially: "immediate"},
		{Table: "e1", Type: "EXCLUDE", Name: "e1_r_excl", Deferrable: true, Initially: "deferred"},
		{Table: "u4", Type: "UNIQUE", Name: "u4_k", Columns: []string{"x"}, Deferrable: true, Initially: "deferred"},
	})
}

// TestRead_DroppedPrimaryKeyTakesItsDeferral drops a deferrable primary key
// and adds a plain one: the new key does not defer. The deferral belongs to
// the key the ALTER TABLE dropped, not to the table.
func TestRead_DroppedPrimaryKeyTakesItsDeferral(t *testing.T) {
	const sql = `CREATE TABLE p (x int, CONSTRAINT p_pk PRIMARY KEY (x) DEFERRABLE INITIALLY DEFERRED);
ALTER TABLE p DROP CONSTRAINT p_pk;`
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(sql), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].PrimaryKey, qt.HasLen, 0)
	c.Assert(database.Tables[0].PrimaryKeyDeferrable, qt.IsFalse)
	c.Assert(database.Tables[0].PrimaryKeyInitially, qt.Equals, "")
}
