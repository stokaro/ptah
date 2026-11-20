package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// TestRead_TemporalKeys_HappyPath reads WITHOUT OVERLAPS and PERIOD onto the
// model: on the table for a primary key, on the constraint for the others
// (stokaro/ptah#6330). Each statement was run on PostgreSQL 18.6.
func TestRead_TemporalKeys_HappyPath(t *testing.T) {
	t.Run("a primary key in the table body", func(t *testing.T) {
		c := qt.New(t)

		database, _, err := sqlschema.Read([]byte(
			`CREATE TABLE p (id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));`), "postgres")

		c.Assert(err, qt.IsNil)
		c.Assert(database.Tables, qt.HasLen, 1)
		c.Assert(database.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id", "valid_at"})
		c.Assert(database.Tables[0].PrimaryKeyWithoutOverlaps, qt.IsTrue)
	})
	t.Run("a primary key ALTER TABLE adds", func(t *testing.T) {
		c := qt.New(t)

		database, _, err := sqlschema.Read([]byte(
			"CREATE TABLE p (id int, valid_at daterange);\n"+
				"ALTER TABLE p ADD CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);"), "postgres")

		c.Assert(err, qt.IsNil)
		c.Assert(database.Tables[0].PrimaryKeyWithoutOverlaps, qt.IsTrue)
		c.Assert(database.Tables[0].PrimaryKeyName, qt.Equals, "p_pk")
	})
	t.Run("a unique constraint and a foreign key", func(t *testing.T) {
		c := qt.New(t)

		database, _, err := sqlschema.Read([]byte(
			"CREATE TABLE p (id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));\n"+
				"CREATE TABLE c (pid int, valid_at daterange, CONSTRAINT c_uq UNIQUE (pid, valid_at WITHOUT OVERLAPS), "+
				"CONSTRAINT c_fk FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at));"), "postgres")

		c.Assert(err, qt.IsNil)
		c.Assert(database.Constraints, qt.HasLen, 2)
		c.Assert(database.Constraints[0].Name, qt.Equals, "c_uq")
		c.Assert(database.Constraints[0].WithoutOverlaps, qt.IsTrue)
		c.Assert(database.Constraints[0].Period, qt.IsFalse)
		c.Assert(database.Constraints[1].Name, qt.Equals, "c_fk")
		c.Assert(database.Constraints[1].Period, qt.IsTrue)
		c.Assert(database.Constraints[1].WithoutOverlaps, qt.IsFalse)
		c.Assert(database.Constraints[1].ForeignColumns, qt.DeepEquals, []string{"id", "valid_at"})
	})
	t.Run("dropping the key takes the clause with it", func(t *testing.T) {
		c := qt.New(t)

		database, _, err := sqlschema.Read([]byte(
			"CREATE TABLE p (id int, valid_at daterange, CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));\n"+
				"ALTER TABLE p DROP CONSTRAINT p_pk;\n"+
				"ALTER TABLE p ADD PRIMARY KEY (id, valid_at);"), "postgres")

		c.Assert(err, qt.IsNil)
		c.Assert(database.Tables[0].PrimaryKeyWithoutOverlaps, qt.IsFalse)
	})
}
