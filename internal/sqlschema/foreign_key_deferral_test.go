package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// deferralOfKeys lists the columns and the deferral of every foreign key the
// model carries, the columns' own keys first and then the table's.
func deferralOfKeys(database schemamodel.Database) []schemamodel.Constraint {
	var keys []schemamodel.Constraint
	for _, field := range database.Fields {
		if field.Foreign != "" {
			keys = append(keys, schemamodel.Constraint{Columns: []string{field.Name}, Deferrable: field.Deferrable, Initially: field.Initially})
		}
	}
	for _, constraint := range database.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			keys = append(keys, schemamodel.Constraint{Columns: constraint.Columns, Deferrable: constraint.Deferrable, Initially: constraint.Initially})
		}
	}
	return keys
}

// TestRead_ForeignKeyDeferral carries the deferral a SQL file writes on a
// foreign key into the model, so a plan creates the key as the file declares
// it and a comparison with the database finds it synced. Left unread, the
// table-level clause is a column named deferrable, and the column one is
// refused (stokaro/ptah#3818).
func TestRead_ForeignKeyDeferral(t *testing.T) {
	const sql = `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (
  a int REFERENCES p (id) DEFERRABLE INITIALLY DEFERRED,
  b int,
  d int,
  e int REFERENCES p (id),
  FOREIGN KEY (b) REFERENCES p (id) INITIALLY DEFERRED,
  FOREIGN KEY (d) REFERENCES p (id) DEFERRABLE
);
ALTER TABLE c ADD COLUMN f int REFERENCES p (id) DEFERRABLE INITIALLY IMMEDIATE;`
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(sql), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(deferralOfKeys(database), qt.DeepEquals, []schemamodel.Constraint{
		{Columns: []string{"a"}, Deferrable: true, Initially: "deferred"},
		{Columns: []string{"e"}},
		{Columns: []string{"f"}, Deferrable: true, Initially: "immediate"},
		{Columns: []string{"b"}, Deferrable: true, Initially: "deferred"},
		{Columns: []string{"d"}, Deferrable: true},
	})
}
