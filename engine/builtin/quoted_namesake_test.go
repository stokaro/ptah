package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// TestGetOrderedCreateStatements_BareKeyToAQuotedNamesake reads an Oracle SQL
// file that declares the referenced table quoted and names it bare in a
// foreign key. On Oracle `"customers"` and customers are different tables, so
// the key references a table the file does not declare; the refusal names the
// quoted declaration and the spelling to write.
func TestGetOrderedCreateStatements_BareKeyToAQuotedNamesake(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(`CREATE TABLE "customers" (id NUMBER(10) PRIMARY KEY);
CREATE TABLE orders (id NUMBER(10) PRIMARY KEY, cust_id NUMBER(10), CONSTRAINT fk1 FOREIGN KEY (cust_id) REFERENCES customers (id));
`), platform.Oracle)
	c.Assert(err, qt.IsNil)

	statements, err := builtin.GetOrderedCreateStatements(&database, platform.Oracle)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*invalid foreign key: constraint "fk1" references unknown table "customers"; `+
		`the schema declares the table "customers" quoted, which oracle keeps apart from the bare customers: write "customers" to name it`)
	c.Assert(statements, qt.HasLen, 0)
}
