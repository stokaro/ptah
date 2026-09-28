package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
)

// TestParseSource_IndexInvisible reads `invisible="true"` on an index
// annotation, an index MySQL builds INVISIBLE and MariaDB IGNORED
// (stokaro/ptah#3853).
func TestParseSource_IndexInvisible(t *testing.T) {
	c := qt.New(t)
	source := `package entities

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="total" type="INT"
	//ptah:schema:index name="k_total" fields="total" comment="lookup" invisible="true"
	Total int
}
`

	db, err := goschema.ParseSource("orders.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Invisible, qt.IsTrue)
	c.Assert(db.Indexes[0].Comment, qt.Equals, "lookup")
}
