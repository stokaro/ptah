package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/ptaherr"
)

func TestParseSequenceAnnotation_AllOptions(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence name="order_seq" schema="app" as="BigInt" start="1000" increment="2" minvalue="1" maxvalue="9999" cache="20" cycle="true" owned_by="orders.id" comment="Order numbers"
type OrderSeq struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Sequences, qt.HasLen, 1)
	seq := db.Sequences[0]
	c.Assert(seq.Name, qt.Equals, "order_seq")
	c.Assert(seq.Schema, qt.Equals, "app")
	c.Assert(seq.AsType, qt.Equals, "bigint") // canonicalized to lower-case
	c.Assert(*seq.Start, qt.Equals, int64(1000))
	c.Assert(*seq.Increment, qt.Equals, int64(2))
	c.Assert(*seq.MinValue, qt.Equals, int64(1))
	c.Assert(*seq.MaxValue, qt.Equals, int64(9999))
	c.Assert(*seq.Cache, qt.Equals, int64(20))
	c.Assert(seq.Cycle, qt.IsTrue)
	c.Assert(seq.OwnedBy, qt.Equals, "orders.id")
	c.Assert(seq.Comment, qt.Equals, "Order numbers")
	c.Assert(seq.QualifiedName(), qt.Equals, "app.order_seq")
}

func TestParseSequenceAnnotation_MinimalLeavesOptionsUnset(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence name="s"
type S struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Sequences, qt.HasLen, 1)
	seq := db.Sequences[0]
	c.Assert(seq.Name, qt.Equals, "s")
	c.Assert(seq.Start, qt.IsNil)
	c.Assert(seq.Increment, qt.IsNil)
	c.Assert(seq.MinValue, qt.IsNil)
	c.Assert(seq.MaxValue, qt.IsNil)
	c.Assert(seq.Cache, qt.IsNil)
	c.Assert(seq.Cycle, qt.IsFalse)
	c.Assert(seq.QualifiedName(), qt.Equals, "s")
}

func TestParseSequenceAnnotation_MissingNameRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence increment="1"
type S struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "migrator:schema:sequence")
}

func TestParseSequenceAnnotation_InvalidIntegerRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence name="s" increment="not-a-number"
type S struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Attribute, qt.Equals, "increment")
}

func TestParseSequenceAnnotation_UnknownAttributeRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence name="s" incrementt="1"
type S struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "migrator:schema:sequence")
}

func TestParseSequenceAnnotation_TypeAliasNormalized(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence name="s" as="int8"
type S struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Sequences, qt.HasLen, 1)
	c.Assert(db.Sequences[0].AsType, qt.Equals, "bigint") // int8 canonicalized
}

func TestParseSequenceAnnotation_InvalidTypeRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:sequence name="s" as="bigint MAXVALUE 5 CYCLE"
type S struct{}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Attribute, qt.Equals, "as")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
}

func TestParseGrantOnSequence(t *testing.T) {
	const src = `package fixture

//migrator:schema:grant role="app_user" privilege="USAGE,SELECT" on_sequence="order_seq" comment="Sequence usage"
type Access struct{}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Grants, qt.HasLen, 1)
	grant := db.Grants[0]
	c.Assert(grant.OnSequence, qt.Equals, "order_seq")
	c.Assert(grant.OnTable, qt.Equals, "")
	c.Assert(grant.OnSchema, qt.Equals, "")
	c.Assert(grant.Privileges, qt.DeepEquals, []string{"USAGE", "SELECT"})
}
