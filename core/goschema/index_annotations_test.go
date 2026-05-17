package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

// TestParseIndexAnnotation_TypeAndGranularity exercises the parser's strict
// attribute validation and parsing for the ClickHouse-flavoured
// type= / granularity= keys, alongside the existing PG-flavoured type= use.
func TestParseIndexAnnotation_TypeAndGranularity(t *testing.T) {
	const src = `package fixture

//migrator:schema:table name="events"
type Event struct {
	//migrator:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//migrator:schema:field name="payload" type="String"
	Payload string

	//migrator:schema:index name="idx_e_payload" fields="payload" type="bloom_filter" granularity="64"
	_ int
}
`
	db := goschema.ParseSource("fixture.go", src)

	c := qt.New(t)
	c.Assert(db.Indexes, qt.HasLen, 1)
	idx := db.Indexes[0]
	c.Assert(idx.Name, qt.Equals, "idx_e_payload")
	c.Assert(idx.Fields, qt.DeepEquals, []string{"payload"})
	c.Assert(idx.Type, qt.Equals, "bloom_filter")
	c.Assert(idx.Granularity, qt.Equals, 64)
}

// TestParseIndexAnnotation_UnknownKeyRejected verifies that the strict
// validation gate added on //migrator:schema:index catches typos in attribute
// keys (e.g. "granluarity") rather than silently dropping them and producing
// a wrong default value.
func TestParseIndexAnnotation_UnknownKeyRejected(t *testing.T) {
	const src = `package fixture

//migrator:schema:table name="events"
type Event struct {
	//migrator:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//migrator:schema:index name="idx_e_payload" fields="payload" granluarity="64"
	_ int
}
`
	c := qt.New(t)
	defer func() {
		r := recover()
		c.Assert(r, qt.Not(qt.IsNil), qt.Commentf("expected parser to panic on unknown index attribute"))
	}()
	_ = goschema.ParseSource("fixture.go", src)
}

// TestParseIndexAnnotation_NoTypeNoGranularity confirms that the new fields
// default cleanly when omitted, so existing user code without these keys
// continues to behave exactly as before.
func TestParseIndexAnnotation_NoTypeNoGranularity(t *testing.T) {
	const src = `package fixture

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)"
	Email string

	//migrator:schema:index name="idx_users_email" fields="email" unique="true"
	_ int
}
`
	db := goschema.ParseSource("fixture.go", src)
	c := qt.New(t)
	c.Assert(db.Indexes, qt.HasLen, 1)
	idx := db.Indexes[0]
	c.Assert(idx.Type, qt.Equals, "")
	c.Assert(idx.Granularity, qt.Equals, 0)
	c.Assert(idx.Unique, qt.IsTrue)
}
