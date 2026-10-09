package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseIndexAnnotation_TypeAndGranularity exercises the parser's strict
// attribute validation for a ClickHouse skipping index: the common type= key,
// and the granularity as a ClickHouse source property the owner decodes.
func TestParseIndexAnnotation_TypeAndGranularity(t *testing.T) {
	const src = `package fixture

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="payload" type="String"
	Payload string

	//ptah:schema:index name="idx_e_payload" fields="payload" type="bloom_filter" platform.clickhouse.granularity="64"
	_ int
}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Indexes, qt.HasLen, 1)
	idx := db.Indexes[0]
	c.Assert(idx.Name, qt.Equals, "idx_e_payload")
	c.Assert(idx.Fields, qt.DeepEquals, []string{"payload"})
	c.Assert(idx.Type, qt.Equals, "bloom_filter")
	c.Assert(idx.Overrides, qt.DeepEquals, map[string]map[string]string{"clickhouse": {"granularity": "64"}})
}

// TestParseIndexAnnotation_BareGranularityRejected pins that the setting has
// one spelling. The ClickHouse owner reads platform.clickhouse.granularity; a
// bare key is an unknown attribute rather than a second representation.
func TestParseIndexAnnotation_BareGranularityRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="payload" type="String"
	Payload string

	//ptah:schema:index name="idx_e_payload" fields="payload" granularity="64"
	_ int
}
`
	c := qt.New(t)
	db, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Attribute, qt.Equals, "granularity")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(db.Indexes, qt.HasLen, 0)
}

// TestParseIndexAnnotation_UnknownKeyRejected verifies that the strict
// validation gate added on //ptah:schema:index catches typos in attribute
// keys (e.g. "granluarity") rather than silently dropping them and producing
// a wrong default value.
func TestParseIndexAnnotation_UnknownKeyRejected(t *testing.T) {
	const src = `package fixture

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:index name="idx_e_payload" fields="payload" granluarity="64"
	_ int
}
`
	c := qt.New(t)
	_, err := goschema.ParseSource("fixture.go", src)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:index")
	c.Assert(parseErr.Attribute, qt.Equals, "granluarity")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
}

// TestParseIndexAnnotation_NoTypeNoGranularity confirms that the new fields
// default cleanly when omitted, so existing user code without these keys
// continues to behave exactly as before.
func TestParseIndexAnnotation_NoTypeNoGranularity(t *testing.T) {
	const src = `package fixture

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)"
	Email string

	//ptah:schema:index name="idx_users_email" fields="email" unique="true"
	_ int
}
`
	c := qt.New(t)
	db := mustParseSource(c, "fixture.go", src)
	c.Assert(db.Indexes, qt.HasLen, 1)
	idx := db.Indexes[0]
	c.Assert(idx.Type, qt.Equals, "")
	c.Assert(idx.Overrides, qt.HasLen, 0)
	c.Assert(idx.Unique, qt.IsTrue)
}
