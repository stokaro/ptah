package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// TestParse_RowTTLIsACockroachDBPlatformProperty pins the Go annotation
// spelling: the parameters are platform.cockroachdb properties the owner
// decodes, and the source claims complete knowledge of row-level TTL, so a
// table without the properties requests no TTL.
func TestParse_RowTTLIsACockroachDBPlatformProperty(t *testing.T) {
	c := qt.New(t)

	database, err := goschema.ParseSource("sessions.go", `package entities

//ptah:schema:table name="sessions" platform.cockroachdb.ttl_expire_after="3 days" platform.crdb.ttl_job_cron="@daily"
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
}

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
}
`)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"cockroachdb": {"ttl_expire_after": "3 days"}, "crdb": {"ttl_job_cron": "@daily"},
	})
	c.Assert(database.Tables[0].Facets.IsZero(), qt.IsTrue)
	events := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "events")
	c.Assert(database.FeatureCoverage.Lookup(crdbschema.RowTTLKind, events).State, qt.Equals, schemaext.Complete)
}

// TestParse_RowTTLHasNoBareAttribute pins that the storage parameter names are
// not attributes of the table directive: written without the platform
// prefix, one is an unknown attribute.
func TestParse_RowTTLHasNoBareAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := goschema.ParseSource("sessions.go", `package entities

//ptah:schema:table name="sessions" ttl_expire_after="3 days"
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
}
`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(err, qt.ErrorMatches, `(?s).*ttl_expire_after.*`)
}
