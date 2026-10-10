package crdbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/crdbsource"
)

const sessions = `package entities

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
`

// TestAnnotations_ClaimRowTTLForAGoSource pins the Go annotation spelling: the
// parameters are platform.cockroachdb properties the owner decodes once a
// target is selected, and a parse that selects the owner claims complete
// knowledge of row-level TTL, so a table without the properties requests no
// TTL. Without the owner the parse claims nothing, and an existing policy is
// left unmanaged rather than read as one to drop.
func TestAnnotations_ClaimRowTTLForAGoSource(t *testing.T) {
	tests := []struct {
		name   string
		owners []annotation.Extension
		state  schemaext.KnowledgeState
	}{
		{name: "the owner selected", owners: []annotation.Extension{crdbsource.Annotations()}, state: schemaext.Complete},
		{name: "no owner selected", state: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := annotation.NewSet(test.owners...)
			c.Assert(err, qt.IsNil)

			database, err := goschema.ParseSource(set, "sessions.go", sessions)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
				"cockroachdb": {"ttl_expire_after": "3 days"}, "crdb": {"ttl_job_cron": "@daily"},
			})
			c.Assert(database.Tables[0].Facets.IsZero(), qt.IsTrue)
			events := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "events")
			c.Assert(database.FeatureCoverage.Lookup(crdbschema.RowTTLKind, events).State, qt.Equals, test.state)
		})
	}
}

// TestAnnotations_RowTTLHasNoBareAttribute pins that the storage parameter
// names are not attributes of the table directive: written without the
// platform prefix, one is an unknown attribute.
func TestAnnotations_RowTTLHasNoBareAttribute(t *testing.T) {
	c := qt.New(t)
	owner, err := annotation.NewSet(crdbsource.Annotations())
	c.Assert(err, qt.IsNil)

	_, err = goschema.ParseSource(owner, "sessions.go", `package entities

//ptah:schema:table name="sessions" ttl_expire_after="3 days"
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
}
`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(err, qt.ErrorMatches, `(?s).*ttl_expire_after.*`)
}

const sessionsYAML = `
tables:
  sessions:
    platform:
      cockroachdb:
        ttl_expiration_expression: expires_at
        ttl_select_batch_size: 500
    columns:
      id: { type: INT8, primary: true }
      expires_at: { type: TIMESTAMPTZ }
  events:
    columns:
      id: { type: INT8, primary: true }
`

// TestYAML_ClaimsRowTTLForAYAMLDocument pins the YAML spelling: the parameters
// sit in the table's cockroachdb platform group, and a parse that selects the
// owner claims complete knowledge of row-level TTL, so a table without the
// group requests no TTL. Without the owner the document claims nothing.
func TestYAML_ClaimsRowTTLForAYAMLDocument(t *testing.T) {
	tests := []struct {
		name   string
		owners []yamlext.Extension
		state  schemaext.KnowledgeState
	}{
		{name: "the owner selected", owners: []yamlext.Extension{crdbsource.YAML()}, state: schemaext.Complete},
		{name: "no owner selected", state: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := yamlext.NewSet(test.owners...)
			c.Assert(err, qt.IsNil)

			db, err := yamlschema.Parse(set, []byte(sessionsYAML))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables[1].Name, qt.Equals, "sessions")
			c.Assert(db.Tables[1].Overrides, qt.DeepEquals, map[string]map[string]string{
				"cockroachdb": {"ttl_expiration_expression": "expires_at", "ttl_select_batch_size": "500"},
			})
			events := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "events")
			c.Assert(db.FeatureCoverage.Lookup(crdbschema.RowTTLKind, events).State, qt.Equals, test.state)
		})
	}
}
