package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// TestParse_RowTTLIsACockroachDBPlatformGroup pins the YAML spelling: the
// parameters sit in the table's cockroachdb platform group, and the document
// claims complete knowledge of row-level TTL, so a table without the group
// requests no TTL.
func TestParse_RowTTLIsACockroachDBPlatformGroup(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
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
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[1].Name, qt.Equals, "sessions")
	c.Assert(db.Tables[1].Overrides, qt.DeepEquals, map[string]map[string]string{
		"cockroachdb": {"ttl_expiration_expression": "expires_at", "ttl_select_batch_size": "500"},
	})
	events := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "events")
	c.Assert(db.FeatureCoverage.Lookup(crdbschema.RowTTLKind, events).State, qt.Equals, schemaext.Complete)
}
