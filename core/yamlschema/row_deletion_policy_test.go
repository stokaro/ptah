package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestParse_RowDeletionPolicyIsAPlatformGroup pins the YAML spelling: the
// Spanner policy and the YDB TTL sit in the table's spanner and ydb platform
// groups. The claims belong to their owners, which spannersource and
// ydbsource test; a parse that selects neither claims no knowledge of the YDB
// TTL.
func TestParse_RowDeletionPolicyIsAPlatformGroup(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(noOwners, []byte(`
tables:
  events:
    platform:
      spanner:
        row_deletion_column: created_at
        row_deletion_interval: 30 days
      ydb:
        row_deletion_column: expires
        row_deletion_interval: PT1H
        row_deletion_unit: nanoseconds
    columns:
      id: { type: bigint, primary: true }
  plain:
    columns:
      id: { type: bigint, primary: true }
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Name, qt.Equals, "events")
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"spanner": {"row_deletion_column": "created_at", "row_deletion_interval": "30 days"},
		"ydb":     {"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "nanoseconds"},
	})
	ydb := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "plain")
	c.Assert(db.FeatureCoverage.Lookup(ydbschema.TTLKind, ydb).State, qt.Not(qt.Equals), schemaext.Complete)
}

// TestParse_RowDeletionPolicyHasNoBareKey pins that the policy is not a key of
// the table itself: a document that writes it there is refused.
func TestParse_RowDeletionPolicyHasNoBareKey(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(noOwners, []byte(`
tables:
  events:
    row_deletion_column: created_at
    row_deletion_interval: P30D
    columns:
      id: { type: bigint, primary: true }
`))

	c.Assert(err, qt.ErrorMatches, `(?s).*row_deletion_column.*`)
	c.Assert(db, qt.IsNil)
}
