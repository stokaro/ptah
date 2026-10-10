package spannersource_test

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
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/spanner/spannersource"
)

const events = `package entities

//ptah:schema:table name="events" platform.spanner.row_deletion_column="created_at" platform.spanner.row_deletion_interval="30 days"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:table name="plain"
type Plain struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`

// TestAnnotations_ClaimRowDeletionForAGoSource pins the Go annotation
// spelling: the policy is a platform.spanner property the owner decodes once
// a target is selected, and a parse that selects the owner claims complete
// knowledge of the policy, so a table without the properties requests none.
// Without the owner the parse claims nothing, and an existing policy is left
// unmanaged rather than read as one to drop.
func TestAnnotations_ClaimRowDeletionForAGoSource(t *testing.T) {
	tests := []struct {
		name   string
		owners []annotation.Extension
		state  schemaext.KnowledgeState
	}{
		{name: "the owner selected", owners: []annotation.Extension{spannersource.Annotations()}, state: schemaext.Complete},
		{name: "no owner selected", state: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := annotation.NewSet(test.owners...)
			c.Assert(err, qt.IsNil)

			database, err := goschema.ParseSource(set, "events.go", events)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
				"spanner": {"row_deletion_column": "created_at", "row_deletion_interval": "30 days"},
			})
			c.Assert(database.Tables[0].Facets.IsZero(), qt.IsTrue)
			plain := objectidentity.NewBuilder(identifier.ForDialect("spanner")).TableParts("", "plain")
			c.Assert(database.FeatureCoverage.Lookup(spannerschema.RowDeletionKind, plain).State, qt.Equals, test.state)
		})
	}
}

// TestAnnotations_RowDeletionPolicyHasNoBareAttribute pins that the policy's
// property names are not attributes of the table directive: written without a
// platform prefix, which would not say whose spelling the interval is in, one
// is an unknown attribute.
func TestAnnotations_RowDeletionPolicyHasNoBareAttribute(t *testing.T) {
	c := qt.New(t)
	owner, err := annotation.NewSet(spannersource.Annotations())
	c.Assert(err, qt.IsNil)

	_, err = goschema.ParseSource(owner, "events.go", `package entities

//ptah:schema:table name="events" row_deletion_column="created_at" row_deletion_interval="P30D"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(err, qt.ErrorMatches, `(?s).*row_deletion_(column|interval).*`)
}

const eventsYAML = `
tables:
  events:
    platform:
      spanner:
        row_deletion_column: created_at
        row_deletion_interval: 30 days
    columns:
      id: { type: bigint, primary: true }
  plain:
    columns:
      id: { type: bigint, primary: true }
`

// TestYAML_ClaimsRowDeletionForAYAMLDocument pins the YAML spelling: the policy
// sits in the table's spanner platform group, and a parse that selects the
// owner claims complete knowledge of it, so a table without the group requests
// none. Without the owner the document claims nothing.
func TestYAML_ClaimsRowDeletionForAYAMLDocument(t *testing.T) {
	tests := []struct {
		name   string
		owners []yamlext.Extension
		state  schemaext.KnowledgeState
	}{
		{name: "the owner selected", owners: []yamlext.Extension{spannersource.YAML()}, state: schemaext.Complete},
		{name: "no owner selected", state: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := yamlext.NewSet(test.owners...)
			c.Assert(err, qt.IsNil)

			db, err := yamlschema.Parse(set, []byte(eventsYAML))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
				"spanner": {"row_deletion_column": "created_at", "row_deletion_interval": "30 days"},
			})
			plain := objectidentity.NewBuilder(identifier.ForDialect("spanner")).TableParts("", "plain")
			c.Assert(db.FeatureCoverage.Lookup(spannerschema.RowDeletionKind, plain).State, qt.Equals, test.state)
		})
	}
}
