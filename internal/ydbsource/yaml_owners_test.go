package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
)

const ownedDocument = `tables:
  events:
    key_bloom_filter: ENABLED
    changefeeds:
      feed: { mode: UPDATES, format: JSON }
    columns:
      id: { type: Int64, primary: true }
secrets:
  pw: { value_env: PTAH_SECRET_PW }
`

// TestParse_TheYDBOwnerReadsItsKeysAndClaims pins what selecting the owner
// adds to a YAML document: its objects, a table's settings, and the claim
// that the document describes each YDB namespace and every table's TTL.
func TestParse_TheYDBOwnerReadsItsKeysAndClaims(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(ydbYAMLOwners, []byte(ownedDocument))

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 2)
	c.Assert(db.Tables[0].Facets.DeclaredKinds(), qt.DeepEquals, []schemaext.Kind{ydbschema.TablePartitioningKind})
	events := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "events")
	c.Assert(db.FeatureCoverage.Lookup(ydbschema.TTLKind, events).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "other")).State, qt.Equals, schemaext.Complete)
}

// TestParse_WithoutTheYDBOwnerItsKeysAreUnknown is the control on the
// selection: without the owner, a YDB key is one the document does not know,
// refused rather than dropped, whether it sits at the top of the document or
// in a table.
func TestParse_WithoutTheYDBOwnerItsKeysAreUnknown(t *testing.T) {
	tests := []struct {
		name, document, wantErr string
	}{
		{name: "a top-level key", document: "secrets:\n  pw: { value_env: PTAH_SECRET_PW }\n",
			wantErr: `parse YAML schema: line 1: unknown key "secrets"`},
		{name: "a table key", document: "tables:\n  events:\n    key_bloom_filter: ENABLED\n",
			wantErr: `parse YAML schema: line 3: unknown key "key_bloom_filter" of table "events"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(yamlext.None(), []byte(test.document))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
