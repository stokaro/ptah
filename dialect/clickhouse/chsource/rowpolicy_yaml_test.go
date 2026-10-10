package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
)

// yamlOwner selects this owner alone, so each test reads the row_policies key
// through the frontend the way a runtime that registers the owner does.
var yamlOwner = must.Must(yamlext.NewSet(chsource.YAML()))

// rowPolicyDocument is a table orders in the analytics database, and the
// given row_policies entries.
func rowPolicyDocument(policies string) []byte {
	return []byte("tables:\n  orders:\n    schema: analytics\n    struct_name: Order\n    columns:\n" +
		"      id: { type: UInt64, primary: true }\nrow_policies:\n" + policies)
}

// TestParse_RowPoliciesKey_HappyPath reads the row_policies key as the
// directive reads its attributes. A table the document declares gives the
// policy its database, whether the entry names it or its struct; a table it
// leaves to the database is named as written; and the entry's key is the
// policy name unless the entry names one.
func TestParse_RowPoliciesKey_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		policies string
		want     schemaext.Object
	}{
		{
			name: "every attribute",
			policies: "  tenant:\n    table: orders\n    using: tenant_id = 1\n    to: ALL EXCEPT admin\n" +
				"    as: restrictive\n",
			want: schemaext.Object{Ref: chschema.RowPolicyRef("analytics", "orders", "tenant"), Targets: []string{"clickhouse"},
				Value: &chschema.DesiredRowPolicy{Filter: new("tenant_id = 1"), Composition: chschema.Restrictive,
					Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}},
		},
		{
			name:     "a table named by its struct, and a name of its own",
			policies: "  tenant:\n    name: open\n    struct_name: Order\n    as: PERMISSIVE\n",
			want: schemaext.Object{Ref: chschema.RowPolicyRef("analytics", "orders", "open"), Targets: []string{"clickhouse"},
				Value: &chschema.DesiredRowPolicy{Composition: chschema.Permissive, StructName: "Order"}},
		},
		{
			name:     "a table the document leaves to the database",
			policies: "  tenant:\n    table: events.raw\n",
			want: schemaext.Object{Ref: chschema.RowPolicyRef("events", "raw", "tenant"), Targets: []string{"clickhouse"},
				Value: &chschema.DesiredRowPolicy{}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(yamlOwner, rowPolicyDocument(test.policies))

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(db.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{test.want})
		})
	}
}

// TestParse_RowPoliciesKey_FailurePath refuses what the directive refuses,
// and a key the entry does not take, at its line: the write check
// ClickHouse would accept and discard is no key of a row policy.
func TestParse_RowPoliciesKey_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		policies string
		wantErr  string
	}{
		{name: "a write check", policies: "  tenant:\n    table: orders\n    with_check: \"true\"\n",
			wantErr: `(?s)parse YAML schema: .*line 10: field with_check not found.*`},
		{name: "no table", policies: "  tenant:\n    using: \"true\"\n",
			wantErr: `row_policies\.tenant: .*a row policy names its table`},
		{name: "a composition ClickHouse does not have", policies: "  tenant:\n    table: orders\n    as: STRICT\n",
			wantErr: `row_policies\.tenant: .*a row policy is PERMISSIVE or RESTRICTIVE, not "STRICT"`},
		{name: "one policy declared twice", policies: "  first:\n    name: tenant\n    table: orders\n" +
			"  second:\n    name: tenant\n    table: orders\n",
			wantErr: `row_policies\.second: .*row_policies\.first and row_policies\.second both declare row policy "tenant".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(yamlOwner, rowPolicyDocument(test.policies))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParse_RowPoliciesKey_RefusesAnInvalidValueAsSuch pins the sentinel a
// value the policy cannot carry is refused with.
func TestParse_RowPoliciesKey_RefusesAnInvalidValueAsSuch(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse(yamlOwner, rowPolicyDocument("  tenant:\n    table: orders\n    as: STRICT\n"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
}

// TestParse_WithoutTheOwnerRowPoliciesIsUnknown is the control on the
// selection: without the owner, row_policies is a key the document does not
// know, refused rather than dropped.
func TestParse_WithoutTheOwnerRowPoliciesIsUnknown(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(yamlext.None(), rowPolicyDocument("  tenant:\n    table: orders\n"))

	c.Assert(err, qt.ErrorMatches, `parse YAML schema: line 7: unknown key "row_policies"`)
	c.Assert(db, qt.IsNil)
}
