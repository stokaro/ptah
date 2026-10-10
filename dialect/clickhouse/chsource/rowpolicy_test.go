package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
)

// clickhouseOwner selects this owner alone, so each test reads the row policy
// directive through the frontend the way a runtime that registers the owner
// does.
func clickhouseOwner(c *qt.C) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(chsource.Annotations())
	c.Assert(err, qt.IsNil)
	return set
}

// rowPolicySource is a table orders and one row policy directive.
func rowPolicySource(directive string) string {
	return "package models\n\n//ptah:schema:table name=\"orders\"\n" + directive + "\ntype Order struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"UInt64\" primary=\"true\"\n\tID uint64\n}\n"
}

// TestParseSource_RowPolicyDirective_HappyPath reads the directive as the
// ClickHouse owner's row policy, bound to the clickhouse target: the filter,
// the composition in either letter case, the role selection, the struct it is
// written on, and a database the table names.
func TestParseSource_RowPolicyDirective_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		wantRef   schemaext.Object
	}{
		{
			name: "every attribute",
			directive: `//ptah:schema:rowpolicy name="tenant" table="orders" using="tenant_id = 1" ` +
				`to="ALL EXCEPT admin" as="restrictive"`,
			wantRef: schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "tenant"), Targets: []string{"clickhouse"},
				Value: &chschema.DesiredRowPolicy{Filter: new("tenant_id = 1"), Composition: chschema.Restrictive,
					Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}, StructName: "Order"}},
		},
		{
			name:      "nothing but the identity",
			directive: `//ptah:schema:rowpolicy name="open" table="analytics.orders" as="PERMISSIVE"`,
			wantRef: schemaext.Object{Ref: chschema.RowPolicyRef("analytics", "orders", "open"), Targets: []string{"clickhouse"},
				Value: &chschema.DesiredRowPolicy{Composition: chschema.Permissive, StructName: "Order"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(clickhouseOwner(c), "orders.go", rowPolicySource(test.directive))

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(db.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{test.wantRef})
			c.Assert(db.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "orders", "other")).State,
				qt.Equals, schemaext.Complete)
		})
	}
}

// TestParseSource_RowPolicyDirective_FailurePath refuses a value ClickHouse
// would not take.
func TestParseSource_RowPolicyDirective_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		wantErr   string
	}{
		{name: "a composition", directive: `//ptah:schema:rowpolicy name="p" table="orders" as="strict"`,
			wantErr: `(?s).*a row policy is PERMISSIVE or RESTRICTIVE, not "strict".*`},
		{name: "a role selection", directive: `//ptah:schema:rowpolicy name="p" table="orders" to="CURRENT_USER"`,
			wantErr: `(?s).*CURRENT_USER is a keyword.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(clickhouseOwner(c), "orders.go", rowPolicySource(test.directive))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestParseSource_RowPolicyDirective_RefusesItsGrammar refuses an attribute
// the directive does not declare, such as a write check, which ClickHouse
// would accept and discard, and a directive without its table.
func TestParseSource_RowPolicyDirective_RefusesItsGrammar(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		wantErr   string
	}{
		{name: "a write check", directive: `//ptah:schema:rowpolicy name="p" table="orders" with_check="true"`,
			wantErr: `unknown annotation attribute "with_check" on //ptah:schema:rowpolicy at Order`},
		{name: "no table", directive: `//ptah:schema:rowpolicy name="p"`,
			wantErr: `missing required annotation attribute "table" on //ptah:schema:rowpolicy at Order`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(clickhouseOwner(c), "orders.go", rowPolicySource(test.directive))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestYAML_ClaimsEveryRowPolicy makes the owner's claim on a YAML document:
// selected, a policy the document leaves out is absent; not selected, it is
// unknown, so a comparison keeps it.
func TestYAML_ClaimsEveryRowPolicy(t *testing.T) {
	c := qt.New(t)
	document := []byte("tables:\n  docs:\n    columns:\n      id: { type: UInt64, primary: true }\n")
	other := chschema.RowPolicyRef("", "docs", "other")

	selected := must.Must(yamlschema.Parse(must.Must(yamlext.NewSet(chsource.YAML())), document))
	unselected := must.Must(yamlschema.Parse(yamlext.None(), document))

	c.Assert(selected.FeatureCoverage.Lookup(chschema.RowPolicyKind, other).State, qt.Equals, schemaext.Complete)
	c.Assert(unselected.FeatureCoverage.Lookup(chschema.RowPolicyKind, other).State, qt.Not(qt.Equals), schemaext.Complete)
}
