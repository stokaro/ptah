package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

func TestHCLRowPolicyRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name, attributes string
		want             chschema.DesiredRowPolicy
	}{
		{"defaults", "", chschema.DesiredRowPolicy{}},
		{"filter and all except", `using = sql("tenant = 1")
 to = "ALL EXCEPT admin"
 as = "RESTRICTIVE"`, chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}},
		{"all", `to = "ALL"`, chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true}}},
		{"none", `to = "NONE"
 as = "PERMISSIVE"`, chschema.DesiredRowPolicy{Composition: chschema.Permissive}},
		{"literal keyword names", "to = \"`ALL`, ` my role `, `a``b`\"", chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"ALL", " my role ", "a`b"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := `schema "a.b" {}
 table "orders" {
 schema = schema["a.b"]
 column "tenant" { type = UInt64 }
 }
 row_policy " p.name " {
 on = table.orders
 ` + test.attributes + "\n}"
			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(source), "policy.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			want := []schemaext.Object{{Ref: chschema.RowPolicyRef("a.b", "orders", " p.name "), Value: &test.want, Targets: []string{platform.ClickHouse}}}
			c.Assert(must.Must(db.FeatureObjects.All()), qt.DeepEquals, want)
			c.Assert(db.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("a.b", "orders", "absent")).State, qt.Equals, schemaext.Complete)
			encoded, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.ClickHouse})
			c.Assert(err, qt.IsNil)
			c.Assert(encoded.Diagnostics, qt.HasLen, 0)
			again, err := atlashcl.ParseWithOptions(t.Context(), encoded.Data, "export.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(again.FeatureObjects.All()), qt.DeepEquals, want)
		})
	}
}

func TestHCLRowPolicyNamesAreScopedToTheirTables(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`row_policy "tenant" { on = table.app.orders }
 row_policy "tenant" { on = table.app.invoices }
 row_policy "tenant" { on = table.other.orders }`), "policies.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 3)
	encoded, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.ClickHouse})
	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Diagnostics, qt.HasLen, 0)
	again, err := atlashcl.ParseWithOptions(t.Context(), encoded.Data, "export.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(again.FeatureObjects, qt.DeepEquals, db.FeatureObjects)
}

func TestHCLRowPolicyRefusals(t *testing.T) {
	for _, test := range []struct{ name, source, reason string }{
		{"missing table", `row_policy "p" { to = "ALL" }`, `requires on`},
		{"wrong table reference", `row_policy "p" { on = role.orders }`, `requires on`},
		{"composition typo", "row_policy \"p\" {\n on = table.orders\n as = \"RESTRICTVE\"\n}", `PERMISSIVE or RESTRICTIVE`},
		{"write check", "row_policy \"p\" {\n on = table.orders\n check = \"tenant = 1\"\n}", `unsupported row_policy attribute "check"`},
		{"command", "row_policy \"p\" {\n on = table.orders\n for = \"UPDATE\"\n}", `unsupported row_policy attribute "for"`},
		{"comment", "row_policy \"p\" {\n on = table.orders\n comment = \"text\"\n}", `unsupported row_policy attribute "comment"`},
		{"recipient type", "row_policy \"p\" {\n on = table.orders\n to = [\"alice\"]\n}", `must be a string`},
		{"current user", "row_policy \"p\" {\n on = table.orders\n to = \"CURRENT_USER\"\n}", `CURRENT_USER`},
		{"duplicate identity", "row_policy \"p\" { on = table.orders }\nrow_policy \"p\" { on = table.orders }", `both declare row policy`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(test.source), "policy.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}

func TestHCLRowPolicyRequiresSelectedOwner(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`row_policy "p" { on = table.orders }`), "policy.hcl", atlashcl.Options{HCL: hclext.None()})
	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported top-level block "row_policy".*`)
	c.Assert(db, qt.IsNil)
	empty, err := atlashcl.ParseWithOptions(t.Context(), nil, "empty.hcl", atlashcl.Options{HCL: hclext.None()})
	c.Assert(err, qt.IsNil)
	c.Assert(empty.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "orders", "p")).State, qt.Equals, schemaext.Uninspected)
}

func TestHCLRowPolicyExportRefusesIncompleteKnowledge(t *testing.T) {
	ref := chschema.RowPolicyRef("", "orders", "p")
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
		for _, coverage := range []schemaext.Coverage{
			must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: state, Reason: "not read"}, nil)),
			must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: chschema.RowPolicyKind, Subject: ref, Knowledge: schemaext.Knowledge{State: state, Reason: "not read"}},
			})),
		} {
			c := qt.New(t)
			encoded, err := atlashclrender.Render(&schemamodel.Database{FeatureCoverage: coverage}, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.ClickHouse})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*cannot be exported.*not read.*`)
			c.Assert(encoded.Data, qt.IsNil)
		}
	}
}

func TestHCLRowPolicyRefusesStrictCEAndKeepsFullMode(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`row_policy "p" { on = table.orders }`), "policy.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(atlascompatpolicy.StrictCE().ValidateDesiredSchema(db), qt.ErrorMatches, `(?s).*desired schema object.*strict compatibility does not support database dialect "clickhouse"`)
	c.Assert(atlascompatpolicy.Full().ValidateDesiredSchema(db), qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
}
