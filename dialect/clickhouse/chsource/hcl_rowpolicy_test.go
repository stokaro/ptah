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

// TestHCLRowPolicyExportRefusesIncompleteKnowledge refuses an export of a
// read that did not describe the row policies as a whole, or did not inspect
// one: the document claims every policy, so applying it would drop the ones
// the read could not see.
func TestHCLRowPolicyExportRefusesIncompleteKnowledge(t *testing.T) {
	ref := chschema.RowPolicyRef("", "orders", "p")
	complete := schemaext.Knowledge{State: schemaext.Complete}
	tests := []struct {
		name     string
		coverage schemaext.Coverage
	}{
		{name: "the model uninspected", coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))},
		{name: "the model unrepresentable", coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "not read"}, nil))},
		{name: "one policy uninspected", coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, complete, []schemaext.SubjectCoverage{
			{Kind: chschema.RowPolicyKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}},
		}))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := atlashclrender.Render(&schemamodel.Database{FeatureCoverage: test.coverage}, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.ClickHouse})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*cannot be exported.*not read.*`)
			c.Assert(encoded.Data, qt.IsNil)
		})
	}
}

// TestHCLRowPolicyExportNamesAPolicyNoBlockDeclares exports a read holding a
// policy it met and no block can declare, such as one written ON db.*: the
// document declares no block for it and says so in its header, as a SQL
// export says so in a comment.
func TestHCLRowPolicyExportNamesAPolicyNoBlockDeclares(t *testing.T) {
	c := qt.New(t)
	coverage := must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("d1", "*", "tenant"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the policy is written ON d1.*"}},
	}))

	encoded, err := atlashclrender.Render(&schemamodel.Database{FeatureCoverage: coverage}, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.ClickHouse})

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded.Data), qt.Contains,
		"// ptah.run/clickhouse/row-policy d1.*.tenant was not described (the policy is written ON d1.*), and no block declares it\n")
	c.Assert(string(encoded.Data), qt.Not(qt.Contains), "row_policy")
}

func TestHCLRowPolicyRefusesStrictCEAndKeepsFullMode(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`row_policy "p" { on = table.orders }`), "policy.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(atlascompatpolicy.StrictCE().ValidateDesiredSchema(db), qt.ErrorMatches, `(?s).*desired schema object.*strict compatibility does not support database dialect "clickhouse"`)
	c.Assert(atlascompatpolicy.Full().ValidateDesiredSchema(db), qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
}
