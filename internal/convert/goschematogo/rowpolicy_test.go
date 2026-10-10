package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// rowPolicySchema declares orders with a restrictive row policy for every
// user but one, and a permissive one stating nothing, as a ClickHouse read
// converted to a declaration holds them.
func rowPolicySchema(coverage schemaext.Knowledge) *schemamodel.Database {
	tenant := must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", "tenant"), chschema.DesiredRowPolicy{
		Filter: new("tenant_id = 1"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}},
	}))
	open := must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", "open"), chschema.DesiredRowPolicy{
		Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"alice"}},
	}))
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", FieldName: "ID", Name: "id", Type: "UInt64", Primary: true},
		},
		FeatureObjects:  must.Must(schemaext.NewObjects(tenant, open)),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, coverage, nil)),
	}
}

func renderClickHouse(c *qt.C, db *schemamodel.Database) (string, error) {
	c.Helper()
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "clickhouse", Runtime: must.Must(builtin.New())})
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	return source.String(), err
}

// TestRender_WritesRowPoliciesAsTheOwnersDirective writes each ClickHouse row
// policy as the owner's directive beside its table, with its composition, so
// a restrictive policy reads back restrictive (stokaro/ptah#4343), and reads
// back as the same policies.
func TestRender_WritesRowPoliciesAsTheOwnersDirective(t *testing.T) {
	c := qt.New(t)
	declared := rowPolicySchema(schemaext.Knowledge{State: schemaext.Complete})

	source, err := renderClickHouse(c, declared)

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Contains, `//ptah:schema:rowpolicy name="tenant" table="orders" using="tenant_id = 1" to="ALL EXCEPT admin" as="RESTRICTIVE"`)
	c.Assert(source, qt.Contains, `//ptah:schema:rowpolicy name="open" table="orders" to="alice" as="PERMISSIVE"`)
	reparsed := must.Must(goschema.ParseSource(builtintest.Annotations(), "schema.go", source))
	read := must.Must(reparsed.FeatureObjects.All())
	c.Assert(read, qt.HasLen, 2)
	for _, object := range read {
		declaredObject, found, err := declared.FeatureObjects.Get(object.Ref)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		policy := object.Value.(*chschema.DesiredRowPolicy)
		policy.StructName = ""
		c.Assert(policy.Equal(declaredObject.Value), qt.IsTrue, qt.Commentf("%s", object.Ref))
	}
}

// TestRender_RowPolicies_FailurePath refuses an export of row policies the
// read did not describe: the directive claims every policy, so the export
// would declare none, and applying it would drop the server's.
func TestRender_RowPolicies_FailurePath(t *testing.T) {
	c := qt.New(t)

	source, err := renderClickHouse(c, rowPolicySchema(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the account may not read system.row_policies"}))

	c.Assert(err, qt.ErrorMatches, `.*ClickHouse row policies cannot be exported: the read did not describe them.*`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(source, qt.Equals, "")
}

// TestRender_SharedRowSecurityKeepsRestrictiveAndForce writes a shared
// declaration's composition and FORCE, so a restrictive policy and a forced
// enablement read back as they were declared (stokaro/ptah#4343).
func TestRender_SharedRowSecurityKeepsRestrictiveAndForce(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true}},
		RLSPolicies: []schemamodel.RLSPolicy{{StructName: "Order", Name: "tenant", Table: "orders", UsingExpression: "true",
			Restrictive: true, Dialects: []string{"mysql"}}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{StructName: "Order", Table: "orders", Forced: true, Dialects: []string{"mysql"}}},
	}

	files, err := goschematogo.Render(c.Context(), declared, goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	source := string(files[0].Data)
	c.Assert(source, qt.Contains, `//ptah:schema:rls:policy name="tenant" table="orders" using="true" as="RESTRICTIVE" dialects="mysql"`)
	c.Assert(source, qt.Contains, `//ptah:schema:rls:enable table="orders" force="true" dialects="mysql"`)
	reparsed := must.Must(goschema.ParseSource(builtintest.Annotations(), "schema.go", source))
	c.Assert(reparsed.RLSPolicies, qt.HasLen, 1)
	c.Assert(reparsed.RLSPolicies[0].Restrictive, qt.IsTrue)
	c.Assert(reparsed.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(reparsed.RLSEnabledTables[0].Forced, qt.IsTrue)
}
