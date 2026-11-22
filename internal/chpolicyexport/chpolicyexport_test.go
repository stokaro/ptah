package chpolicyexport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicyexport"
)

// policy is an observed row policy on table of database d1.
func policy(table, name string) schemaext.Object {
	return must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("d1", table, name), chschema.ObservedRowPolicy{
		Filter: new("1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true},
	}))
}

// TestWithout_LeavesOutAPolicyWhoseTableIsGone leaves out the policy the
// database kept for a dropped table, keeps the policy on a table the read
// holds, and names the statement that drops the one it left out.
func TestWithout_LeavesOutAPolicyWhoseTableIsGone(t *testing.T) {
	c := qt.New(t)
	kept, outlived := policy("orders", "tenant"), policy("gone", "p")
	schema := &catalog.Database{Tables: []catalog.Table{{Name: "orders"}}, FeatureObjects: must.Must(schemaext.NewObjects(kept, outlived))}
	var warnings bytes.Buffer

	got, err := chpolicyexport.Without(t.Context(), &warnings, schema)

	c.Assert(err, qt.IsNil)
	objects := must.Must(got.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref.Key(), qt.Equals, kept.Ref.Key())
	c.Assert(warnings.String(), qt.Equals, `warning: ClickHouse row policy "p" on table "gone" is left out: the database holds it for a table `+
		"it does not hold, and applies it to the next table created under that name; no schema can declare it, so applying a schema "+
		"written from this read drops it with "+
		"DROP ROW POLICY `p` ON `d1`.`gone`;\n")
	c.Assert(must.Must(schema.FeatureObjects.All()), qt.HasLen, 2)
}

// TestWithout_KeepsASchemaWithNoPolicyToLeaveOut returns the schema it was
// given, and says nothing, when every policy's table is in the read.
func TestWithout_KeepsASchemaWithNoPolicyToLeaveOut(t *testing.T) {
	c := qt.New(t)
	schema := &catalog.Database{Tables: []catalog.Table{{Name: "orders"}}, FeatureObjects: must.Must(schemaext.NewObjects(policy("orders", "tenant")))}
	var warnings bytes.Buffer

	got, err := chpolicyexport.Without(t.Context(), &warnings, schema)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, schema)
	c.Assert(warnings.String(), qt.Equals, "")
	nothing, err := chpolicyexport.Without(t.Context(), &warnings, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(nothing, qt.IsNil)
}

// TestReportUndescribed names each policy the read met and could not
// describe, the database-wide one by what it applies to.
func TestReportUndescribed(t *testing.T) {
	c := qt.New(t)
	coverage := must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("d1", "*", "wide"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the policy is written ON d1.*"}},
		{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("d1", "v", "on_view"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the policy is on a view"}},
	}))
	var warnings bytes.Buffer

	chpolicyexport.ReportUndescribed(t.Context(), &warnings, coverage)

	c.Assert(warnings.String(), qt.Equals,
		"warning: ClickHouse row policy \"wide\" on every table of the database is left out: the policy is written ON d1.*\n"+
			"warning: ClickHouse row policy \"on_view\" on \"v\" is left out: the policy is on a view\n")
}

// TestDeclarable leaves out of the side to reach the policy whose table is
// gone and the records of the policies the read could not describe, naming
// each, and keeps the policy on a table the read holds. Without the record, a
// lookup of the database-wide policy answers what the kind as a whole says:
// the read is complete, so the policy is not declared.
func TestDeclarable(t *testing.T) {
	c := qt.New(t)
	kept, outlived := policy("orders", "tenant"), policy("gone", "p")
	wide := chschema.RowPolicyRef("d1", "*", "wide")
	schema := &catalog.Database{
		Tables:         []catalog.Table{{Name: "orders"}},
		FeatureObjects: must.Must(schemaext.NewObjects(kept, outlived)),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
			{Kind: chschema.RowPolicyKind, Subject: wide, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the policy is written ON d1.*"}},
		})),
	}
	var warnings bytes.Buffer

	got, err := chpolicyexport.Declarable(t.Context(), &warnings, schema)

	c.Assert(err, qt.IsNil)
	objects := must.Must(got.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref.Key(), qt.Equals, kept.Ref.Key())
	c.Assert(got.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
	c.Assert(got.FeatureCoverage.Lookup(chschema.RowPolicyKind, wide).State, qt.Equals, schemaext.Complete)
	c.Assert(warnings.String(), qt.Equals, `warning: ClickHouse row policy "p" on table "gone" is left out: the database holds it for a table `+
		"it does not hold, and applies it to the next table created under that name; no schema can declare it, so applying a schema "+
		"written from this read drops it with DROP ROW POLICY `p` ON `d1`.`gone`;\n"+
		"warning: ClickHouse row policy \"wide\" on every table of the database is left out: the policy is written ON d1.*\n")
	c.Assert(schema.FeatureCoverage.SubjectRecords(), qt.HasLen, 1)
}

// TestDeclarable_KeepsASchemaWithNothingToLeaveOut returns the schema it was
// given, and says nothing, when the read describes every policy it holds.
func TestDeclarable_KeepsASchemaWithNothingToLeaveOut(t *testing.T) {
	c := qt.New(t)
	schema := &catalog.Database{Tables: []catalog.Table{{Name: "orders"}}, FeatureObjects: must.Must(schemaext.NewObjects(policy("orders", "tenant")))}
	var warnings bytes.Buffer

	got, err := chpolicyexport.Declarable(t.Context(), &warnings, schema)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, schema)
	c.Assert(warnings.String(), qt.Equals, "")
	nothing, err := chpolicyexport.Declarable(t.Context(), &warnings, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(nothing, qt.IsNil)
}
