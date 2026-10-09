package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestClassify_ResourcePool judges what a YDB resource pool statement
// changes: a drop removes no data and moves the queries a classifier sends to
// the pool into the pool default, and a dropped classifier sends its member's
// queries elsewhere; a creation changes nothing a query meets yet.
func TestClassify_ResourcePool(t *testing.T) {
	tests := []struct {
		name         string
		node         ast.Node
		wantSubject  string
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "a pool dropped", node: &ast.ExtensionStatement{Payload: &ydbast.ResourcePool{Operation: ydbast.PoolDrop, Name: "batch"}},
			wantSubject: "batch", wantSeverity: safety.Warning,
			wantReason: "DROP RESOURCE POOL runs the queries a classifier sends to the pool in the pool default",
		},
		{
			wantSubject: "etl", name: "a classifier dropped", node: &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolDrop, Name: "etl"}},
			wantSeverity: safety.Warning,
			wantReason: "DROP RESOURCE POOL CLASSIFIER sends its member's queries to another classifier's pool " +
				"or to the pool default",
		},
		{
			wantSubject: "batch", name: "a pool created", node: &ast.ExtensionStatement{Payload: &ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: "batch", Spec: &ast.ResourcePoolSpec{}}},
			wantSeverity: safety.Safe,
			wantReason:   "does not remove data or tighten constraints",
		},
		{
			wantSubject: "batch", name: "a pool altered", node: &ast.ExtensionStatement{Payload: &ydbast.ResourcePool{Operation: ydbast.PoolAlter, Name: "batch", Spec: &ast.ResourcePoolSpec{ResourceWeight: new(25.0)}, Previous: &ast.ResourcePoolSpec{}}},
			wantSeverity: safety.Warning, wantReason: "ALTER RESOURCE POOL changes the limits of running and queued queries",
		},
		{
			wantSubject: "etl", name: "a classifier created", node: &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: "etl", Spec: &ast.ResourcePoolClassifierSpec{ResourcePool: "batch"}}},
			wantSeverity: safety.Warning, wantReason: "resource pool classifier settings change which pool receives matching queries",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{test.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
			c.Assert(assessments[0].Subject, qt.Equals, test.wantSubject)
		})
	}
}

// A diff that changes which pool a query runs in is a warning, and one that
// only adds a pool nothing routes to is safe.
func TestClassifySchemaDiff_ResourcePools(t *testing.T) {
	c := qt.New(t)

	added := safety.ClassifySchemaDiff(&difftypes.SchemaDiff{
		ResourcePoolsAdded: difftypes.ResourcePoolChanges{{Name: "batch"}},
	})
	routed := safety.ClassifySchemaDiff(&difftypes.SchemaDiff{
		ResourcePoolsAdded:           difftypes.ResourcePoolChanges{{Name: "batch"}},
		ResourcePoolClassifiersAdded: difftypes.ResourcePoolClassifierChanges{{Name: "etl"}},
	})

	c.Assert(safety.Highest(added), qt.Equals, safety.Safe)
	c.Assert(safety.Highest(routed), qt.Equals, safety.Warning)
}
