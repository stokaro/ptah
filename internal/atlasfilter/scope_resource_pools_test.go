package atlasfilter_test

import (
	"testing"

	"ptah.run/dialect/ydb/ydbworkload"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// classifierScope keeps one YDB classifier by name and nothing else.
func classifierScope() atlasfilter.Scope {
	return atlasfilter.Scope{Include: []string{"etl_users[type=resource_pool_classifier]"}, DefaultSchema: "public"}
}

// A scope that keeps a YDB resource pool classifier keeps the pool it sends
// queries to, and no other pool: a description holding the classifier
// without its pool would name a pool it says is absent.
func TestScopeDatabase_AKeptClassifierKeepsItsPool(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		ResourcePools: []catalog.ResourcePool{{Name: "batch"}, {Name: "reports"}},
		ResourcePoolClassifiers: []catalog.ResourcePoolClassifier{
			{Name: "etl_users", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 10}},
			{Name: "analysts", Spec: ydbworkload.ClassifierSpec{ResourcePool: "reports", Rank: 20}},
		},
	}

	got, err := atlasfilter.ScopeDatabase(database, classifierScope())

	c.Assert(err, qt.IsNil)
	c.Assert(got.ResourcePoolClassifiers, qt.DeepEquals, database.ResourcePoolClassifiers[:1])
	c.Assert(got.ResourcePools, qt.DeepEquals, database.ResourcePools[:1])
}

// The same projection over a declared schema.
func TestScopeGenerated_AKeptClassifierKeepsItsPool(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		ResourcePools: []schemamodel.ResourcePool{{Name: "batch"}, {Name: "reports"}},
		ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{
			{Name: "etl_users", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 10}},
			{Name: "analysts", Spec: ydbworkload.ClassifierSpec{ResourcePool: "reports", Rank: 20}},
		},
	}

	got, _, err := atlasfilter.ScopeGeneratedSelectionReport(database, classifierScope())

	c.Assert(err, qt.IsNil)
	c.Assert(got.ResourcePoolClassifiers, qt.DeepEquals, database.ResourcePoolClassifiers[:1])
	c.Assert(got.ResourcePools, qt.DeepEquals, database.ResourcePools[:1])
}
