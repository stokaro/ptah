package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// readPool is a pool as the reader describes one created with a limit of ten
// queries: every other setting unset, which .sys/resource_pools reports as -1
// and the reader as nil.
func readPool(name string) catalog.ResourcePool {
	return catalog.ResourcePool{Name: name, Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10))}}
}

// A declaration and a read of the same pool or classifier compare equal, and a
// difference is reported once per object, with both sides. A pool or a
// classifier only the database holds is never a removal: both belong to the
// whole database, which other applications may share.
func TestCompare_ResourcePools(t *testing.T) {
	everyone := ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 1000}
	tests := []struct {
		name                string
		desired             *schemamodel.Database
		current             *catalog.Database
		added               []string
		modified            []difftypes.ResourcePoolDiff
		classifiersAdded    []string
		classifiersModified []difftypes.ResourcePoolClassifierDiff
	}{
		{
			name: "the same pool and classifier",
			desired: &schemamodel.Database{
				ResourcePools: []schemamodel.ResourcePool{{Name: "batch",
					Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10))}}},
				ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{Name: "all", Spec: everyone}},
			},
			current: &catalog.Database{
				ResourcePools:           []catalog.ResourcePool{readPool("batch")},
				ResourcePoolClassifiers: []catalog.ResourcePoolClassifier{{Name: "all", Spec: everyone}},
			},
		},
		{
			name: "objects only the declaration has",
			desired: &schemamodel.Database{
				ResourcePools:           []schemamodel.ResourcePool{{Name: "batch"}},
				ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{Name: "all", Spec: everyone}},
			},
			current:          &catalog.Database{},
			added:            []string{"batch"},
			classifiersAdded: []string{"all"},
		},
		{
			name:    "objects only the database has, which no comparison drops",
			desired: &schemamodel.Database{},
			current: &catalog.Database{
				ResourcePools:           []catalog.ResourcePool{readPool("batch"), {Name: "default"}},
				ResourcePoolClassifiers: []catalog.ResourcePoolClassifier{{Name: "all", Spec: everyone}},
			},
		},
		{
			name: "another limit, and another pool and rank for the classifier",
			desired: &schemamodel.Database{
				ResourcePools: []schemamodel.ResourcePool{{Name: "batch",
					Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(20))}}},
				ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{Name: "all",
					Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 5}}},
			},
			current: &catalog.Database{
				ResourcePools:           []catalog.ResourcePool{readPool("batch")},
				ResourcePoolClassifiers: []catalog.ResourcePoolClassifier{{Name: "all", Spec: everyone}},
			},
			modified: []difftypes.ResourcePoolDiff{{Name: "batch",
				Desired: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(20))},
				Current: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10))}}},
			classifiersModified: []difftypes.ResourcePoolClassifierDiff{{Name: "all", RankChanged: true,
				Desired: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 5}, Current: everyone}},
		},
		{
			name: "a declared default, changed in place",
			desired: &schemamodel.Database{ResourcePools: []schemamodel.ResourcePool{{Name: "default",
				Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}}}},
			current: &catalog.Database{ResourcePools: []catalog.ResourcePool{{Name: "default"}}},
			modified: []difftypes.ResourcePoolDiff{{Name: "default",
				Desired: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(test.desired, test.current, platform.YDB)

			c.Assert(names(diff.ResourcePoolsAdded), qt.DeepEquals, test.added)
			c.Assert(diff.ResourcePoolsRemoved, qt.HasLen, 0)
			c.Assert(diff.ResourcePoolsModified, qt.DeepEquals, test.modified)
			c.Assert(classifierNames(diff.ResourcePoolClassifiersAdded), qt.DeepEquals, test.classifiersAdded)
			c.Assert(diff.ResourcePoolClassifiersRemoved, qt.HasLen, 0)
			c.Assert(diff.ResourcePoolClassifiersModified, qt.DeepEquals, test.classifiersModified)
			c.Assert(diff.HasChanges(), qt.Equals,
				len(test.added)+len(test.modified)+len(test.classifiersAdded)+len(test.classifiersModified) > 0)
		})
	}
}

// A read that left the pools out -- a dev realm's, whose pools are its
// database's -- does not decide that a declared one is missing: the addition
// is withheld and reported, since CREATE RESOURCE POOL over a pool that is
// there fails.
func TestCompare_ResourcePools_Coverage(t *testing.T) {
	c := qt.New(t)
	outside := coverage.Set{}.With(
		coverage.Object{Kind: coverage.ResourcePool, Reason: coverage.OutsideScope},
		coverage.Object{Kind: coverage.ResourcePoolClassifier, Reason: coverage.OutsideScope},
	)

	diff, undecided := schemadiff.CompareReportingUndecidedAdditions(
		&schemamodel.Database{
			ResourcePools: []schemamodel.ResourcePool{{Name: "batch"}},
			ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{Name: "all",
				Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 1}}},
		},
		&catalog.Database{NotDescribed: outside},
		&config.CompareOptions{Dialect: platform.YDB},
	)

	c.Assert(diff.ResourcePoolsAdded, qt.HasLen, 0)
	c.Assert(diff.ResourcePoolClassifiersAdded, qt.HasLen, 0)
	c.Assert(undecided, qt.DeepEquals, []coverage.Object{
		{Kind: coverage.ResourcePool, Name: "batch", Reason: coverage.OutsideScope},
		{Kind: coverage.ResourcePoolClassifier, Name: "all", Reason: coverage.OutsideScope},
	})
}

func names(pools difftypes.ResourcePoolChanges) []string {
	var out []string
	for _, pool := range pools {
		out = append(out, pool.Name)
	}
	return out
}

func classifierNames(classifiers difftypes.ResourcePoolClassifierChanges) []string {
	var out []string
	for _, classifier := range classifiers {
		out = append(out, classifier.Name)
	}
	return out
}
