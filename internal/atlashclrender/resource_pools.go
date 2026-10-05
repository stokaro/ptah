package atlashclrender

import (
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
)

// reportResourcePools names every YDB resource pool and classifier the
// document leaves out. Atlas HCL has no block for either, and Ptah does not
// invent one, so each is a loss the export says out loud: `ptah schema export
// --cleanup-go-annotations` refuses to delete annotations a loss diagnostic
// names.
func (r *renderer) reportResourcePools() {
	for _, pool := range r.db.ResourcePools {
		r.warn("resource_pools."+pool.Name, "a YDB resource pool is not represented in HCL")
	}
	for _, classifier := range r.db.ResourcePoolClassifiers {
		r.warn("resource_pool_classifiers."+classifier.Name,
			"a YDB resource pool classifier is not represented in HCL")
	}
}

// resourcePoolsNotDescribed records that the document does not describe
// resource pools or classifiers, so applying it back does not read their
// absence as a request about them.
//
// The record is made for every YDB render, whether or not the schema holds a
// pool, because the document read against another YDB database cannot say
// that one holds none. A render for another dialect records it only when the
// schema holds a pool or a classifier, which a declaration written for YDB
// can and a read of another engine cannot.
func (r *renderer) resourcePoolsNotDescribed() coverage.Set {
	held := r.db != nil && len(r.db.ResourcePools)+len(r.db.ResourcePoolClassifiers) > 0
	if platform.NormalizeDialect(r.dialect) != platform.YDB && !held {
		return coverage.Set{}
	}
	return coverage.Set{}.With(
		coverage.Object{Kind: coverage.ResourcePool, Reason: coverage.Unsupported, Provenance: coverage.Defaulted},
		coverage.Object{
			Kind: coverage.ResourcePoolClassifier, Reason: coverage.Unsupported, Provenance: coverage.Defaulted,
		},
	)
}
