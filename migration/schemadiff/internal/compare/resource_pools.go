package compare

import (
	"sort"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbpool"
	"ptah.run/migration/schemadiff/difftypes"
)

// ResourcePools compares declared YDB resource pools and classifiers against
// the ones the database reports, by name.
//
// A pool or classifier both sides hold is compared through [ydbpool]: a
// setting a declaration leaves out is the unset value the database reports
// for one never set. One only the declaration holds is a creation where the
// read looked, and withheld and recorded where it did not, since CREATE
// RESOURCE POOL has no guard; a dev realm records both kinds whole, because
// its pools are the database's.
//
// One only the database holds is never a removal. A pool and a classifier
// belong to the whole database rather than to a directory, so the
// applications a database serves may each declare their own, and a plan made
// from one declaration would otherwise drop the others'. This is the rule
// [Roles] keeps for the same reason; a pool a declaration stops naming is
// dropped by hand, or by a rollback of the plan that created it.
func ResourcePools(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	heldPools := make(map[string]catalog.ResourcePool, len(database.ResourcePools))
	for _, pool := range database.ResourcePools {
		heldPools[pool.Name] = pool
	}
	var addedPools difftypes.ResourcePoolChanges
	for _, pool := range desired.ResourcePools {
		current, exists := heldPools[pool.Name]
		switch {
		case !exists:
			addedPools = append(addedPools, pool)
		case !ydbpool.PoolsEqual(pool.Spec, current.Spec):
			diff.ResourcePoolsModified = append(diff.ResourcePoolsModified, difftypes.ResourcePoolDiff{
				Name: pool.Name, Desired: pool.Spec.Clone(), Current: current.Spec.Clone(),
			})
		}
	}
	keptPools, withheldPools := keepPlannedAdditions(cov, coverage.ResourcePool, addedPools,
		func(pool schemamodel.ResourcePool) (string, []string) { return globalName(pool.Name) },
		func(pool schemamodel.ResourcePool) string { return pool.Name },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheldPools)
	diff.ResourcePoolsAdded = keptPools

	heldClassifiers := make(map[string]catalog.ResourcePoolClassifier, len(database.ResourcePoolClassifiers))
	for _, classifier := range database.ResourcePoolClassifiers {
		heldClassifiers[classifier.Name] = classifier
	}
	var addedClassifiers difftypes.ResourcePoolClassifierChanges
	for _, classifier := range desired.ResourcePoolClassifiers {
		current, exists := heldClassifiers[classifier.Name]
		switch {
		case !exists:
			addedClassifiers = append(addedClassifiers, classifier)
		case !ydbpool.ClassifiersEqual(classifier.Spec, current.Spec):
			diff.ResourcePoolClassifiersModified = append(diff.ResourcePoolClassifiersModified,
				difftypes.ResourcePoolClassifierDiff{
					Name:        classifier.Name,
					RankChanged: classifier.Spec.Rank != current.Spec.Rank,
					Desired:     classifier.Spec,
					Current:     current.Spec,
				})
		}
	}
	keptClassifiers, withheldClassifiers := keepPlannedAdditions(cov, coverage.ResourcePoolClassifier,
		addedClassifiers,
		func(classifier schemamodel.ResourcePoolClassifier) (string, []string) {
			return globalName(classifier.Name)
		},
		func(classifier schemamodel.ResourcePoolClassifier) string { return classifier.Name },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheldClassifiers)
	diff.ResourcePoolClassifiersAdded = keptClassifiers

	sort.Slice(diff.ResourcePoolsAdded, func(i, j int) bool {
		return diff.ResourcePoolsAdded[i].Name < diff.ResourcePoolsAdded[j].Name
	})
	sort.Slice(diff.ResourcePoolsModified, func(i, j int) bool {
		return diff.ResourcePoolsModified[i].Name < diff.ResourcePoolsModified[j].Name
	})
	sort.Slice(diff.ResourcePoolClassifiersAdded, func(i, j int) bool {
		return diff.ResourcePoolClassifiersAdded[i].Name < diff.ResourcePoolClassifiersAdded[j].Name
	})
	sort.Slice(diff.ResourcePoolClassifiersModified, func(i, j int) bool {
		return diff.ResourcePoolClassifiersModified[i].Name < diff.ResourcePoolClassifiersModified[j].Name
	})
}
