package atlasfilter

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// Workload objects have database-wide names. A dot is a literal character,
// never a directory separator. Include selectors retain captured destination
// pools of kept classifiers and streaming queries, without inventing a pool
// whose definition was not captured. Explicit exclusions remain authoritative.
func (s *scopeSelection) selectWorkloadFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage, error) {
	objects, coverage = selectNamedFeatures(objects, coverage, ydbworkload.ClassifierKind, func(_, name string) bool {
		return s.selectedNames(typeList("resource_pool_classifier"), name)
	})
	pools, err := streamingPools(objects)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	classifiers, err := objects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(ydbworkload.ClassifierKind)
	}).All()
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	for _, object := range classifiers {
		switch value := object.Value.(type) {
		case *ydbworkload.DesiredClassifier:
			pools[value.Spec.ResourcePool] = true
		case *ydbworkload.ObservedClassifier:
			pools[value.Spec.ResourcePool] = true
		default:
			return schemaext.Objects{}, schemaext.Coverage{}, fmt.Errorf("%w: unexpected resource pool classifier %T", schemaext.ErrInvalidValue, object.Value)
		}
	}
	objects, coverage = selectNamedFeatures(objects, coverage, ydbworkload.PoolKind, func(_, name string) bool {
		return s.selectedNames(typeList("resource_pool"), name) || pools[name]
	})
	return objects, coverage, nil
}

func (s *exclusionState) filterWorkloadFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	objects, coverage = selectNamedFeatures(objects, coverage, ydbworkload.PoolKind, func(_, name string) bool {
		return !s.matches("resource_pool", name)
	})
	return selectNamedFeatures(objects, coverage, ydbworkload.ClassifierKind, func(_, name string) bool {
		return !s.matches("resource_pool_classifier", name)
	})
}
