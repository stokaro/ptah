package atlasfilter

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

func (s *scopeSelection) selectStreamingFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbstreaming.Kind, func(schema, name string) bool { return s.selected(typeList("streaming_query"), schema, name) })
}

func (s *exclusionState) filterStreamingFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	return selectNamedFeatures(objects, coverage, ydbstreaming.Kind, func(schema, name string) bool {
		return !s.matches("streaming_query", s.nameCandidates(schema, name)...) && !s.schemaExcluded(schema)
	})
}

// A selected query retains its resource pool even when no pool selector names
// it. Invalid captured values refuse projection instead of losing a dependency.
func streamingPools(objects schemaext.Objects) (map[string]bool, error) {
	selected := objects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbstreaming.Kind) })
	values, err := selected.All()
	if err != nil {
		return nil, err
	}
	pools := make(map[string]bool)
	for _, object := range values {
		switch value := object.Value.(type) {
		case *ydbstreaming.Desired:
			pools[ydbstreaming.Pool(value.Spec)] = true
		case *ydbstreaming.Observed:
			pools[ydbstreaming.Pool(value.Spec)] = true
		default:
			return nil, fmt.Errorf("%w: unexpected streaming query %T", schemaext.ErrInvalidValue, object.Value)
		}
	}
	return pools, nil
}
