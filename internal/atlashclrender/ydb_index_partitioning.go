package atlashclrender

import (
	"strconv"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// heldIndexPartitioning is the settings an index's facet states, declared or
// read; an invalid value states none here and is refused where it is used.
func heldIndexPartitioning(facets schemaext.Facets) *ydbschema.IndexPartitioning {
	if declared, found, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](facets, ydbschema.IndexPartitioningKind); err == nil && found {
		return &declared.IndexPartitioning
	}
	if observed, found, err := schemaext.FacetAs[*ydbschema.ObservedIndexPartitioning](facets, ydbschema.IndexPartitioningKind); err == nil && found {
		return &observed.IndexPartitioning
	}
	return nil
}

func (r *renderer) renderIndexPartitioning(facets schemaext.Facets) {
	spec := heldIndexPartitioning(facets)
	if spec == nil || spec.IsZero() {
		return
	}
	for _, setting := range []struct {
		name  string
		value *bool
	}{
		{ydbpartition.AttributeBySize, spec.BySize},
		{ydbpartition.AttributeByLoad, spec.ByLoad},
	} {
		if setting.value != nil {
			value := "DISABLED"
			if *setting.value {
				value = "ENABLED"
			}
			r.stringAttr(2, setting.name, value)
		}
	}
	for _, setting := range []struct {
		name  string
		value uint64
	}{
		{ydbpartition.AttributePartitionSizeMB, spec.PartitionSizeMB},
		{ydbpartition.AttributeMinPartitions, spec.MinPartitions},
		{ydbpartition.AttributeMaxPartitions, spec.MaxPartitions},
	} {
		if setting.value != 0 {
			r.rawAttr(2, setting.name, strconv.FormatUint(setting.value, 10))
		}
	}
	r.stringAttr(2, ydbpartition.AttributeReadReplicas, spec.ReadReplicas)
}
