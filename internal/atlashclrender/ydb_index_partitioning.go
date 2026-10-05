package atlashclrender

import (
	"strconv"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbpartition"
)

func (r *renderer) renderIndexPartitioning(spec *ast.IndexPartitioningSpec) {
	if spec.IsZero() {
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
