package ydbcompare

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

func coordinationCoverage(request schemaext.ObjectComparisonRequest, nodes map[objectidentity.Key]coordinationNode) (schemaext.Coverage, error) {
	values := make([]standalonePresence, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, standalonePresence{ref: node.ref, desired: node.desired != nil, current: node.current != nil})
	}
	return standaloneCoverage(request, values, ydbcoordination.Kind, "coordination nodes", ydbcoordination.Coverage)
}
