package ydbcompare

import (
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

func coordinationCoverage(request schemaext.ObjectComparisonRequest, nodes map[objectidentity.Key]coordinationNode) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		coverage, err := ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe coordination nodes"}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = coverage.KindRecords()
	}
	records := make(map[objectidentity.Key]schemaext.SubjectCoverage)
	for _, record := range request.Desired.Coverage.SubjectRecords() {
		records[record.Subject.Key()] = record
	}
	for _, node := range nodes {
		if node.desired != nil || !unknown(request.Desired.Coverage.Lookup(ydbcoordination.Kind, node.ref)) {
			continue
		}
		knowledge := request.Current.Coverage.Lookup(ydbcoordination.Kind, node.ref)
		if node.current != nil && !coordinationLimited(request.Current.Coverage, node.ref) {
			knowledge = schemaext.Knowledge{State: schemaext.Complete}
		}
		records[node.ref.Key()] = schemaext.SubjectCoverage{Kind: ydbcoordination.Kind, Subject: node.ref, Knowledge: knowledge}
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, slices.Collect(maps.Values(records)))
}
