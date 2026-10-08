package ydbcompare

import (
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

func effectiveCoverage(request schemaext.ObjectComparisonRequest, streams map[objectidentity.Key]stream) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		definition, err := ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = definition.KindRecords()
		kinds[0].Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe changefeeds"}
	}
	records := make(map[objectidentity.Key]schemaext.SubjectCoverage)
	for _, record := range request.Desired.Coverage.SubjectRecords() {
		records[record.Subject.Key()] = record
	}
	for _, parent := range request.Parents {
		if parent.Desired && parent.Current && unknown(request.Desired.Coverage.Lookup(ydbschema.ChangefeedKind, parent.Subject)) {
			records[parent.Subject.Key()] = schemaext.SubjectCoverage{Kind: ydbschema.ChangefeedKind, Subject: parent.Subject,
				Knowledge: request.Current.Coverage.Lookup(ydbschema.ChangefeedKind, parent.Subject)}
		}
	}
	for _, value := range streams {
		if value.desired != nil || !unknown(request.Desired.Coverage.Lookup(ydbschema.ChangefeedKind, value.ref)) {
			continue
		}
		parentPresent := slices.ContainsFunc(request.Parents, func(parent schemaext.ParentState) bool {
			return parent.Desired && parent.Current && parent.Subject.Key() == parentRef(value.ref).Key()
		})
		if !parentPresent {
			continue
		}
		knowledge := request.Current.Coverage.Lookup(ydbschema.ChangefeedKind, value.ref)
		if value.current != nil && !objectLimited(request.Current.Coverage, value.ref) {
			knowledge = schemaext.Knowledge{State: schemaext.Complete}
		}
		records[value.ref.Key()] = schemaext.SubjectCoverage{Kind: ydbschema.ChangefeedKind, Subject: value.ref, Knowledge: knowledge}
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, slices.Collect(maps.Values(records)))
}
