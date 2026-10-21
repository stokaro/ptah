package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func validateComparisonExclusions(request *protocol.CompareModelsRequest, result *protocol.ComparedModels) error {
	for _, before := range request.GetDesired().GetRecords() {
		if before.GetValue() != nil {
			continue
		}
		after := findComparisonRecord(result.GetDesired().GetRecords(), before)
		if !proto.Equal(before, after) {
			return fmt.Errorf("%w: comparison changed an excluded facet", ErrProtocol)
		}
		for _, change := range result.GetChanges() {
			if sameComparisonSubject(before, change.GetKind(), change.GetSubject()) {
				return fmt.Errorf("%w: comparison changed an excluded facet", ErrProtocol)
			}
		}
		for _, diagnostic := range result.GetUndecided() {
			if sameComparisonSubject(before, diagnostic.GetKind(), diagnostic.GetSubject()) {
				return fmt.Errorf("%w: comparison reported an excluded facet", ErrProtocol)
			}
		}
	}
	for _, after := range result.GetDesired().GetRecords() {
		if after.GetValue() == nil && !proto.Equal(after, findComparisonRecord(request.GetDesired().GetRecords(), after)) {
			return fmt.Errorf("%w: comparison invented a facet exclusion", ErrProtocol)
		}
	}
	return nil
}

func sameComparisonSubject(record *protocol.ComparisonRecord, kind string, subject *protocol.ComparisonIdentity) bool {
	return record.GetKind() == kind && CompareIdentities(record.GetSubject(), subject) == 0
}

func findComparisonRecord(records []*protocol.ComparisonRecord, subject *protocol.ComparisonRecord) *protocol.ComparisonRecord {
	index := slices.IndexFunc(records, func(record *protocol.ComparisonRecord) bool { return compareComparisonRecords(record, subject) == 0 })
	if index < 0 {
		return nil
	}
	return records[index]
}
