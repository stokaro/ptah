package ydbsource

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// ValidateCoordinationExport refuses captured limits a declaration format
// cannot carry. Readers enroll this namespace even when they find no nodes;
// dropping a limit would turn unreadable state into declared absence.
func ValidateCoordinationExport(coverage schemaext.Coverage) error {
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == ydbcoordination.Kind && record.Knowledge.State != schemaext.Complete {
			return fmt.Errorf("%w: coordination namespace is not fully described: %s", ptaherr.ErrUnsupportedFeature, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == ydbcoordination.Kind && (record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable) {
			return fmt.Errorf("%w: coordination node %s cannot be exported without losing its read limit: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}
