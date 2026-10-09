package ydbsource

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// ValidateCoordinationExport refuses captured limits a declaration format
// cannot carry. Readers enroll this namespace even when they find no nodes;
// dropping a limit would turn unreadable state into declared absence.
func ValidateCoordinationExport(coverage schemaext.Coverage) error {
	return validateExportCoverage(coverage, ydbcoordination.Kind, "coordination")
}

// ValidateStreamingExport refuses read limits that a Go export cannot preserve
// as a complete streaming-query declaration.
func ValidateStreamingExport(coverage schemaext.Coverage) error {
	return validateExportCoverage(coverage, ydbstreaming.Kind, "streaming query")
}

func validateExportCoverage(coverage schemaext.Coverage, kind schemaext.Kind, label string) error {
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == kind && record.Knowledge.State != schemaext.Complete {
			return fmt.Errorf("%w: %s namespace is not fully described: %s", ptaherr.ErrUnsupportedFeature, label, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == kind && (record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable) {
			return fmt.Errorf("%w: %s object %s cannot be exported without losing its read limit: %s", ptaherr.ErrUnsupportedFeature, label, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}
