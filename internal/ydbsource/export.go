package ydbsource

import (
	"fmt"

	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ValidateCoordinationExport refuses captured limits a declaration format
// cannot carry. Readers enroll this namespace even when they find no nodes;
// dropping a limit would turn unreadable state into declared absence.
func ValidateCoordinationExport(known schemaext.Coverage) error {
	return validateExportCoverage(known, ydbcoordination.Kind, "coordination", "")
}

// HCLCoordinationDirectives preserves unenrolled namespaces and source-authored
// unmanaged scopes. Captured read limits that the header cannot reproduce stay
// export errors; their details must not be replaced by a coarse source limit.
func HCLCoordinationDirectives(known schemaext.Coverage) ([]string, error) {
	if err := validateExportCoverage(known, ydbcoordination.Kind, "coordination", "coordination nodes"); err != nil {
		return nil, err
	}
	var directives []string
	kind := coverage.Kind(sourceToken(ydbcoordination.Kind))
	for _, token := range UnenrolledNamespaces(known, ydbcoordination.Kind) {
		directives = append(directives, (coverage.Object{Kind: coverage.Kind(token)}).Directive())
	}
	for _, record := range known.KindRecords() {
		if record.Model.Kind == ydbcoordination.Kind && record.Knowledge.State == schemaext.Uninspected {
			directives = append(directives, (coverage.Object{Kind: kind}).Directive())
		}
	}
	for _, record := range known.SubjectRecords() {
		if record.Kind != ydbcoordination.Kind || record.Knowledge.State != schemaext.Uninspected {
			continue
		}
		if err := ydbcoordination.ValidateIdentity(record.Subject); err != nil {
			return nil, err
		}
		// Keep the slash for a root name too: without it a literal dot could
		// be decoded as a directory separator by source qualification rules.
		name := record.Subject.Schema.Source + "/" + record.Subject.Name.Source
		directives = append(directives, (coverage.Object{Kind: kind, Name: name}).Directive())
	}
	return directives, nil
}

// ValidateStreamingExport refuses read limits that a Go export cannot preserve
// as a complete streaming-query declaration.
func ValidateStreamingExport(known schemaext.Coverage) error {
	return validateExportCoverage(known, ydbstreaming.Kind, "streaming query", "")
}

// ValidateWorkloadExport refuses incomplete pool or classifier observations
// before a Go export could turn their missing declarations into known absence.
func ValidateWorkloadExport(known schemaext.Coverage) error {
	if err := validateExportCoverage(known, ydbworkload.PoolKind, "resource pool", ""); err != nil {
		return err
	}
	return validateExportCoverage(known, ydbworkload.ClassifierKind, "resource pool classifier", "")
}

func validateExportCoverage(known schemaext.Coverage, kind schemaext.Kind, label, sourceLabel string) error {
	namespaceReason, subjectReason := "", ""
	if sourceLabel != "" {
		namespaceReason, subjectReason = unmanagedNamespaceReason(sourceLabel), unmanagedObjectReason
	}
	for _, record := range known.KindRecords() {
		if record.Model.Kind == kind && record.Knowledge.State != schemaext.Complete && !sourceLimitRepresentable(known, record.Knowledge, namespaceReason) {
			return fmt.Errorf("%w: %s namespace is not fully described: %s", ptaherr.ErrUnsupportedFeature, label, record.Knowledge.Reason)
		}
	}
	for _, record := range known.SubjectRecords() {
		if record.Kind == kind && record.Knowledge.State != schemaext.Complete && !sourceLimitRepresentable(known, record.Knowledge, subjectReason) {
			detail := record.Knowledge.Reason
			if detail == "" {
				detail = string(record.Knowledge.State)
			}
			return fmt.Errorf("%w: %s object %s cannot be exported without losing its coverage record: %s", ptaherr.ErrUnsupportedFeature, label, record.Subject, detail)
		}
	}
	return nil
}

// A source directive has a fixed decoded meaning. Equality with that meaning
// proves it can round-trip; arbitrary read errors cannot use this spelling.
func sourceLimitRepresentable(known schemaext.Coverage, knowledge schemaext.Knowledge, reason string) bool {
	return reason != "" && known.Representation() == schemaext.Desired && knowledge.State == schemaext.Uninspected && knowledge.Reason == reason
}
