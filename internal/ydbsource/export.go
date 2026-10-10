package ydbsource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ExportLimits encodes source-authored unmanaged scopes and missing namespaces.
// Captured models and read limits that the source cannot reproduce are refused.
// With no kinds specified, it checks every standalone family the source owns.
func ExportLimits(known schemaext.Coverage, kinds ...schemaext.Kind) ([]coverage.Object, error) {
	// This supplies the source's exact model definitions for comparison only.
	// It never adds enrollment to the captured input.
	declared, err := Coverage(Limits{})
	if err != nil {
		return nil, err
	}
	models := make(map[schemaext.Kind]schemaext.CodecIdentity)
	for _, record := range declared.KindRecords() {
		models[record.Model.Kind] = record.Model
	}
	var limits []coverage.Object
	for _, family := range sourceKinds {
		if len(kinds) != 0 && !slices.Contains(kinds, family.kind) {
			continue
		}
		encoded, err := exportFamilyLimits(known, family, models[family.kind])
		if err != nil {
			return nil, err
		}
		limits = append(limits, encoded...)
	}
	return limits, nil
}

// HCLCoordinationDirectives preserves exact model enrollment and captured
// namespace and subject knowledge. An unenrolled namespace emits nothing:
// HCL does not infer namespace authority from omitted declarations.
func HCLCoordinationDirectives(known schemaext.Coverage) ([]string, error) {
	selected := known.SelectKinds([]schemaext.Kind{ydbcoordination.Kind})
	for _, record := range selected.SubjectRecords() {
		if err := ydbcoordination.ValidateIdentity(record.Subject); err != nil {
			return nil, err
		}
	}
	registry, err := hclCoverageRegistry()
	if err != nil {
		return nil, err
	}
	directive, err := registry.EncodeCoverageHeader(context.Background(), schemaext.Desired, selected)
	if err != nil {
		return nil, fmt.Errorf("%w: HCL coordination coverage: %w", ptaherr.ErrUnsupportedFeature, err)
	}
	if directive == "" {
		return nil, nil
	}
	return []string{directive}, nil
}

func exportFamilyLimits(known schemaext.Coverage, family sourceFamily, model schemaext.CodecIdentity) ([]coverage.Object, error) {
	namespace := schemaext.Uninspected
	for _, record := range known.KindRecords() {
		if record.Model.Kind != family.kind {
			continue
		}
		if record.Model != model {
			return nil, fmt.Errorf("%w: %s coverage uses a model the source cannot reproduce", ptaherr.ErrUnsupportedFeature, family.label)
		}
		if record.Knowledge.State != schemaext.Complete && !sourceLimitRepresentable(record.Knowledge, unmanagedNamespaceReason(family.label)) {
			return nil, fmt.Errorf("%w: %s namespace is not fully described: %s", ptaherr.ErrUnsupportedFeature, family.label, record.Knowledge.Reason)
		}
		namespace = record.Knowledge.State
	}
	var limits []coverage.Object
	if namespace == schemaext.Uninspected {
		limits = append(limits, coverage.Object{Kind: coverage.Kind(family.token)})
	}
	for _, record := range known.SubjectRecords() {
		if record.Kind != family.kind {
			continue
		}
		name, err := limitSubjectName(family.kind, record.Subject)
		if err != nil {
			return nil, err
		}
		if record.Knowledge.State == schemaext.Complete && namespace == schemaext.Complete {
			continue
		}
		if !sourceLimitRepresentable(record.Knowledge, unmanagedObjectReason) &&
			!slices.ContainsFunc(family.unmanaged, func(reason string) bool { return sourceLimitRepresentable(record.Knowledge, reason) }) {
			return nil, fmt.Errorf("%w: %s object %s cannot be exported without losing its coverage record: %s %s",
				ptaherr.ErrUnsupportedFeature, family.label, record.Subject, record.Knowledge.State, record.Knowledge.Reason)
		}
		limits = append(limits, coverage.Object{Kind: coverage.Kind(family.token), Name: name})
	}
	return limits, nil
}

// A source directive has a fixed decoded meaning. Equality with that meaning
// proves it can round-trip; arbitrary read errors cannot use this spelling.
// A family's own unmanaged reason is the exception: the read left the object
// unmanaged on purpose, which is what the directive says.
func sourceLimitRepresentable(knowledge schemaext.Knowledge, reason string) bool {
	return knowledge.State == schemaext.Uninspected && knowledge.Reason == reason
}

func limitSubjectName(kind schemaext.Kind, ref objectidentity.ID) (string, error) {
	switch kind {
	case ydbworkload.PoolKind, ydbworkload.ClassifierKind:
		return ref.Name.Source, ydbworkload.ValidateIdentity(ref, kind)
	case ydbcoordination.Kind:
		if err := ydbcoordination.ValidateIdentity(ref); err != nil {
			return "", err
		}
	case ydbstreaming.Kind:
		if err := ydbstreaming.ValidateIdentity(ref); err != nil {
			return "", err
		}
	case ydbsecret.Kind:
		// A secret limit is read as a path, so its directory and name
		// need no leading slash to keep a dot literal.
		return ydbsecret.Display(ref.Schema.Source, ref.Name.Source), ydbsecret.ValidateIdentity(ref)
	case ydbtopic.Kind:
		// A topic limit is read as a path, as a secret limit is.
		return ydbtopic.Display(ref.Schema.Source, ref.Name.Source), ydbtopic.ValidateIdentity(ref)
	case ydbexternal.SourceKind, ydbexternal.TableKind:
		// An external object limit is read as a path too.
		return ydbexternal.Display(ref.Schema.Source, ref.Name.Source), ydbexternal.ValidateIdentity(ref)
	default:
		return "", fmt.Errorf("%w: no source limit spelling for %s", schemaext.ErrInvalidValue, kind)
	}
	// Keep the slash for a root name too: without it a literal dot could be
	// decoded as a directory separator by source qualification rules.
	return ref.Schema.Source + "/" + ref.Name.Source, nil
}
