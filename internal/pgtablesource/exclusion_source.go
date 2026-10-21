package pgtablesource

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func exclusionCoverage() (schemaext.Coverage, error) {
	return pgtable.ExclusionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

func exclusionObject(schema, table, name string, value pgtable.DesiredExclusion, targets []string) (schemaext.Object, error) {
	ref := objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres)).ConstraintParts(schema, table, name)
	ref.Kind = objectidentity.Kind(pgtable.ExclusionKind)
	if err := pgtable.ValidateExclusionRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := pgtable.ValidateDesiredExclusion(&value); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: &value, Targets: slices.Clone(targets)}, nil
}

func exclusionValue(object schemaext.Object) (*pgtable.DesiredExclusion, error) {
	if err := pgtable.ValidateExclusionRef(object.Ref); err != nil {
		return nil, err
	}
	value, ok := object.Value.(*pgtable.DesiredExclusion)
	if !ok {
		return nil, fmt.Errorf("%w: EXCLUDE source requires a declaration, got %T", schemaext.ErrInvalidValue, object.Value)
	}
	if err := pgtable.ValidateDesiredExclusion(value); err != nil {
		return nil, err
	}
	if !object.Ref.Database.Empty() || !object.Ref.Namespace.IsZero() || !object.Ref.Catalog.Empty() {
		return nil, fmt.Errorf("%w: EXCLUDE source cannot represent catalog %q", ptaherr.ErrUnsupportedFeature, object.Ref.Catalog.Source)
	}
	return value, nil
}

// Extension names are exact catalog names, including punctuation and case.
// The common extension identity lets sources preserve dependencies without
// importing the extension owner's model.
func exclusionRequirements(names []string) []objectidentity.ID {
	result := make([]objectidentity.ID, 0, len(names))
	for _, name := range names {
		result = append(result, objectidentity.ID{Kind: objectidentity.KindExtension, Name: objectidentity.Part{Source: name, Normalized: name}})
	}
	slices.SortFunc(result, schemaext.CompareRefs)
	return result
}

func exclusionRequirementNames(refs []objectidentity.ID) ([]string, error) {
	var names []string
	for _, ref := range refs {
		expected := objectidentity.ID{Kind: objectidentity.KindExtension, Name: objectidentity.Part{Source: ref.Name.Source, Normalized: ref.Name.Source}}
		if ref != expected {
			return nil, fmt.Errorf("%w: EXCLUDE source cannot represent dependency %s", ptaherr.ErrUnsupportedFeature, ref)
		}
		names = append(names, ref.Name.Source)
	}
	slices.Sort(names)
	return names, nil
}

func exclusionExportCoverage(coverage schemaext.Coverage) error {
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == pgtable.ExclusionKind && record.Knowledge.State != schemaext.Complete {
			return fmt.Errorf("%w: EXCLUDE source cannot preserve namespace coverage: %s", ptaherr.ErrUnsupportedFeature, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == pgtable.ExclusionKind && record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return fmt.Errorf("%w: EXCLUDE source cannot preserve %s coverage: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}
