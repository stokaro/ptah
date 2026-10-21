package tableidentity

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// BindCoverage resolves table and table-owned claims together with their
// namespace limits. Comparison and reverse creation projection share this
// binding. Regions without enough identity structure are refused; unknown
// regions are never dropped. The result is projected onto the selected target.
func BindCoverage(coverage schemaext.Coverage, selected schemaext.TargetSelection, semantics identifier.Semantics) (schemaext.Coverage, error) {
	return (coverageBinder{selected: selected, semantics: semantics}).bind(coverage)
}

// BindComparisonCoverage also resolves index and routine claims. Their explicit
// identities and namespace limits use the same mode selection and builders.
func BindComparisonCoverage(coverage schemaext.Coverage, selected schemaext.TargetSelection, semantics identifier.Semantics) (schemaext.Coverage, error) {
	return (coverageBinder{selected: selected, semantics: semantics, comparison: true}).bind(coverage)
}

type coverageBinder struct {
	selected   schemaext.TargetSelection
	semantics  identifier.Semantics
	comparison bool
}

type coverageBinding uint8

const (
	ownedCoverage coverageBinding = iota
	tableCoverage
	indexCoverage
	routineCoverage
	unchangedCoverage
)

// mode is shared by subject and namespace binding. Separate kind lists could
// rebind a subject while leaving its unknown namespace behind.
func (b coverageBinder) mode(kind objectidentity.Kind) coverageBinding {
	switch kind {
	case objectidentity.KindTable:
		return tableCoverage
	case objectidentity.KindIndex:
		if b.comparison {
			return indexCoverage
		}
		return unchangedCoverage
	case objectidentity.KindFunction, objectidentity.KindProcedure:
		if b.comparison {
			return routineCoverage
		}
		return unchangedCoverage
	}
	return ownedCoverage
}

func (b coverageBinder) bind(coverage schemaext.Coverage) (schemaext.Coverage, error) {
	if coverage.IsZero() {
		return coverage, nil
	}
	var err error
	if b.selected.Name() != "" {
		coverage, err = coverage.ForTarget(b.selected)
		if err != nil {
			return schemaext.Coverage{}, err
		}
	}
	kinds := coverage.KindRecords()
	for i, record := range kinds {
		kinds[i].Knowledge, err = record.Knowledge.MapScopes(b.scopes)
		if err != nil {
			return schemaext.Coverage{}, err
		}
	}
	subjects := coverage.SubjectRecords()
	for i, record := range subjects {
		subjects[i].Subject, err = b.subject(record.Subject)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if b.selected.Name() != "" && len(record.Targets) != 0 {
			subjects[i].Targets = []string{b.selected.Name()}
		}
	}
	return schemaext.NewCoverage(coverage.Representation(), kinds, subjects)
}

func (b coverageBinder) subject(ref objectidentity.ID) (objectidentity.ID, error) {
	mode := b.mode(ref.Kind)
	switch mode {
	case unchangedCoverage:
		return ref, nil
	case ownedCoverage:
		return bindOwned(ref, b.selected.Name(), b.semantics), nil
	}
	if !ref.Database.Empty() || !ref.Namespace.IsZero() || !ref.Catalog.Empty() {
		return objectidentity.ID{}, fmt.Errorf("%w: invalid common coverage identity %s", schemaext.ErrInvalidValue, ref)
	}
	schema := ref.Schema.Authored()
	switch mode {
	case tableCoverage:
		if !ref.Parent.Empty() || !ref.Signature.IsZero() {
			return objectidentity.ID{}, fmt.Errorf("%w: invalid table coverage identity %s", schemaext.ErrInvalidValue, ref)
		}
		return Subject(schema, ref.Name.Source, b.selected.Name(), b.semantics), nil
	case routineCoverage:
		if !ref.Parent.Empty() {
			return objectidentity.ID{}, fmt.Errorf("%w: invalid routine coverage identity %s", schemaext.ErrInvalidValue, ref)
		}
		result := objectidentity.NewBuilder(b.semantics).SchemaScopedParts(ref.Kind, schema, ref.Name.Source)
		result.Signature = ref.Signature
		return result, nil
	case indexCoverage:
		tableScoped := b.semantics.IndexNamespace != identifier.IndexNamespaceSchema
		if !ref.Signature.IsZero() || tableScoped != !ref.Parent.Empty() {
			return objectidentity.ID{}, fmt.Errorf("%w: invalid index coverage identity %s", schemaext.ErrInvalidValue, ref)
		}
		return objectidentity.NewBuilder(b.semantics).IndexParts(schema, ref.Parent.Source, ref.Name.Source), nil
	}
	return objectidentity.ID{}, fmt.Errorf("%w: unknown coverage binding", schemaext.ErrInvalidValue)
}
