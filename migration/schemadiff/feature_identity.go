package schemadiff

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/internal/tableidentity"
)

type comparisonIdentity struct {
	desired   *schemamodel.Database
	current   *catalog.Database
	semantics identifier.Semantics
}

// Readers and source codecs can capture facet claims before a connection supplies
// its default database. Bind those claims with the same semantics as the common
// owners before ForParent selects them. No new knowledge is inferred here.
func comparisonTableIdentities(desired *schemamodel.Database, current *catalog.Database, opts *config.CompareOptions) (comparisonIdentity, error) {
	semantics, err := comparisonIdentifiers(desired, current, opts)
	if err != nil {
		return comparisonIdentity{}, err
	}
	declared, observed := *desired, *current
	declared.FeatureCoverage, err = bindFacetCoverage(desired.FeatureCoverage, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	observed.FeatureCoverage, err = bindFacetCoverage(current.FeatureCoverage, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	return comparisonIdentity{desired: &declared, current: &observed, semantics: semantics}, nil
}

func bindFacetCoverage(coverage schemaext.Coverage, target string, semantics identifier.Semantics) (schemaext.Coverage, error) {
	coverage, err := tableidentity.BindCoverage(coverage, target, semantics)
	if err != nil || coverage.IsZero() {
		return coverage, err
	}
	subjects := coverage.SubjectRecords()
	for i, record := range subjects {
		ref := record.Subject
		if ref.Kind != objectidentity.KindIndex {
			continue
		}
		tableScoped := semantics.IndexNamespace != identifier.IndexNamespaceSchema
		hasParent := !ref.Parent.Empty()
		if !ref.Catalog.Empty() || ref.Signature != "" || tableScoped != hasParent {
			return schemaext.Coverage{}, fmt.Errorf("%w: invalid index coverage identity %s", schemaext.ErrInvalidValue, ref)
		}
		schema := ref.Schema.Source
		if ref.Schema.Defaulted {
			schema = ""
		}
		subjects[i].Subject = objectidentity.NewBuilder(semantics).IndexParts(schema, ref.Parent.Source, ref.Name.Source)
	}
	return schemaext.NewCoverage(coverage.Representation(), coverage.KindRecords(), subjects)
}
