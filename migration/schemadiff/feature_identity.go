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

// Readers and source codecs can capture table claims before a connection supplies
// its default database. Bind those claims with the same semantics as the common
// tables before ForParent selects them. No new knowledge is inferred here.
func comparisonTableIdentities(desired *schemamodel.Database, current *catalog.Database, opts *config.CompareOptions) (comparisonIdentity, error) {
	semantics, err := comparisonIdentifiers(desired, current, opts)
	if err != nil {
		return comparisonIdentity{}, err
	}
	declared, observed := *desired, *current
	declared.FeatureCoverage, err = bindTableCoverage(desired.FeatureCoverage, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	observed.FeatureCoverage, err = bindTableCoverage(current.FeatureCoverage, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	return comparisonIdentity{desired: &declared, current: &observed, semantics: semantics}, nil
}

func bindTableCoverage(coverage schemaext.Coverage, target string, semantics identifier.Semantics) (schemaext.Coverage, error) {
	if coverage.IsZero() {
		return coverage, nil
	}
	subjects := coverage.SubjectRecords()
	for i, record := range subjects {
		ref := record.Subject
		if ref.Kind != objectidentity.KindTable {
			continue
		}
		if !ref.Catalog.Empty() || !ref.Parent.Empty() || ref.Signature != "" {
			return schemaext.Coverage{}, fmt.Errorf("%w: invalid table coverage identity %s", schemaext.ErrInvalidValue, ref)
		}
		schema := ref.Schema.Source
		if ref.Schema.Defaulted {
			schema = ""
		}
		subjects[i].Subject = tableidentity.Subject(schema, ref.Name.Source, target, semantics)
	}
	// Rebuilding refuses claims that collide under the selected semantics,
	// including differently spelled claims with the same knowledge state.
	return schemaext.NewCoverage(coverage.Representation(), coverage.KindRecords(), subjects)
}
