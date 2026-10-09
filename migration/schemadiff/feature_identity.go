package schemadiff

import (
	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
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
	declared.FeatureCoverage, err = tableidentity.BindCoverage(desired.FeatureCoverage, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	observed.FeatureCoverage, err = tableidentity.BindCoverage(current.FeatureCoverage, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	return comparisonIdentity{desired: &declared, current: &observed, semantics: semantics}, nil
}
