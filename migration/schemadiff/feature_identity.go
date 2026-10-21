package schemadiff

import (
	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableidentity"
)

type comparisonIdentity struct {
	desired   *schemamodel.Database
	current   *catalog.Database
	semantics identifier.Semantics
}

// Readers and source codecs can capture facet claims and table-owned feature
// objects before a connection supplies its default database. Bind both with the
// same semantics as the common owners before ForParent selects them. No new
// knowledge is inferred here.
func comparisonTableIdentities(desired *schemamodel.Database, current *catalog.Database, opts *config.CompareOptions, selected schemaext.TargetSelection) (comparisonIdentity, error) {
	semantics, err := comparisonIdentifiers(desired, current, opts)
	if err != nil {
		return comparisonIdentity{}, err
	}
	declared, observed := *desired, *current
	declared.FeatureCoverage, err = tableidentity.BindComparisonCoverage(desired.FeatureCoverage, selected, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	observed.FeatureCoverage, err = tableidentity.BindComparisonCoverage(current.FeatureCoverage, selected, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	declared.FeatureObjects, err = tableidentity.BindObjects(desired.FeatureObjects, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	observed.FeatureObjects, err = tableidentity.BindObjects(current.FeatureObjects, opts.Dialect, semantics)
	if err != nil {
		return comparisonIdentity{}, err
	}
	if err := refuseReservedNames(&declared, &observed, opts.Dialect, semantics); err != nil {
		return comparisonIdentity{}, err
	}
	return comparisonIdentity{desired: &declared, current: &observed, semantics: semantics}, nil
}
