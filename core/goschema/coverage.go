package goschema

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/ydbsource"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the YDB objects its directives name and the YDB TTL a table
// declares through its platform.ydb properties; the claims of the owners the
// caller selected, for their own directives and for the platform properties
// they decode, such as CockroachDB row-level TTL and the Spanner row deletion
// policy; the ClickHouse refresh schedule a materialized view declares with
// `refresh=`; and both PostgreSQL row-security models. A schema that could
// have declared one of them and did not describes a database without it. An
// owner the caller did not select is not enrolled, so its models stay unknown
// rather than absent.
func sourceCoverage(limits ydbsource.Limits, annotations annotation.Set) (schemaext.Coverage, error) {
	known, err := ydbsource.Coverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	rowSecurity := func() (schemaext.Coverage, error) { return pgpolicy.CompleteCoverage(schemaext.Desired) }
	owners := []func() (schemaext.Coverage, error){annotations.Coverage, chsource.RefreshCoverage, rowSecurity}
	for _, owned := range owners {
		coverage, err := owned()
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if known, err = known.Combine(coverage); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return known, nil
}
