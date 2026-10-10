package goschema

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/dialect/spanner/spannersource"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/ydbsource"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the YDB objects its directives name and the YDB TTL, CockroachDB
// row-level TTL and the Spanner row deletion policy, which a table declares
// through its platform.ydb, platform.cockroachdb and platform.spanner
// properties; the models of the owners the caller selected, whose directives
// the source could have written; the ClickHouse refresh schedule a
// materialized view declares with `refresh=`; and both PostgreSQL
// row-security models. A table without those properties therefore requests
// none of the policies, a schema without an owner's directive describes a
// database without what it declares, and a materialized view without
// `refresh=` requests no schedule. An owner the caller did not select is not
// enrolled, so its models stay unknown rather than absent.
func sourceCoverage(limits ydbsource.Limits, annotations annotation.Set) (schemaext.Coverage, error) {
	known, err := ydbsource.Coverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	rowSecurity := func() (schemaext.Coverage, error) { return pgpolicy.CompleteCoverage(schemaext.Desired) }
	owners := []func() (schemaext.Coverage, error){crdbsource.Coverage, spannersource.Coverage, annotations.Coverage, chsource.RefreshCoverage, rowSecurity}
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
