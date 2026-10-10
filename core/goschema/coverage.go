package goschema

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/dialect/spanner/spannersource"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/ydbsource"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the YDB objects its directives name and the YDB TTL, CockroachDB
// row-level TTL and the Spanner row deletion policy, which a table declares
// through its platform.ydb, platform.cockroachdb and platform.spanner
// properties, both TimescaleDB models, which have annotations of their own,
// the ClickHouse refresh schedule a materialized view declares with
// `refresh=`, and both PostgreSQL row-security models. A table without those properties therefore requests none of the
// policies, a schema without a hypertable or aggregate annotation describes a
// database without either, and a materialized view without `refresh=`
// requests no schedule.
func sourceCoverage(limits ydbsource.Limits) (schemaext.Coverage, error) {
	known, err := ydbsource.Coverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	timescale := func() (schemaext.Coverage, error) { return tsschema.CompleteCoverage(schemaext.Desired) }
	rowSecurity := func() (schemaext.Coverage, error) { return pgpolicy.CompleteCoverage(schemaext.Desired) }
	owners := []func() (schemaext.Coverage, error){crdbsource.Coverage, spannersource.Coverage, timescale, chsource.RefreshCoverage, rowSecurity}
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
