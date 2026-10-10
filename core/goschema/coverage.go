package goschema

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/dialect/spanner/spannersource"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/ydbsource"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the YDB objects its directives name and the YDB TTL, CockroachDB
// row-level TTL and the Spanner row deletion policy, which a table declares
// through its platform.ydb, platform.cockroachdb and platform.spanner
// properties, and both TimescaleDB models, which have annotations of their
// own. A table without those properties therefore requests none of the
// policies, and a schema without a hypertable or aggregate annotation
// describes a database without either.
func sourceCoverage(limits ydbsource.Limits) (schemaext.Coverage, error) {
	known, err := ydbsource.Coverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	timescale := func() (schemaext.Coverage, error) { return tsschema.CompleteCoverage(schemaext.Desired) }
	for _, owned := range []func() (schemaext.Coverage, error){crdbsource.Coverage, spannersource.Coverage, timescale} {
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
