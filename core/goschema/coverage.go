package goschema

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/ydbsource"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the YDB objects its directives name, CockroachDB row-level TTL,
// which a table declares through its platform.cockroachdb properties, and both
// TimescaleDB models, which have annotations of their own. A table without
// those properties therefore requests no TTL, and a schema without a
// hypertable or aggregate annotation describes a database without either.
func sourceCoverage(limits ydbsource.Limits) (schemaext.Coverage, error) {
	known, err := ydbsource.Coverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	ttl, err := crdbsource.Coverage()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	timescale, err := tsschema.CompleteCoverage(schemaext.Desired)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	known, err = known.Combine(ttl)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return known.Combine(timescale)
}
