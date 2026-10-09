package goschema

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/internal/ydbsource"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the YDB objects its directives name, and CockroachDB row-level TTL,
// which a table declares through its platform.cockroachdb properties. A table
// without those properties therefore requests no TTL.
func sourceCoverage(limits ydbsource.Limits) (schemaext.Coverage, error) {
	known, err := ydbsource.Coverage(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	ttl, err := crdbsource.Coverage()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return known.Combine(ttl)
}
