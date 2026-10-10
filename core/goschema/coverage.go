package goschema

import (
	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare. These are the claims of the owners the caller selected: for their
// own directives, given the not-described declarations of theirs the source
// wrote in limits; for the declarations of the frontend's directives they
// read by target scope, such as PostgreSQL row-level security; for the
// attributes they add to the frontend's directives, such as the ClickHouse
// refresh schedule; and for the platform properties they decode, such as
// CockroachDB row-level TTL. A schema that could have declared one of them
// and did not describes a database without it. An owner the caller did not
// select is not enrolled, so its models stay unknown rather than absent.
func sourceCoverage(annotations annotation.Set, limits ...coverage.Object) (schemaext.Coverage, error) {
	return annotations.Coverage(limits...)
}
