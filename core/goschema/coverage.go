package goschema

import (
	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/feature/pgpolicy"
)

// sourceCoverage enrolls the feature namespaces a Go annotation source can
// declare: the claims of the owners the caller selected, for their own
// directives, given the not-described declarations of theirs the source
// wrote in limits, for the attributes they add to the frontend's, such as the
// ClickHouse refresh schedule, and for the platform properties they decode,
// such as CockroachDB row-level TTL and the Spanner row deletion policy; and
// both PostgreSQL row-security models. A schema that could have declared
// one of them and did not describes a database without it. An owner the
// caller did not select is not enrolled, so its models stay unknown rather
// than absent.
func sourceCoverage(annotations annotation.Set, limits ...coverage.Object) (schemaext.Coverage, error) {
	known, err := annotations.Coverage(limits...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	rowSecurity := func() (schemaext.Coverage, error) { return pgpolicy.CompleteCoverage(schemaext.Desired) }
	securityPolicies := func() (schemaext.Coverage, error) {
		return mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	}
	for _, claim := range []func() (schemaext.Coverage, error){rowSecurity, securityPolicies} {
		claimed, err := claim()
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if known, err = known.Combine(claimed); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return known, nil
}
