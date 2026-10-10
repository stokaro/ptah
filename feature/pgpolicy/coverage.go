package pgpolicy

import (
	"ptah.run/core/schemaext"
)

// ownedCoverage builds this owner's codec registry once, on first use.
var ownedCoverage = schemaext.OwnedCoverageSource(Owner, Codecs)

// Coverage enrolls one row-security model in a source's claims. knowledge is
// the namespace-wide claim and subjects override it for individual policies or
// tables. Enrollment names this model's exact definition, so a source captured
// before a runtime gained another provider never claims it.
//
// Known absence needs complete enumeration: a source that read only some
// tables, or could not read a policy's state, claims less than complete
// knowledge for what it could not see rather than reporting it absent.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(kind, representation, knowledge, subjects)
}

// CompleteCoverage enrolls both models with complete knowledge: the source
// describes every policy and every table's row-security switches it holds, so
// an omitted policy is absent and a table without the facet has both switches
// off.
func CompleteCoverage(representation schemaext.Representation) (schemaext.Coverage, error) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	policies, err := Coverage(PolicyKind, representation, complete, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	tables, err := Coverage(TableStateKind, representation, complete, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return policies.Combine(tables)
}
