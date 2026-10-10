package mssqlschema

import (
	"ptah.run/core/schemaext"
)

// ownedCoverage builds this owner's codec registry once, on first use.
var ownedCoverage = schemaext.OwnedCoverageSource(Owner, Codecs)

// Coverage enrolls the security policy model in a source's claims. knowledge
// is the namespace-wide claim and subjects override it for individual
// policies. Enrollment names this model's exact definition, so a source
// captured before a runtime gained another provider never claims it.
//
// Known absence needs complete enumeration: a source that read only some
// schemas, or could not read a policy's predicates, claims less than complete
// knowledge for what it could not see rather than reporting it absent.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(SecurityPolicyKind, representation, knowledge, subjects)
}
