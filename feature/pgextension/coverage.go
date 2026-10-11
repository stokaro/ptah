package pgextension

import (
	"ptah.run/core/schemaext"
)

// ownedCoverage builds this owner's codec registry once, on first use.
var ownedCoverage = schemaext.OwnedCoverageSource(Owner, Codecs)

// Coverage enrolls the extension model in a source's claims. knowledge is
// the database-wide claim and subjects override it for individual
// extensions. Enrollment names the model's exact definition, so a source
// captured before a runtime gained another provider never claims it.
//
// Known absence needs complete enumeration: a read that could not list the
// installed extensions claims less than complete knowledge rather than
// reporting each one absent.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(ExtensionKind, representation, knowledge, subjects)
}

// CompleteCoverage enrolls the model with complete knowledge: the source
// describes every extension it holds, so an extension it leaves out is
// absent.
func CompleteCoverage(representation schemaext.Representation) (schemaext.Coverage, error) {
	return Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil)
}
