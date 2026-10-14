package pgdefaultprivilege

import (
	"fmt"

	"ptah.run/core/schemaext"
)

var ownedCoverage = schemaext.OwnedCoverageSource(Owner, Codecs)

// Coverage enrolls this model with knowledge of its namespace and any subject
// overrides. Unknown ACL contents are represented by coverage, never fabricated
// grants. Complete global knowledge establishes the built-in default when a
// delta is absent; it does not establish the absence of all privileges.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	for _, subject := range subjects {
		if err := ValidateCoverageSubject(subject); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return ownedCoverage(DefaultPrivilegeKind, representation, knowledge, subjects)
}

// CompleteCoverage enrolls a source that describes every default-privilege delta
// it holds. Missing scoped entries have no additional grants; missing global
// entries retain the built-in default.
func CompleteCoverage(representation schemaext.Representation) (schemaext.Coverage, error) {
	return Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// ValidateCoverageSubject checks the owner-defined identity and the meaning of
// one claim. Class scopes carry only unknown state: an unsupported catalog ACL
// never grants authority to remove a named role's defaults.
func ValidateCoverageSubject(subject schemaext.SubjectCoverage) error {
	if subject.Kind != DefaultPrivilegeKind {
		return fmt.Errorf("%w: unrelated default privilege coverage kind", schemaext.ErrInvalidValue)
	}
	if subject.Subject.Kind == ClassSubjectKind {
		if subject.Knowledge.State != schemaext.Uninspected && subject.Knowledge.State != schemaext.Unrepresentable {
			return fmt.Errorf("%w: a default privilege class can only record unknown coverage", schemaext.ErrInvalidValue)
		}
		_, err := ClassOf(subject.Subject)
		return err
	}
	_, err := IdentityOf(subject.Subject)
	return err
}
