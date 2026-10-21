// Package defaultprivnote reports incomplete default privilege observations
// on schema read surfaces. The owner supplies the identities and the reason;
// catalog query shapes do not cross this boundary.
package defaultprivnote

import (
	"fmt"
	"io"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// ReportUndescribed names the ACL scopes the read cannot fully describe. A
// namespace refusal and a partial ACL remain distinct from an inspected empty
// catalog. A nil writer suppresses diagnostics; write errors do not fail a read.
func ReportUndescribed(w io.Writer, database *catalog.Database) {
	if w == nil || database == nil {
		return
	}
	known := database.FeatureCoverage.SelectKinds([]schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind})
	for _, kind := range known.KindRecords() {
		if unknown(kind.Knowledge.Unscoped()) {
			_, _ = fmt.Fprintf(w, "note: default privileges are not described: %s. A comparison leaves this scope unmanaged.\n", kind.Knowledge.Unscoped().Reason)
		}
	}
	for _, claim := range known.SubjectRecords() {
		if !unknown(claim.Knowledge) {
			continue
		}
		subject, err := subjectName(claim)
		if err != nil {
			_, _ = fmt.Fprintf(w, "note: default privilege coverage is invalid: %v.\n", err)
			continue
		}
		_, _ = fmt.Fprintf(w, "note: default privileges for %s are not fully described: %s.\n", subject, claim.Knowledge.Reason)
	}
}

func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func subjectName(claim schemaext.SubjectCoverage) (string, error) {
	if err := pgdefaultprivilege.ValidateCoverageSubject(claim); err != nil {
		return "", err
	}
	if claim.Subject.Kind == pgdefaultprivilege.ClassSubjectKind {
		class, err := pgdefaultprivilege.ClassOf(claim.Subject)
		return class.String(), err
	}
	identity, err := pgdefaultprivilege.IdentityOf(claim.Subject)
	return identity.String(), err
}
