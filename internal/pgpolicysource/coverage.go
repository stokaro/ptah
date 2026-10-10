package pgpolicysource

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// RequireRepresentable refuses row-level security coverage a source format
// cannot write back: either model, or one of its subjects, that the source
// could not describe. The switches a source leaves to the owner's default
// beside a table's policies (see [pgpolicy.DefaultedSwitches]) pass, because
// writing the policies records them again.
func RequireRepresentable(coverage schemaext.Coverage, objects schemaext.Objects) error {
	for _, record := range coverage.KindRecords() {
		kind := record.Model.Kind
		if (kind == pgpolicy.PolicyKind || kind == pgpolicy.TableStateKind) && record.Knowledge.State != schemaext.Complete {
			return fmt.Errorf("%w: %s cannot be exported without losing its coverage record: %s %s",
				ptaherr.ErrUnsupportedFeature, kind, describeState(record.Knowledge.State), record.Knowledge.Reason)
		}
	}
	policyTables := make(map[objectidentity.Key]bool)
	for _, ref := range objects.Refs() {
		if ref.Kind == objectidentity.Kind(pgpolicy.PolicyKind) {
			policyTables[pgpolicy.Table(ref).Key()] = true
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind != pgpolicy.PolicyKind && record.Kind != pgpolicy.TableStateKind {
			continue
		}
		state := record.Knowledge.State
		if state == schemaext.Complete || state == schemaext.Absent ||
			(record.Kind == pgpolicy.TableStateKind && state == schemaext.Defaulted && policyTables[record.Subject.Key()]) {
			continue
		}
		return fmt.Errorf("%w: %s cannot be exported without losing its coverage record: %s %s",
			ptaherr.ErrUnsupportedFeature, record.Subject, describeState(state), record.Knowledge.Reason)
	}
	return nil
}

// describeState names a knowledge state in a refusal; the uninspected state
// is the empty string.
func describeState(state schemaext.KnowledgeState) string {
	if state == schemaext.Uninspected {
		return "uninspected"
	}
	return string(state)
}
