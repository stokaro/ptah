package pgpolicysource

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// unmanagedSwitchesReason is why a source leaves a table's switches to the
// database.
const unmanagedSwitchesReason = "the declaration names the table's row-level security policies and not its switches"

// UnmanagedSwitches is the knowledge a source records for the row-level
// security switches of a table that declares policies and no switches
// (stokaro/ptah#2048). The comparison then plans neither ENABLE nor DISABLE on
// a table that exists, and a table the plan creates is enabled because it has
// policies. A declaration that wants row-level security off says so by
// declaring no policies for the table.
func UnmanagedSwitches() schemaext.Knowledge {
	return schemaext.Knowledge{State: schemaext.Uninspected, Reason: unmanagedSwitchesReason}
}

// RequireRepresentable refuses row-level security coverage a source format
// cannot write back: either model, or one of its subjects, that the source
// could not describe. The switches a source leaves unmanaged beside a table's
// policies pass, because writing the policies records them again.
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
			(record.Kind == pgpolicy.TableStateKind && record.Knowledge == UnmanagedSwitches() && policyTables[record.Subject.Key()]) {
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
