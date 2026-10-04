package atlashclrender

import (
	"fmt"
)

// reportRowDeletionPolicies names every table whose row deletion policy the
// document leaves out, because HCL has no spelling for one. A cleanup that
// deletes the source annotations once the HCL is written then refuses rather
// than loses the policy. Reading the document back does not remove a policy
// either: the loader records that HCL cannot express one.
func (r *renderer) reportRowDeletionPolicies() {
	for _, table := range r.db.Tables {
		if table.RowDeletionPolicy.IsZero() {
			continue
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     fmt.Sprintf("table.%s", table.QualifiedName()),
			Message: fmt.Sprintf("row deletion policy (TTL %s on %s) is not represented in HCL",
				table.RowDeletionPolicy.Interval, table.RowDeletionPolicy.Column),
		})
	}
}
