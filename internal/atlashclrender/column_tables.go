package atlashclrender

import "fmt"

func (r *renderer) reportColumnTables() {
	for _, table := range r.db.Tables {
		if table.YDBColumnTable == nil {
			continue
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{Severity: SeverityWarning, Path: fmt.Sprintf("table.%s", table.QualifiedName()), Message: "YDB column storage, hash partitioning and tiered TTL are not represented in HCL"})
	}
}
