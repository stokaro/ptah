package atlashclrender

import (
	"fmt"
	"slices"

	"ptah.run/dialect/ydb/ydbschema"
)

// reportColumnTables names every table whose YDB column storage the document
// leaves out, because HCL has no block for one: reading the document back
// describes a row table.
func (r *renderer) reportColumnTables() {
	for _, table := range r.db.Tables {
		if !slices.Contains(table.Facets.Kinds(), ydbschema.ColumnStoreKind) {
			continue
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{Severity: SeverityWarning, Path: fmt.Sprintf("table.%s", table.QualifiedName()), Message: "YDB column storage, hash partitioning and tiered TTL are not represented in HCL"})
	}
}
