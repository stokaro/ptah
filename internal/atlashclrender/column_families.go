package atlashclrender

import (
	"fmt"
	"strings"

	"ptah.run/internal/ydbfamily"
)

// reportColumnFamilies names every table whose YDB column families the
// document leaves out, because HCL has no block for one. That makes the loss
// [ptah.run/internal/goannotationexport.ErrLossyCleanup] for `ptah schema
// export --cleanup-go-annotations`, which would otherwise delete the only
// place the families are declared. Reading the document back moves no column
// either: the loader records that HCL cannot express a family. A table holding
// only a default family that is [ydbfamily.Plain] loses nothing and is not
// named.
func (r *renderer) reportColumnFamilies() {
	for _, table := range r.db.Tables {
		families := ydbfamily.Stated(table.YDBColumnFamilies)
		if len(families) == 0 {
			continue
		}
		names := make([]string, len(families))
		for i, family := range families {
			names[i] = family.Name
		}
		message := fmt.Sprintf("column families %s are not represented in HCL", strings.Join(names, ", "))
		if len(names) == 1 {
			message = fmt.Sprintf("column family %s is not represented in HCL", names[0])
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     fmt.Sprintf("table.%s", table.QualifiedName()),
			Message:  message,
		})
	}
}
