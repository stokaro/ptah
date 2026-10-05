package atlashclrender

import (
	"fmt"
	"strings"

	"ptah.run/internal/ydbpartition"
)

// reportTablePartitioning names every table whose YDB settings the document
// leaves out -- how it splits into partitions, its read replicas, its key bloom
// filter and the partitions it starts with -- because HCL has no spelling for
// them. A cleanup that deletes the source annotations once the HCL is written
// then refuses rather than loses the settings. Reading the document back does
// not reset them either: the loader records that HCL cannot express them, and
// the comparison keeps the settings the database holds.
func (r *renderer) reportTablePartitioning() {
	for _, table := range r.db.Tables {
		if table.YDBPartitioning.IsZero() {
			continue
		}
		settings := ydbpartition.CreateClause(table.YDBPartitioning)
		switch {
		case table.YDBPartitioning.UniformPartitions != 0:
			settings = append(settings, fmt.Sprintf("UNIFORM_PARTITIONS = %d", table.YDBPartitioning.UniformPartitions))
		case len(table.YDBPartitioning.PartitionAtKeys) != 0:
			settings = append(settings, "PARTITION_AT_KEYS = ("+
				ydbpartition.FormatSplitPoints(table.YDBPartitioning.PartitionAtKeys)+")")
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     fmt.Sprintf("table.%s", table.QualifiedName()),
			Message:  fmt.Sprintf("YDB table settings (%s) are not represented in HCL", strings.Join(settings, ", ")),
		})
	}
}
