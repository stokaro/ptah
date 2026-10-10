package atlashclrender

import (
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// reportTablePartitioning names every table whose YDB settings the document
// leaves out -- how it splits into partitions, its read replicas, its key bloom
// filter and the partitions it starts with -- because HCL has no spelling for
// them. A cleanup that deletes the source annotations once the HCL is written
// then refuses rather than loses the settings. Reading the document back does
// not reset them either: the document states no setting, and a setting a
// declaration leaves out keeps what the table holds.
func (r *renderer) reportTablePartitioning() {
	for _, table := range r.db.Tables {
		spec := heldPartitioning(table.Facets)
		if spec == nil || spec.IsZero() {
			continue
		}
		settings := ydbpartition.CreateClause(spec)
		switch {
		case spec.UniformPartitions != 0:
			settings = append(settings, fmt.Sprintf("UNIFORM_PARTITIONS = %d", spec.UniformPartitions))
		case len(spec.PartitionAtKeys) != 0:
			settings = append(settings, "PARTITION_AT_KEYS = ("+ydbpartition.FormatSplitPoints(spec.PartitionAtKeys)+")")
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     fmt.Sprintf("table.%s", table.QualifiedName()),
			Message:  fmt.Sprintf("YDB table settings (%s) are not represented in HCL", strings.Join(settings, ", ")),
		})
	}
}

// heldPartitioning is the settings a table's facet states, declared or read;
// an invalid value states none here and is refused where it is used.
func heldPartitioning(facets schemaext.Facets) *ydbschema.TablePartitioning {
	if declared, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](facets, ydbschema.TablePartitioningKind); err == nil && found {
		return &declared.TablePartitioning
	}
	if observed, found, err := schemaext.FacetAs[*ydbschema.ObservedTablePartitioning](facets, ydbschema.TablePartitioningKind); err == nil && found {
		return &observed.TablePartitioning
	}
	return nil
}
