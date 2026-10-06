package dbmlrender

import (
	"fmt"

	"ptah.run/core/schemamodel"
)

// omittedStorage reports properties missing from the tables DBML writes. Use
// the renderer's selection and ownership rules so an excluded table adds no
// warning, even when another table has the same column or index name.
func (b *builder) omittedStorage() []string {
	counts := make(map[string]int)
	for _, table := range b.selected() {
		countTableStorage(counts, table)
		for _, field := range b.fieldsOf(table) {
			if field.IdentityStart != "" || field.IdentityIncrement != "" || field.IdentityOptions != "" {
				counts["identity sequence settings"]++
			}
			if field.IdentityGeneration != "" {
				counts["identity generation modes"]++
			}
		}
		for _, index := range b.db.Indexes {
			if index.StructName == table.StructName {
				countIndexStorage(counts, index)
			}
		}
	}
	omitted := make([]string, 0, len(counts))
	for name, count := range counts {
		if count > 0 {
			omitted = append(omitted, fmt.Sprintf("%s (%d)", name, count))
		}
	}
	return omitted
}

func countTableStorage(counts map[string]int, table schemamodel.Table) {
	counts["changefeeds"] += len(table.Changefeeds)
	counts["column families"] += len(table.YDBColumnFamilies)
	if !table.RowDeletionPolicy.IsZero() {
		counts["row deletion policies"]++
	}
	if table.YDBColumnTable != nil {
		counts["column-oriented storage and settings"]++
	}
	if !table.YDBPartitioning.IsZero() {
		counts["table partitioning, read replicas and key bloom filters"]++
	}
}

func countIndexStorage(counts map[string]int, index schemamodel.Index) {
	if len(index.IncludeColumns) > 0 {
		counts["covering columns on indexes"]++
	}
	if len(index.StorageParams) > 0 {
		counts["index storage and analyzer settings"]++
	}
	if !index.Partitioning.IsZero() {
		counts["index partitioning and read replicas"]++
	}
	if index.Vector != nil {
		counts["vector index settings"]++
	}
}
