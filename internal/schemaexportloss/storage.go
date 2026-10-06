package schemaexportloss

import "ptah.run/core/schemamodel"

// CountTableStorage counts table properties absent from DBML and inspection JSON.
func CountTableStorage(counts map[string]int, table schemamodel.Table) {
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

// CountIndexStorage counts index settings absent from DBML and inspection JSON.
func CountIndexStorage(counts map[string]int, index schemamodel.Index) {
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

// CountColumnIdentity counts identity details absent from DBML and inspection JSON.
func CountColumnIdentity(counts map[string]int, field schemamodel.Field) {
	if field.IdentityStart != "" || field.IdentityIncrement != "" || field.IdentityOptions != "" {
		counts["identity sequence settings"]++
	}
	if field.IdentityGeneration != "" {
		counts["identity generation modes"]++
	}
}
