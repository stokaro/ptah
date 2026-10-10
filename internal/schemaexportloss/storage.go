package schemaexportloss

import "ptah.run/core/schemamodel"

// CountTableStorage counts table properties absent from DBML and inspection JSON.
func CountTableStorage(counts map[string]int, table schemamodel.Table) {
}

// CountIndexStorage counts index settings absent from DBML and inspection JSON.
func CountIndexStorage(counts map[string]int, index schemamodel.Index) {
	if len(index.IncludeColumns) > 0 {
		counts["covering columns on indexes"]++
	}
	if len(index.StorageParams) > 0 {
		counts["index storage and analyzer settings"]++
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
