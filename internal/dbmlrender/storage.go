package dbmlrender

import "ptah.run/internal/schemaexportloss"

// omittedStorage reports properties missing from the tables DBML writes. Use
// the renderer's selection and ownership rules so an excluded table adds no
// warning, even when another table has the same column or index name.
func (b *builder) omittedStorage() []string {
	counts := make(map[string]int)
	for _, table := range b.selected() {
		schemaexportloss.CountTableStorage(counts, table)
		for _, field := range b.fieldsOf(table) {
			schemaexportloss.CountColumnIdentity(counts, field)
		}
		for _, index := range b.db.Indexes {
			if index.StructName == table.StructName {
				schemaexportloss.CountIndexStorage(counts, index)
			}
		}
	}
	return schemaexportloss.Descriptions(counts)
}
