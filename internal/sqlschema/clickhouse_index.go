package sqlschema

import (
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chast"
)

// The SQL-source adapter preserves expressions as one value. Splitting a tuple
// or function call on commas would invent indexes on nonexistent columns.
func appendSkippingIndex(database *schemamodel.Database, target alterTarget, index *chast.AddSkippingIndex, sourcePlatform string) error {
	if err := index.Validate(); err != nil {
		return err
	}
	_, tableName := normalizeSQLTableIdentifier(sourcePlatform, target.written)
	database.Indexes = append(database.Indexes, schemamodel.Index{
		Name: normalizeSQLIdentifier(sourcePlatform, index.Name), StructName: tableName,
		Fields: []string{index.Expression}, Type: index.IndexType,
		Granularity: index.Granularity, TableName: target.qualified,
	})
	return nil
}
