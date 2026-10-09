package sqlschema

import (
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chast"
)

// The SQL-source adapter preserves expressions as one value. Splitting a tuple
// or function call on commas would invent indexes on nonexistent columns. The
// type and granularity are the ClickHouse owner's declaration, bound to that
// target, rather than the common index type.
func appendSkippingIndex(database *schemamodel.Database, target alterTarget, index *chast.AddSkippingIndex, sourcePlatform string) error {
	facets, err := index.DeclaredFacets()
	if err != nil {
		return err
	}
	_, tableName := normalizeSQLTableIdentifier(sourcePlatform, target.written)
	database.Indexes = append(database.Indexes, schemamodel.Index{
		Facets: facets, Name: normalizeSQLIdentifier(sourcePlatform, index.Name), StructName: tableName,
		Fields: []string{index.Expression}, TableName: target.qualified,
	})
	return nil
}
