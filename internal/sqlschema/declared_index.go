package sqlschema

import (
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
)

// appendDeclaredIndex adds the index an owner's ALTER TABLE operation
// declares, such as a ClickHouse data-skipping index. The expression stays one
// value: splitting a tuple or a call on its commas would invent indexes on
// columns that do not exist. The owner's settings of the index travel as its
// facets, bound to the owner's target, rather than as the common index type.
func appendDeclaredIndex(database *schemamodel.Database, target alterTarget, index sqlext.Index, sourcePlatform string) error {
	_, tableName := normalizeSQLTableIdentifier(sourcePlatform, target.written)
	database.Indexes = append(database.Indexes, schemamodel.Index{
		Facets: index.Facets, Name: normalizeSQLIdentifier(sourcePlatform, index.Name), StructName: tableName,
		Fields: []string{index.Expression}, TableName: target.qualified,
	})
	return nil
}
