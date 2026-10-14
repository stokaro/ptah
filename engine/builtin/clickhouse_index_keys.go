package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/chindexkey"
	"ptah.run/internal/chkey"
	"ptah.run/internal/schemaprep"
)

// validateDeclaredSkippingIndexKeys refuses a ClickHouse data-skipping index
// whose key names a column its table does not declare.
//
// ClickHouse refuses such an ADD INDEX, and the comparison compares a skipping
// index's key: a declaration whose key changed is a replacement, DROP INDEX
// and then ADD INDEX, which ClickHouse runs outside a transaction. A misspelled
// column in the new key would drop the index and then fail to add it, leaving
// the table without one. So the declaration is refused here, before a plan or
// a render exists, for an index being created as for one being replaced.
//
// Only a name that can be nothing but a column is checked, the way the
// settings replacement checks it: a bare key part, or a bare argument of a
// call, such as `payload` in `lower(payload)`; see
// [chkey.PlainColumnReferences]. An index on a materialized view, or one
// whose table the schema does not declare, is left to the checks that answer
// for those.
func validateDeclaredSkippingIndexKeys(dialect string, database *schemamodel.Database) error {
	if platform.NormalizeDialect(dialect) != platform.ClickHouse || len(database.Indexes) == 0 {
		return nil
	}
	owners := schemamodel.ResolveIndexTableNames(database.Indexes, database.Tables)
	for position, index := range database.Indexes {
		table, found := tableNamed(database.Tables, owners[position])
		if !found {
			continue
		}
		declared := make(map[string]bool)
		for _, field := range database.Fields {
			if field.StructName == table.StructName {
				declared[field.Name] = true
			}
		}
		expression := chindexkey.Expression(schemaprep.IndexKeyParts(index))
		for _, column := range chkey.PlainColumnReferences(expression) {
			if !declared[column] {
				return &ptaherr.RenderError{
					Dialect: platform.ClickHouse,
					Err:     ptaherr.ErrInvalidSchemaDiff,
					Message: fmt.Sprintf("index %q reads column %q, which table %s does not declare; "+
						"ClickHouse refuses the index, and replacing an index drops it before it adds the new one",
						index.Name, column, table.Name),
				}
			}
		}
	}
	return nil
}

// tableNamed returns the table whose qualified name is name.
func tableNamed(tables []schemamodel.Table, name string) (schemamodel.Table, bool) {
	if name == "" {
		return schemamodel.Table{}, false
	}
	for _, table := range tables {
		if table.QualifiedName() == name {
			return table, true
		}
	}
	return schemamodel.Table{}, false
}
