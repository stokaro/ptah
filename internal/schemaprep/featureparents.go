package schemaprep

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// ValidateFeatureParents refuses named children whose table is not declared.
// Rendering validation and AST lowering share this check so neither can accept
// an orphan that the other silently leaves out. A standalone object has no
// parent to validate; its selected owner decides whether it can be lowered.
func ValidateFeatureParents(database *schemamodel.Database, dialect string) error {
	if database == nil {
		return fmt.Errorf("%w: missing desired schema", ptaherr.ErrInvalidSchemaDiff)
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect(dialect))
	parents := make(map[objectidentity.Key]bool, len(database.Tables))
	for _, table := range database.Tables {
		parents[builder.TableParts(table.Schema, table.Name).Key()] = true
	}
	for _, ref := range database.FeatureObjects.Refs() {
		if ref.Parent.Empty() {
			continue
		}
		parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
		if !parents[parent.Key()] {
			return fmt.Errorf("%w: feature object %s has no declared parent table", ptaherr.ErrInvalidSchemaDiff, ref)
		}
	}
	return nil
}
