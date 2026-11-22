package schemaprep

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// ValidateFeatureParents refuses named children whose table is not declared.
// On a dialect with no default schema of its own, such as ClickHouse, a child
// whose schema the author did not write pairs with a table that names none,
// whatever default the child's identity was built with. On a dialect that has
// one, a defaulted schema is the schema the child is in.
// Rendering validation and AST lowering share this check so neither can accept
// an orphan that the other silently leaves out. A standalone object has no
// parent to validate; its selected owner decides whether it can be lowered.
func ValidateFeatureParents(database *schemamodel.Database, dialect string) error {
	if database == nil {
		return fmt.Errorf("%w: missing desired schema", ptaherr.ErrInvalidSchemaDiff)
	}
	rules := identifier.ForDialect(dialect)
	builder := objectidentity.NewBuilder(rules)
	parents := make(map[objectidentity.Key]bool, len(database.Tables))
	for _, table := range database.Tables {
		parents[builder.TableParts(table.Schema, table.Name).Key()] = true
	}
	for _, ref := range database.FeatureObjects.Refs() {
		if ref.Parent.Empty() {
			continue
		}
		parent := objectidentity.ID{Kind: objectidentity.KindTable, Database: ref.Database, Namespace: ref.Namespace, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
		if ref.Schema.Defaulted && rules.DefaultSchema == "" {
			// The author named no schema, and the reference was built under a
			// connection's default, which these rules do not know: ClickHouse
			// has none of its own, so a policy captured from a read as
			// d1.t.p would miss its table t. The parent is the table named
			// without a schema, as this check keys it. A dialect with a
			// default of its own keys an unqualified table under it, so there
			// the defaulted schema stays: on PostgreSQL a child built under
			// the connection's app pairs with app.t, never with public.t.
			parent.Schema = builder.TableParts(ref.Schema.Authored(), ref.Parent.Source).Schema
		}
		if !parents[parent.Key()] {
			return fmt.Errorf("%w: feature object %s has no declared parent table", ptaherr.ErrInvalidSchemaDiff, ref)
		}
	}
	return nil
}
