package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// liftInlineReference retains constraint-owned state on a common constraint.
// Field.Foreign has no constraint facet slot. Naming happens first so lifting
// preserves the identity assigned from the document's declaration order.
func liftInlineReference(database *schemamodel.Database, table schemamodel.Table, column *ast.ColumnNode, sourcePlatform string) {
	if column == nil || column.ForeignKey == nil || column.ForeignKey.Facets.Len() == 0 {
		return
	}
	name := normalizeSQLIdentifier(sourcePlatform, column.Name)
	for i := range database.Fields {
		field := &database.Fields[i]
		if field.StructName != table.StructName || field.Name != name {
			continue
		}
		node := &ast.ConstraintNode{Type: ast.ForeignKeyConstraint, Name: field.ForeignKeyName,
			Columns: []string{column.Name}, Reference: column.ForeignKey, Facets: column.ForeignKey.Facets}
		constraint, _ := ToConstraint(node, table.StructName, table.QualifiedName(), sourcePlatform)
		// The field's name is already logical. ToConstraint reads SQL identifiers.
		constraint.Name = field.ForeignKeyName
		database.Constraints = append(database.Constraints, constraint)
		field.Foreign, field.ForeignKeyName, field.OnDelete, field.OnUpdate = "", "", "", ""
		field.Deferrable, field.ForeignKeyNotEnforced = false, false
		field.Initially, field.ForeignKeyMatch = "", ""
		return
	}
}
