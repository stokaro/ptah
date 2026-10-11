package schemacapture

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// ConstraintsFor returns independent constraints owned by a table, preserving
// input order. Struct, bare table, and qualified table references are accepted.
// An explicit table reference takes precedence over a struct name. Empty input
// returns nil. No identity normalization or target policy is applied.
func ConstraintsFor(constraints []schemamodel.Constraint, table schemamodel.Table) []schemamodel.Constraint {
	owned := make([]schemamodel.Constraint, 0, len(constraints))
	for _, constraint := range constraints {
		if Owns(table, constraint) {
			owned = append(owned, constraint.Clone())
		}
	}
	return nilWhenEmpty(owned)
}

// Owns reports whether table owns constraint, by the rule [ConstraintsFor]
// captures a table's constraints with: an explicit table reference, bare or
// qualified, or else the owning struct.
func Owns(table schemamodel.Table, constraint schemamodel.Constraint) bool {
	named := strings.TrimSpace(constraint.Table)
	return named == table.QualifiedName() || named == table.Name ||
		(named == "" && constraint.StructName != "" && constraint.StructName == table.StructName)
}

// DatabasePrimaryKeyFacets is [PrimaryKeyFacets] for a table of db, with the
// constraints [Owns] gives it there. Both pointers address db.
func DatabasePrimaryKeyFacets(db *schemamodel.Database, table *schemamodel.Table) *schemaext.Facets {
	var owned []*schemamodel.Constraint
	for i := range db.Constraints {
		if Owns(*table, db.Constraints[i]) {
			owned = append(owned, &db.Constraints[i])
		}
	}
	return PrimaryKeyFacets(table, owned)
}

// PrimaryKeyFacets addresses the settings of the primary key of table, given
// the constraints it owns: those of the first PRIMARY KEY constraint, where the
// declaration declares its key as one, and [schemamodel.Table.PrimaryKeyFacets]
// otherwise. A key has one place its settings are read from, so the
// comparison and an owner planning the key read the same one.
func PrimaryKeyFacets(table *schemamodel.Table, owned []*schemamodel.Constraint) *schemaext.Facets {
	for _, constraint := range owned {
		if strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") {
			return &constraint.Facets
		}
	}
	return &table.PrimaryKeyFacets
}

// DeclareTable captures one table and its declared children without mutating
// the source. Fields include embedded declarations. Indexes and constraints
// match their explicit table or owning struct; triggers match the table name.
// Feature scopes and knowledge limits are retained. Nil source captures only
// the supplied table; empty child lists are nil.
func DeclareTable(desired *schemamodel.Database, table schemamodel.Table, semantics identifier.Semantics) TableDeclaration {
	declaration := TableDeclaration{Table: table.Clone()}
	if desired == nil {
		return declaration
	}
	parent := objectidentity.NewBuilder(semantics).TableParts(table.Schema, table.Name)
	declaration.OwnedObjects = desired.FeatureObjects.ForParent(parent)
	declaration.FeatureCoverage = desired.FeatureCoverage.ForParent(parent)
	qualified := table.QualifiedName()

	all := schemamodel.ProcessEmbeddedFields(desired.EmbeddedFields, desired.Fields)
	owned := make([]schemamodel.Field, 0, len(all))
	for _, field := range all {
		if field.StructName == table.StructName {
			owned = append(owned, field.Clone())
		}
	}
	declaration.Fields = nilWhenEmpty(owned)
	declaration.Enums = EnumsFor(owned, desired.Enums)

	declaration.Constraints = nilWhenEmpty(ConstraintsFor(desired.Constraints, table))

	indexes := make([]schemamodel.Index, 0, len(desired.Indexes))
	for _, index := range desired.Indexes {
		named := strings.TrimSpace(index.TableName)
		if named == qualified || named == table.Name || (named == "" && index.StructName == table.StructName) {
			indexes = append(indexes, index.Clone())
		}
	}
	declaration.Indexes = nilWhenEmpty(indexes)

	triggers := make([]schemamodel.Trigger, 0, len(desired.Triggers))
	for _, trigger := range desired.Triggers {
		if trigger.Table == qualified || trigger.Table == table.Name {
			triggers = append(triggers, trigger.Clone())
		}
	}
	declaration.Triggers = nilWhenEmpty(triggers)

	return declaration
}

func nilWhenEmpty[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return values
}
