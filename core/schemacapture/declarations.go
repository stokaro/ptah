package schemacapture

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// ConstraintsFor returns independent constraints owned by a table, preserving
// input order. Struct, bare table, and qualified table references are accepted.
// An explicit table reference takes precedence over a struct name. Empty input
// returns nil. No identity normalization or target policy is applied.
func ConstraintsFor(constraints []schemamodel.Constraint, table schemamodel.Table) []schemamodel.Constraint {
	qualified := table.QualifiedName()
	owned := make([]schemamodel.Constraint, 0, len(constraints))
	for _, constraint := range constraints {
		named := strings.TrimSpace(constraint.Table)
		if named == qualified || named == table.Name ||
			(named == "" && constraint.StructName != "" && constraint.StructName == table.StructName) {
			owned = append(owned, constraint.Clone())
		}
	}
	return nilWhenEmpty(owned)
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
