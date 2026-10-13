package sqlschema

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

func (t alterTarget) ownsConstraint(ref objectidentity.ID) bool {
	return normalizeSQLIdentifier("", ref.Parent.Source) == t.table.Name && sameSchema(t.sourcePlatform, ref.Schema.Authored(), t.table.Schema)
}

func (t alterTarget) ownedConstraint(name string) (*schemamodel.Database, objectidentity.ID, bool) {
	for _, holder := range t.databases {
		for _, definition := range schemaext.DefinedConstraints(holder.FeatureObjects) {
			if t.ownsConstraint(definition.Ref) && normalizeSQLIdentifier("", definition.Ref.Name.Source) == name {
				return holder, definition.Ref, true
			}
		}
	}
	return nil, objectidentity.ID{}, false
}

func (t alterTarget) renameOwnedConstraint(holder *schemamodel.Database, ref objectidentity.ID, to string) error {
	if t.holdsConstraint(to) || relationHeld(t.databases, t.document, t.table.Schema, to, t.sourcePlatform) {
		return fmt.Errorf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s: the name is already held by a constraint or relation", t.written, ref.Name.Source, to)
	}
	object, _, err := holder.FeatureObjects.Get(ref)
	if err != nil {
		return err
	}
	object.Ref.Name = objectidentity.Part{Source: to, Normalized: to}
	objects, err := holder.FeatureObjects.Without(ref).With(object)
	if err != nil {
		return err
	}
	holder.FeatureObjects = objects
	if comment, found := t.document.comments[ref.Key()]; found {
		delete(t.document.comments, ref.Key())
		t.document.comments[object.Ref.Key()] = comment
	}
	return nil
}

func ownedConstraintColumnReference(target alterTarget, database *schemamodel.Database, column string) string {
	for _, definition := range schemaext.DefinedConstraints(database.FeatureObjects) {
		if !target.ownsConstraint(definition.Ref) {
			continue
		}
		for _, expression := range definition.Expressions {
			if expressionMentions(target, expression, column) {
				return fmt.Sprintf("constraint %s", definition.Ref.Name.Source)
			}
		}
	}
	return ""
}
