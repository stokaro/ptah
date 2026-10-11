package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// applyOwnerStatement applies an owner's statement, which the parse read
// through the owner: it adds the objects the statement declares to database,
// and replaces the ones it amends where they were declared, in database or in
// what the earlier documents declared. handled is false for any other
// statement.
func applyOwnerStatement(database *schemamodel.Database, document *Document, stmt ast.Node) (handled bool, err error) {
	statement, ok := stmt.(*ast.ExtensionStatement)
	if !ok {
		return false, nil
	}
	value, owned := document.ownerStatement(statement)
	if !owned {
		return false, nil
	}
	for _, object := range value.Objects {
		objects, err := database.FeatureObjects.With(object)
		if err != nil {
			return true, err
		}
		database.FeatureObjects = objects
	}
	for _, object := range value.Replaces {
		if err := replaceDeclared(database, document.base, object); err != nil {
			return true, err
		}
	}
	return true, nil
}

// replaceDeclared replaces object where it was declared: in database, the
// document's own declarations, or in base, the earlier documents', which a
// change is made to in place; see [ReadOnto].
func replaceDeclared(database, base *schemamodel.Database, object schemaext.Object) error {
	for _, declared := range []*schemamodel.Database{database, base} {
		if declared == nil {
			continue
		}
		_, found, err := declared.FeatureObjects.Get(object.Ref)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		replaced, err := declared.FeatureObjects.Replace(object)
		if err != nil {
			return err
		}
		declared.FeatureObjects = replaced
		return nil
	}
	return undeclared(fmt.Errorf("%w: an owner's statement changes %s, which this schema does not declare",
		ErrUnmodeledStatement, object.Ref))
}
