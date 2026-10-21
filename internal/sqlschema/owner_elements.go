package sqlschema

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// appendTableObjects accepts only children of the declaration's actual table.
// A schema-scoped object or a child of another table cannot escape through an
// inline element. Capture the whole batch before changing the destination.
func appendTableObjects(database *schemamodel.Database, databases []*schemamodel.Database, table schemamodel.Table, objects schemaext.Objects, sourcePlatform string) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	result := database.FeatureObjects
	for _, object := range all {
		parent := childTable(databases, object.Ref, sourcePlatform)
		if !object.Ref.Database.Empty() || !object.Ref.Namespace.IsZero() || !object.Ref.Catalog.Empty() || parent == nil || parent.Schema != table.Schema || parent.Name != table.Name {
			return fmt.Errorf("%w: owned element %s is not a child of table %s", schemaext.ErrInvalidValue, object.Ref, table.QualifiedName())
		}
		object.Ref = declaredChild(databases, object.Ref, sourcePlatform)
		if err := refuseEarlierChild(database, databases, object.Ref); err != nil {
			return err
		}

		result, err = result.With(object)
		if err != nil {
			return err
		}
	}
	database.FeatureObjects = result
	return nil
}

func refuseEarlierChild(database *schemamodel.Database, databases []*schemamodel.Database, ref objectidentity.ID) error {
	for _, earlier := range databases {
		if earlier == database {
			continue
		}
		_, found, err := earlier.FeatureObjects.Get(ref)
		if err != nil {
			return err
		}
		if found {
			return fmt.Errorf("%w: object %s", schemaext.ErrDuplicate, ref)
		}
	}
	return nil
}
