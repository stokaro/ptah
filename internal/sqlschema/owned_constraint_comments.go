package sqlschema

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
)

func (d *Document) ownedConstraintComment(databases []*schemamodel.Database, statement commentStatement, dialect string) bool {
	if statement.kind != "CONSTRAINT" || statement.onDomain {
		return false
	}
	table := resolveTable(databases, normalizeSQLTableReference(dialect, statement.table), dialect)
	if table == nil {
		return false
	}
	target := tableTarget(table, databases, d, dialect)
	_, ref, found := target.ownedConstraint(normalizeSQLIdentifier(dialect, statement.name))
	if !found {
		return false
	}
	d.comments[ref.Key()] = statement.comment
	return true
}

func (d *Document) applyOwnedComments(ctx context.Context, database *schemamodel.Database, dialect string, owners sqlext.Set) error {
	var edits []sqlext.CommentEdit
	locations := make(map[objectidentity.Key]*schemamodel.Database)
	for _, holder := range declaringDatabases(database, d.base) {
		for _, ref := range holder.FeatureObjects.Refs() {
			comment, found := d.comments[ref.Key()]
			if !found {
				continue
			}
			object, _, err := holder.FeatureObjects.Get(ref)
			if err != nil {
				return err
			}
			locations[ref.Key()] = holder
			edits = append(edits, sqlext.CommentEdit{Object: object, Text: comment})
		}
	}
	if len(edits) == 0 {
		return ctx.Err()
	}
	objects, err := owners.ApplyComments(ctx, dialect, edits)
	if err != nil {
		return err
	}
	replacements := make(map[*schemamodel.Database]schemaext.Objects)
	for _, object := range objects {
		holder := locations[object.Ref.Key()]
		captured, found := replacements[holder]
		if !found {
			captured = holder.FeatureObjects
		}
		captured, err = captured.Replace(object)
		if err != nil {
			return err
		}
		replacements[holder] = captured
	}
	for holder, objects := range replacements {
		holder.FeatureObjects = objects
	}
	return nil
}
