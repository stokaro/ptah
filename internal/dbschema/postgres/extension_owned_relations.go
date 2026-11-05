package postgres

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
)

// extensionOwnedRelationsQuery lists the tables, views, materialized views and
// sequences of one schema that an extension owns.
const extensionOwnedRelationsQuery = `
		SELECT c.relname
		FROM pg_depend d
		JOIN pg_class c ON c.oid = d.objid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE d.classid = 'pg_class'::regclass
		AND d.deptype = 'e'
		AND n.nspname = $1
		ORDER BY c.relname`

// leaveOutExtensionOwnedRelations removes each relation an extension owns from
// a read, with the indexes, constraints, triggers and policies that hang off it.
// `CREATE EXTENSION` creates such a relation and the server refuses to drop it
// alone, so a comparison that saw one would plan a drop that fails with 2BP01 on
// every run, for a view of pg_stat_statements or for PostGIS's spatial_ref_sys.
//
// One pass over the finished read makes this one decision. A filter on each query
// would leave a policy or an index that names a table the read does not report.
// A grant on such a relation stays, because a source may declare one: the read
// has to report it for the comparison to see the grant is held.
//
// The list reads pg_depend, which is what [capability.CatalogDependencies] names.
// A server without that relation has no extensions either.
func (r *Reader) leaveOutExtensionOwnedRelations(ctx context.Context, schema *catalog.Database) error {
	if !r.caps.Has(capability.CatalogDependencies) {
		return nil
	}
	owned, err := r.readExtensionOwnedRelations(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the relations extensions own: %w", err)
	}
	if len(owned) == 0 {
		return nil
	}
	schema.Tables = slices.DeleteFunc(schema.Tables, func(table catalog.Table) bool { return owned[table.QualifiedName()] })
	schema.Views = slices.DeleteFunc(schema.Views, func(view catalog.View) bool {
		return owned[catalog.QualifyTableName(view.Schema, view.Name)]
	})
	schema.MatViews = slices.DeleteFunc(schema.MatViews, func(view catalog.MaterializedView) bool {
		return owned[catalog.QualifyTableName(view.Schema, view.Name)]
	})
	schema.Sequences = slices.DeleteFunc(schema.Sequences, func(sequence catalog.Sequence) bool {
		return owned[catalog.QualifyTableName(sequence.Schema, sequence.Name)]
	})
	schema.Indexes = slices.DeleteFunc(schema.Indexes, func(index catalog.Index) bool { return owned[index.QualifiedTableName()] })
	schema.Constraints = slices.DeleteFunc(schema.Constraints, func(constraint catalog.Constraint) bool {
		return owned[constraint.QualifiedTableName()]
	})
	schema.Triggers = slices.DeleteFunc(schema.Triggers, func(trigger catalog.Trigger) bool { return owned[trigger.QualifiedTable()] })
	schema.ObjectOwners = slices.DeleteFunc(schema.ObjectOwners, func(owner catalog.ObjectOwner) bool {
		return owned[catalog.QualifyTableName(owner.Schema, owner.Name)] && owner.Kind != "schema"
	})
	schema.FeatureObjects = schema.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Parent.Empty() || !owned[catalog.QualifyTableName(ref.Schema.Authored(), ref.Parent.Source)]
	})
	return nil
}

// readExtensionOwnedRelations answers which relations the schemas of this read
// hold that an extension owns, by the name the read reports each with.
func (r *Reader) readExtensionOwnedRelations(ctx context.Context) (map[string]bool, error) {
	owned := make(map[string]bool)
	for _, schemaName := range r.schemasToRead() {
		if err := r.readExtensionOwnedRelationsOfSchema(ctx, schemaName, owned); err != nil {
			return nil, err
		}
	}
	return owned, nil
}

func (r *Reader) readExtensionOwnedRelationsOfSchema(ctx context.Context, schemaName string, owned map[string]bool) error {
	rows, err := r.db.QueryContext(ctx, extensionOwnedRelationsQuery, schemaName)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		owned[catalog.QualifyTableName(r.outputSchema(schemaName), name)] = true
	}
	return rows.Err()
}
