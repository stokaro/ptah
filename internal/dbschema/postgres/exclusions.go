package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgexclude"
	"ptah.run/internal/revisiontable"
)

// readExclusions observes each exclusion as an independently named object.
// Catalog failures fail the read. Missing catalog support leaves the namespace
// uninspected on a target that holds EXCLUDE constraints, so an unavailable
// read cannot authorize constraint removal. A target the owner creates none on,
// such as Spanner, has none for the read to miss, and its namespace is
// described: recording it as unknown would make every export of that target
// refuse a loss that cannot happen.
func (r *Reader) readExclusions(ctx context.Context, database *catalog.Database) error {
	indexes := make(map[objectidentity.Key]int, len(database.Indexes))
	builder := objectidentity.NewBuilder(identifier.ForDialect(r.dialect))
	for i, index := range database.Indexes {
		indexes[builder.IndexParts(index.Schema, index.TableName, index.Name).Key()] = i
	}
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	if pgtable.SupportsExclusion(r.dialect) {
		knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the server cannot read exclusion constraints from pg_constraint"}
	}
	if r.caps.Has(capability.PostgresCatalogFunctions) {
		for _, schema := range r.schemasToRead() {
			objects, err := r.readExclusionsForSchema(ctx, schema)
			if err != nil {
				return err
			}
			for _, object := range objects {
				if database.FeatureObjects, err = database.FeatureObjects.With(object.object); err != nil {
					return err
				}
				if i, found := indexes[object.index.Key()]; found {
					database.Indexes[i].ManagedBy = object.object.Ref
				}
			}
		}
		knowledge = schemaext.Knowledge{State: schemaext.Complete}
	}
	known, err := pgtable.ExclusionCoverage(schemaext.Observed, knowledge, nil)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}

// catalogExclusion keeps the constraint and its physical index identities
// separate. pg_constraint.conindid establishes ownership, not a name match.
type catalogExclusion struct {
	object schemaext.Object
	index  objectidentity.ID
}

func (r *Reader) readExclusionsForSchema(ctx context.Context, schema string) ([]catalogExclusion, error) {
	query := `
			SELECT
				n.nspname AS schema_name,
				c.conname AS constraint_name,
				cl.relname AS table_name,
				c.contype AS constraint_type,
			pg_get_constraintdef(c.oid) AS constraint_definition,
			-- The extensions the backing index resolves to. An EXCLUDE element
			-- prints its operator, and its operator class only when that class
			-- is not the default, so EXCLUDE USING gist (room WITH =, ...) over
			-- an integer column needs btree_gist and says nothing of it, while
			-- (txt gist_trgm_ops WITH =) needs pg_trgm and does print the
			-- class. See requiredExtensionsProjection.
			` + requiredExtensionsProjection("ix.indclass", "ic.relam") + ` AS required_extensions,
			COALESCE(obj_description(c.oid, 'pg_constraint'), '') AS constraint_comment,
			c.condeferrable,
			c.condeferred,
			ic.relname AS backing_index
		FROM pg_constraint c
		JOIN pg_class cl ON c.conrelid = cl.oid
		JOIN pg_namespace n ON cl.relnamespace = n.oid
		LEFT JOIN pg_index ix ON ix.indexrelid = c.conindid
		LEFT JOIN pg_class ic ON ic.oid = c.conindid
		WHERE c.contype IN ('x')
		AND n.nspname = $1
		AND cl.relname NOT IN (` + revisiontable.NativeSQLNames + `)
		ORDER BY cl.relname, c.conname`
	rows, err := r.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("read EXCLUDE constraints in schema %s: %w", schema, err)
	}
	defer rows.Close()
	var objects []catalogExclusion
	for rows.Next() {
		object, err := r.scanExclusion(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read EXCLUDE constraints in schema %s: %w", schema, err)
	}
	return objects, nil
}

func (r *Reader) scanExclusion(rows *sql.Rows) (catalogExclusion, error) {
	var schema, name, table, constraintType, definition, requiredExtensions, comment, index string
	var deferrable, deferred bool
	if err := rows.Scan(&schema, &name, &table, &constraintType, &definition, &requiredExtensions, &comment, &deferrable, &deferred, &index); err != nil {
		return catalogExclusion{}, fmt.Errorf("scan EXCLUDE constraint: %w", err)
	}
	if constraintType != "x" {
		return catalogExclusion{}, fmt.Errorf("%w: EXCLUDE catalog returns constraint type %q", schemaext.ErrInvalidValue, constraintType)
	}
	parsed, err := pgexclude.Parse(definition)
	if err != nil {
		return catalogExclusion{}, fmt.Errorf("read EXCLUDE constraint %s.%s.%s: %w", schema, table, name, err)
	}
	required, err := decodePostgresNameList(requiredExtensions)
	if err != nil {
		return catalogExclusion{}, fmt.Errorf("read EXCLUDE constraint %s.%s.%s dependencies: %w", schema, table, name, err)
	}
	value := &pgtable.ObservedExclusion{UsingMethod: parsed.UsingMethod, Elements: parsed.Elements, WhereCondition: parsed.WhereCondition,
		Deferrable: deferrable, Comment: comment, Dependencies: extensionRequirements(required)}
	if deferrable {
		value.Initially = deferralTimings[deferred]
	}
	if err := pgtable.ValidateObservedExclusion(value); err != nil {
		return catalogExclusion{}, err
	}
	ref := objectidentity.NewBuilder(identifier.ForDialect(r.dialect)).ConstraintParts(r.outputSchema(schema), table, name)
	ref.Kind = objectidentity.Kind(pgtable.ExclusionKind)
	if err := pgtable.ValidateExclusionRef(ref); err != nil {
		return catalogExclusion{}, err
	}
	if index == "" {
		return catalogExclusion{}, fmt.Errorf("%w: EXCLUDE constraint %s.%s.%s has no backing index", schemaext.ErrInvalidValue, schema, table, name)
	}
	return catalogExclusion{
		object: schemaext.Object{Ref: ref, Value: value},
		index:  objectidentity.NewBuilder(identifier.ForDialect(r.dialect)).IndexParts(r.outputSchema(schema), table, index),
	}, nil
}
