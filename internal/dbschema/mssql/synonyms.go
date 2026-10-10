package mssql

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/synonym"
)

// readSynonyms reads the synonyms the connected database declares.
//
// base_object_name is the target as the server stored it, with its own
// bracket quoting, and SQL Server writes it right-aligned: the last part is
// the object, and an absent middle part is an empty pair of brackets, so
// `[srv]..[dbo].[t]` names a linked server and no database. The target is
// kept in the spelling a declaration uses, see [synonym.DeclaredTarget], which
// keeps that empty part.
func (r *Reader) readSynonyms(ctx context.Context) ([]synonym.ObservedSynonym, error) {
	query := `
		SELECT s.name, sy.name, sy.base_object_name
		FROM sys.synonyms AS sy
		JOIN sys.schemas AS s ON s.schema_id = sy.schema_id
		WHERE sy.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, sy.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var synonyms []synonym.ObservedSynonym
	for rows.Next() {
		var observed synonym.ObservedSynonym
		var stored string
		if err := rows.Scan(&observed.Schema, &observed.Name, &stored); err != nil {
			return nil, err
		}
		observed.Schema = r.outputSchema(observed.Schema)
		observed.Target = synonym.DeclaredTarget(synonym.TargetParts(stored))
		synonyms = append(synonyms, observed)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return synonyms, nil
}

// recordSynonyms adds the synonyms the read found to db as the owner's
// objects, and the knowledge that the read looked: a synonym this read did not
// return is one the database does not have.
func recordSynonyms(db *catalog.Database, synonyms []synonym.ObservedSynonym) error {
	objects := db.FeatureObjects
	for _, observed := range synonyms {
		if err := synonym.ValidateObserved(&observed); err != nil {
			return err
		}
		var err error
		if objects, err = objects.With(synonym.ObservedObject(observed)); err != nil {
			return err
		}
	}
	known, err := synonym.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return err
	}
	if db.FeatureCoverage, err = db.FeatureCoverage.Combine(known); err != nil {
		return err
	}
	db.FeatureObjects = objects
	return nil
}
