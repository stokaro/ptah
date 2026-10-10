package oracle

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/synonym"
)

// synonymQuery reads the synonyms the schema owns. A public synonym belongs to
// PUBLIC rather than to the schema, so it is not one of them.
const synonymQuery = `
SELECT s.synonym_name, s.table_owner, s.table_name, s.db_link
FROM all_synonyms s
WHERE s.owner = :1
ORDER BY s.synonym_name`

// readSynonyms reads the schema's synonyms and records them, with the
// knowledge that the read looked, so a synonym this read did not return is one
// the schema does not have.
//
// Oracle records the target's owner whether the statement named one or not,
// so the target is written as owner.object. A synonym through a database link
// cannot be declared, since a target has no place for the link, so it is
// recorded as unrepresentable rather than as an object: a comparison leaves
// it as it is.
func (r *Reader) readSynonyms(ctx context.Context, db *catalog.Database) error {
	rows, err := r.db.QueryContext(ctx, synonymQuery, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	objects := db.FeatureObjects
	var unrepresentable []schemaext.SubjectCoverage
	for rows.Next() {
		var name string
		var owner, object, link sql.NullString
		if err := rows.Scan(&name, &owner, &object, &link); err != nil {
			return err
		}
		observed := synonym.ObservedSynonym{Synonym: synonym.Synonym{Schema: r.schema, Name: name,
			Target: synonym.DeclaredTarget([4]string{"", "", owner.String, object.String})}}
		if link.Valid && link.String != "" {
			unrepresentable = append(unrepresentable, schemaext.SubjectCoverage{Kind: synonym.Kind, Subject: observed.Ref(),
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable,
					Reason: fmt.Sprintf("the synonym resolves through database link %s, which a declaration cannot name", link.String)}})
			continue
		}
		if err := synonym.ValidateObserved(&observed); err != nil {
			return err
		}
		if objects, err = objects.With(synonym.ObservedObject(observed)); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	known, err := synonym.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, unrepresentable)
	if err != nil {
		return err
	}
	if db.FeatureCoverage, err = db.FeatureCoverage.Combine(known); err != nil {
		return err
	}
	db.FeatureObjects = objects
	return nil
}
