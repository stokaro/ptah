package postgres

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

// persistenceCoverage records complete observations for returned PostgreSQL
// tables. It makes no claim about tables the reader does not return.
func (r *Reader) persistenceCoverage(database *catalog.Database) error {
	if !pgtable.SupportsPersistence(r.dialect) {
		return nil
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(r.dialect))
	subjects := make([]schemaext.SubjectCoverage, 0, len(database.Tables))
	for _, table := range database.Tables {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgtable.PersistenceKind,
			Subject: identities.TableParts(table.Schema, table.Name), Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	known, err := pgtable.PersistenceCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables have inspected persistence"}, subjects)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}
