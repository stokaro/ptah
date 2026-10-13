package postgres

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

// deleteColumnsCoverage records complete selection knowledge only for the
// constraints this reader inspects. Missing constraints remain uninspected.
func (r *Reader) deleteColumnsCoverage(database *catalog.Database) error {
	if !r.caps.Has(capability.ForeignKeyDeleteColumnList) {
		return nil
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(r.dialect))
	subjects := make([]schemaext.SubjectCoverage, 0, len(database.Constraints))
	for _, constraint := range database.Constraints {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgtable.DeleteColumnsKind,
			Subject: identities.ConstraintParts(constraint.Schema, constraint.TableName, constraint.Name), Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	known, err := pgtable.DeleteColumnsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned constraints have inspected ON DELETE column lists"}, subjects)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}
