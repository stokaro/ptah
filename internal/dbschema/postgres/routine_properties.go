package postgres

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/internal/routineidentity"
)

// functionPropertiesCoverage records complete observations only for functions
// returned by this read. A missing overload remains uninspected.
func (r *Reader) functionPropertiesCoverage(database *catalog.Database) error {
	builder := objectidentity.NewBuilder(identifier.ForDialect(r.dialect))
	var subjects []schemaext.SubjectCoverage
	for _, function := range database.Functions {
		subject := routineidentity.Subject(builder, function.Kind, function.QualifiedName(), function.Parameters)
		if !pgroutine.HoldsProperties(subject) {
			continue
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgroutine.PropertiesKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	coverage, err := pgroutine.PropertiesCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned functions have inspected evaluation properties"}, subjects)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(coverage)
	return err
}
