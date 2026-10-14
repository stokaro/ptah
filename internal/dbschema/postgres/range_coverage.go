package postgres

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

func recordRangeCoverage(database *catalog.Database, knowledge schemaext.Knowledge) error {
	known, err := pgtype.Coverage(pgtype.RangeKind, schemaext.Observed, knowledge, nil)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}
