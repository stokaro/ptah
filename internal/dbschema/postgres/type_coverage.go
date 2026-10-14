package postgres

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// recordTypeCoverage records what the read knows of the type owner's models.
// It enrolls domains and composite types as described: a common not-described
// record of either kind, which the read writes when it cannot reach them,
// narrows that claim before the owner compares. Range types take ranges.
func recordTypeCoverage(database *catalog.Database, ranges schemaext.Knowledge) error {
	for _, kind := range pgtype.Kinds() {
		knowledge := schemaext.Knowledge{State: schemaext.Complete}
		if kind == pgtype.RangeKind {
			knowledge = ranges
		}
		known, err := pgtype.Coverage(kind, schemaext.Observed, knowledge, nil)
		if err != nil {
			return err
		}
		if database.FeatureCoverage, err = database.FeatureCoverage.Combine(known); err != nil {
			return err
		}
	}
	return nil
}
