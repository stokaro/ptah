package compare

import (
	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Features carries every named feature object of both sides on diff, changed
// or not, for a planner whose rule about one object depends on others the
// change set does not name: the replica tables a YDB async replication owns,
// and the table and the topic a transfer depends on.
func Features(desired *schemamodel.Database, database *catalog.Database, diff *difftypes.SchemaDiff) {
	diff.Features = difftypes.FeatureContext{
		DesiredObjects:  desired.FeatureObjects,
		CurrentObjects:  database.FeatureObjects,
		CurrentCoverage: database.FeatureCoverage,
	}
}
