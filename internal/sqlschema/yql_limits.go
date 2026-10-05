package sqlschema

import (
	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
)

// markYQLLimits keeps an absent declaration from removing a family this
// frontend cannot read yet. It lives at ReadOnto, including for direct callers
// and empty documents, rather than only at the schema-file adapter.
func markYQLLimits(database *schemamodel.Database) {
	for _, kind := range []schemacoverage.Kind{
		schemacoverage.Replication, schemacoverage.Transfer, schemacoverage.Secret,
		schemacoverage.ExternalDataSource, schemacoverage.ExternalTable, schemacoverage.StreamingQuery,
		schemacoverage.Changefeed, schemacoverage.Role, schemacoverage.Grant,
	} {
		database.NotDescribed = database.NotDescribed.With(schemacoverage.Object{
			Kind: kind, Reason: schemacoverage.Unsupported, Provenance: schemacoverage.DerivedFromFact,
		})
	}
}
