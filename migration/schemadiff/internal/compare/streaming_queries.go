package compare

import (
	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbstream"
	"ptah.run/migration/schemadiff/difftypes"
)

// StreamingQueries compares persistent settings and respects both sides'
// coverage. A format that cannot declare a streaming query never drops one.
func StreamingQueries(desired *schemamodel.Database, database *catalog.Database, diff *difftypes.SchemaDiff, cov Coverage) {
	held := make(map[string]schemamodel.StreamingQuery, len(database.StreamingQueries))
	for _, query := range database.StreamingQueries {
		held[query.QualifiedName()] = schemamodel.StreamingQuery{Name: query.Name, Schema: query.Schema, Spec: query.Spec.Clone()}
	}
	declared := make(map[string]bool, len(desired.StreamingQueries))
	var added []schemamodel.StreamingQuery
	for _, query := range desired.StreamingQueries {
		query.Spec = query.Spec.Clone()
		name := query.QualifiedName()
		declared[name] = true
		current, exists := held[name]
		switch {
		case !exists:
			added = append(added, query)
		case !ydbstream.Equal(query.Spec, current.Spec):
			diff.StreamingQueriesChanged = append(diff.StreamingQueriesChanged, difftypes.StreamingQueryChange{Desired: query, Current: current})
		}
	}
	for name, query := range held {
		if !declared[name] && cov.PlansRemoval(coverage.StreamingQuery, query.Schema, query.Name, name) {
			diff.StreamingQueriesRemoved = append(diff.StreamingQueriesRemoved, query)
		}
	}
	kept, withheld := keepPlannedAdditions(cov, coverage.StreamingQuery, added,
		func(query schemamodel.StreamingQuery) (string, []string) {
			return query.Schema, []string{query.Name, query.QualifiedName()}
		},
		schemamodel.StreamingQuery.QualifiedName, unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.StreamingQueriesAdded = kept
	sortByName(diff.StreamingQueriesAdded, schemamodel.StreamingQuery.QualifiedName)
	sortByName(diff.StreamingQueriesRemoved, schemamodel.StreamingQuery.QualifiedName)
	sortByName(diff.StreamingQueriesChanged, func(change difftypes.StreamingQueryChange) string { return change.Desired.QualifiedName() })
}
