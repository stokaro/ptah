package dbschematogo

import (
	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

func convertStreamingQueries(database *schemamodel.Database, queries []catalog.StreamingQuery) {
	for _, query := range queries {
		database.StreamingQueries = append(database.StreamingQueries, schemamodel.StreamingQuery{Name: query.Name, Schema: query.Schema, Spec: query.Spec.Clone()})
	}
}
