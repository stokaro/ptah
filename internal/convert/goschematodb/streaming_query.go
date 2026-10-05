package goschematodb

import (
	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

func toDBStreamingQueries(queries []schemamodel.StreamingQuery) []catalog.StreamingQuery {
	var out []catalog.StreamingQuery
	for _, query := range queries {
		out = append(out, catalog.StreamingQuery{Name: query.Name, Schema: query.Schema, Spec: query.Spec.Clone()})
	}
	return out
}
