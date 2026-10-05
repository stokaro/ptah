package goschematogo

import (
	"strconv"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbstream"
)

func (ctx *renderContext) writeStreamingQueries(w *sourceWriter) {
	for _, query := range sortedByName(ctx.db.StreamingQueries, schemamodel.StreamingQuery.QualifiedName) {
		w.writeComment(annotation("ptah:schema:streamingquery",
			attr{name: "name", value: query.Name, set: true},
			attr{name: "schema", value: query.Schema, set: query.Schema != ""},
			attr{name: "text", value: query.Spec.Text, set: true},
			attr{name: "run", value: strconv.FormatBool(ydbstream.Running(query.Spec)), set: true},
			attr{name: "resource_pool", value: ydbstream.Pool(query.Spec), set: true},
			attr{name: "allow_state_reset", value: "true", set: query.AllowStateReset},
		))
	}
}
