package goschematogo

import (
	"strconv"

	"ptah.run/dialect/ydb/ydbstreaming"
)

func (ctx *renderContext) writeStreamingQueries(w *sourceWriter) {
	for _, text := range ctx.streamingAnnotations {
		w.writeComment(text)
	}
}

func streamingQueryAnnotation(schema, name string, query *ydbstreaming.Desired) string {
	return annotation("ptah:schema:streamingquery",
		attr{name: "name", value: name, set: true},
		attr{name: "schema", value: schema, set: schema != ""},
		attr{name: "text", value: query.Spec.Text, set: true},
		attr{name: "run", value: strconv.FormatBool(ydbstreaming.Running(query.Spec)), set: true},
		attr{name: "resource_pool", value: ydbstreaming.Pool(query.Spec), set: true},
		attr{name: "allow_state_reset", value: "true", set: query.AllowStateReset},
	)
}
