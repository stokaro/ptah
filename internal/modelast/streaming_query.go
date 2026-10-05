package modelast

import (
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbstream"
)

// FromStreamingQuery lowers a declaration to an independent create statement.
func FromStreamingQuery(query schemamodel.StreamingQuery) *ydbstream.Node {
	return &ydbstream.Node{Operation: ydbstream.CreateOperation, Name: query.QualifiedName(), Spec: query.Spec.Clone(), Creation: ydbstream.CreateOptions{OrReplace: query.AllowStateReset}}
}
