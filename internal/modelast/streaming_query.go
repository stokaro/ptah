package modelast

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
)

// FromStreamingQuery lowers a declaration to an independent create statement.
func FromStreamingQuery(query schemamodel.StreamingQuery) *ast.ExtensionStatement {
	return &ast.ExtensionStatement{Payload: &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate,
		Schema: query.Schema, Name: query.Name, Spec: query.Spec.Clone(), Creation: ydbast.StreamingCreation{OrReplace: query.AllowStateReset}}}
}
