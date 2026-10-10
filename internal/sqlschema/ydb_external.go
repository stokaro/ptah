package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbexternal"
)

// appendExternalDeclaration declares the data source or the external table a
// CREATE [OR REPLACE] EXTERNAL statement names. A statement that drops one is
// no declaration.
func appendExternalDeclaration(database *schemamodel.Database, payload ast.ExtensionPayload) (bool, error) {
	var err error
	switch value := payload.(type) {
	case *ydbast.ExternalDataSource:
		if value.Operation == ydbast.ExternalDrop {
			return false, nil
		}
		database.FeatureObjects, err = ydbexternal.DeclareSource(database.FeatureObjects, value.Schema, value.Name, "", value.Spec)
	case *ydbast.ExternalTable:
		if value.Operation == ydbast.ExternalDrop {
			return false, nil
		}
		database.FeatureObjects, err = ydbexternal.DeclareTable(database.FeatureObjects, value.Schema, value.Name, "", value.Spec)
	default:
		return false, nil
	}
	return true, err
}
