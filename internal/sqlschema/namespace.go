package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// appendNamespace folds a CREATE SCHEMA or CREATE DATABASE into the database
// being built, and reports whether stmt was one of the two.
//
// A CREATE SCHEMA declares a schema on every dialect. A CREATE DATABASE
// declares one on the MySQL family and nothing elsewhere. MySQL and MariaDB
// have one namespace under two names: CREATE DATABASE and CREATE SCHEMA are
// synonyms there, so a database is what the model calls a schema. Without
// that, a schema file for a whole server declares its tables in databases
// nothing creates, and materializing it on a dev server fails with ERROR 1049,
// unknown database (stokaro/ptah#3885). On the other dialects a CREATE
// DATABASE names the database this model already is, so it declares nothing.
func appendNamespace(database *schemamodel.Database, stmt ast.Node, sourcePlatform string) bool {
	switch node := stmt.(type) {
	case *ast.CreateSchemaNode:
		database.Schemas = append(database.Schemas, schemamodel.Schema{
			Name:    normalizeSQLIdentifier(sourcePlatform, node.Name),
			Comment: node.Comment,
			Charset: node.Charset,
			Collate: node.Collate,
		})
		return true
	case *ast.CreateDatabaseNode:
		switch platform.NormalizeDialect(sourcePlatform) {
		case platform.MySQL, platform.MariaDB:
			database.Schemas = append(database.Schemas, schemamodel.Schema{
				Name: normalizeSQLIdentifier(sourcePlatform, node.Name),
			})
		}
		return true
	default:
		return false
	}
}
