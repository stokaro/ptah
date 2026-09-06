package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
)

// Read loads a SQL desired schema into the canonical model.
//
// It is the whole of what a SQL schema source does: parse, convert, and
// finalize. Both callers used to spell those three steps themselves, which is
// how the conversion came to look like a general-purpose AST-to-model service
// with its own package under internal/convert. It is not one -- nothing else
// converts statements into the model, and nothing should have to know that
// finalizing is part of reading (stokaro/ptah#2725).
//
// The statements are returned beside the model because a source fact can
// outlive the conversion. The model records no IF NOT EXISTS for a table, so
// only the statement can say whether a redeclaration in a schema directory is
// guarded, and the declaration order a directory reports is the statements'
// rather than the model's. A caller that needs neither ignores the second
// result.
func Read(data []byte, dialect string) (schemamodel.Database, *ast.StatementList, error) {
	statements, err := parser.NewParser(string(data), parser.WithDialect(dialect)).Parse()
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	database, err := ToDatabase(statements, dialect)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	schemamodel.Finalize(&database)
	return database, statements, nil
}
