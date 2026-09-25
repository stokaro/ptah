package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
)

// Read loads a SQL desired schema into the canonical model.
//
// It is the whole of what a SQL schema source does: parse, convert, and
// finalize. Callers spelling those three steps themselves is what makes the
// conversion look like a general-purpose AST-to-model service with a package of
// its own. It is not one -- nothing else converts statements into the model,
// and nothing should have to know that finalizing is part of reading
// (stokaro/ptah#2725).
//
// The statements are returned beside the model because a source fact can
// outlive the conversion. The model records no IF NOT EXISTS for a table, so
// only the statement can say whether a redeclaration in a schema directory is
// guarded, and the declaration order a directory reports is the statements'
// rather than the model's. A caller that needs neither ignores the second
// result.
func Read(data []byte, dialect string) (schemamodel.Database, *ast.StatementList, error) {
	return ReadOnto(data, dialect, nil)
}

// ReadOnto is [Read] for one file of a schema directory, read against base,
// what the directory's earlier files declared.
//
// A directory is one script run in file order, so a later file may add a
// column to a table an earlier file created, or change one of its columns or
// constraints. The result holds only what this file adds, the added column
// included, and the caller merges it with base. A change to an object base
// declares is made to base, in place, so base must be the caller's own
// accumulated document rather than a copy. A nil base reads the file alone, as
// [Read] does.
func ReadOnto(
	data []byte, dialect string, base *schemamodel.Database,
) (schemamodel.Database, *ast.StatementList, error) {
	statements, err := parser.NewParser(string(data), parser.WithDialect(dialect)).Parse()
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	database, err := toDatabase(statements, dialect, base)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	schemamodel.Finalize(&database)
	return database, statements, nil
}
