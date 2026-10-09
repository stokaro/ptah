package sqlschema

import (
	"ptah.run/core/ast"
	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
	"ptah.run/internal/ydbsource"
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

// ReadOnto is [Read] for one file of a schema directory, read against
// document, what the directory's earlier files declared.
//
// A directory is one script run in file order, so a later file may add a
// column to a table an earlier file created, or change one of its columns or
// constraints. The result holds only what this file adds, the added column
// included, and the caller merges it with the document's model. A change to
// an object that model declares is made to it, in place, so the model must be
// the caller's own accumulated one rather than a copy; see [NewDocument]. The
// same document is passed for every file, because it carries what the model
// cannot. A nil document reads the file alone, as [Read] does.
func ReadOnto(
	data []byte, dialect string, document *Document,
) (schemamodel.Database, *ast.StatementList, error) {
	statements, err := parser.NewParser(string(data), parser.WithDialect(dialect)).Parse()
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	if document == nil {
		document = NewDocument(nil)
	}
	database, err := toDatabase(statements, dialect, document)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	if platform.NormalizeDialect(dialect) == platform.YDB {
		header, err := schemacoverage.DecodeHeader(string(data))
		if err != nil {
			return schemamodel.Database{}, nil, err
		}
		var limits ydbsource.Limits
		for _, object := range header.Objects {
			limits.Add(string(object.Kind), object.Name)
		}
		database.FeatureCoverage, err = ydbsource.Coverage(limits)
		if err != nil {
			return schemamodel.Database{}, nil, err
		}
	}
	schemamodel.Finalize(&database)
	return database, statements, nil
}
