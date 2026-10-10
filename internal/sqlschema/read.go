package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/spanner/spannersource"
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
	// SQL sources have their own declaration vocabulary. Ignoring a captured
	// HCL account would replace its explicit claims with that vocabulary.
	for body := range schemacoverage.HeaderComments(string(data)) {
		if strings.HasPrefix(body, schemaext.CoverageHeaderMarker) {
			return schemamodel.Database{}, nil, fmt.Errorf("%w: SQL schema sources cannot decode a feature coverage header; read the HCL source instead", ptaherr.ErrUnsupportedFeature)
		}
	}
	statements, err := parser.NewParser(string(data), parser.WithDialect(dialect)).Parse()
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	alone := document == nil
	if alone {
		document = NewDocument(nil)
	}
	database, err := toDatabase(statements, dialect, document)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	var limits ydbsource.Limits
	var extension schemacoverage.HeaderExtension
	yql := platform.NormalizeDialect(dialect) == platform.YDB
	if yql {
		extension = limits.ConsumeDirective
	}
	header, err := schemacoverage.DecodeHeader(string(data), extension)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	database.NotDescribed = database.NotDescribed.Merge(header)
	switch {
	case yql:
		database.FeatureCoverage, err = ydbsource.Coverage(limits)
	case platform.NormalizeDialect(dialect) == platform.Spanner:
		// Spanner SQL spells a row deletion policy as a table clause, so a
		// table without one requests none.
		database.FeatureCoverage, err = spannersource.Coverage()
	}
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	if alone {
		if err := OwnRowSecurity(&database, dialect); err != nil {
			return schemamodel.Database{}, nil, err
		}
	}
	schemamodel.Finalize(&database)
	return database, statements, nil
}
