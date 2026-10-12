package sqlschema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/featureselect"
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
//
// Read selects no owner, so it reads the shared grammar alone; [ReadWith]
// takes the owners whose syntax a document carries.
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
	return ReadWith(context.Background(), data, dialect, document, Options{Owners: sqlext.None()})
}

// Options is what a read takes from its caller beside the document.
type Options struct {
	// Owners are the owners whose syntax the document may carry, and whose
	// claims a document of the dialect makes about their models. The zero
	// value is refused; [sqlext.None] reads the shared grammar alone.
	Owners sqlext.Set
	// Vocabulary is the coverage kinds the document's `ptah:not-described`
	// header may name beside the common ones, usually the vocabulary of the
	// runtime the caller compares with, such as a YDB changefeed.
	Vocabulary schemacoverage.Vocabulary
	// Relations says which table a standalone feature object is attached to,
	// usually the runtime the caller compares with. A DROP TABLE removes the
	// objects attached to the table, as the server drops them with it, such as
	// a SQL Server extended property. Without it, a DROP TABLE in a document
	// whose model holds a standalone object is refused, since the read cannot
	// tell whether the object goes with the table. Relation discovery is
	// registered per target, so a document that names no target detaches
	// nothing and is never refused for it.
	Relations featureselect.RelationRuntime
}

// ReadWith is [ReadOnto] with the owners and the coverage vocabulary the
// caller selects. ctx bounds the owners' reading of their syntax.
//
// A dialect whose documents an owner's grammar reads, as YQL is for YDB, is
// read through that grammar as a load of one file, and returns no statements:
// the grammar answers declarations, not syntax. A file of a larger load of such
// a dialect is read with the rest of the load by [ReadLoad], so document must
// be nil for one.
func ReadWith(
	ctx context.Context, data []byte, dialect string, document *Document, opts Options,
) (schemamodel.Database, *ast.StatementList, error) {
	if !opts.Owners.Selected() {
		return schemamodel.Database{}, nil, sqlext.ErrUnselected
	}
	if err := refuseFeatureCoverageHeader(data); err != nil {
		return schemamodel.Database{}, nil, err
	}
	if ReadsAsLoad(dialect, opts.Owners) {
		if document != nil {
			return schemamodel.Database{}, nil, fmt.Errorf(
				"a %s document of a larger load is read with the rest of the load, not on its own", dialect)
		}
		loaded, err := ReadLoad(ctx, sqlext.GrammarRequest{Target: dialect, Files: []sqlext.SourceFile{{Text: string(data)}}}, opts)
		if refused, ok := errors.AsType[*sqlext.FileError](err); ok {
			return schemamodel.Database{}, nil, refused.Err
		}
		if err != nil {
			return schemamodel.Database{}, nil, err
		}
		return loaded.Database, nil, nil
	}
	database, statements, err := convert(ctx, data, dialect, document, opts.Owners, opts.Relations)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	if err := describe(&database, data, dialect, opts); err != nil {
		return schemamodel.Database{}, nil, err
	}
	schemamodel.Finalize(&database)
	return database, statements, nil
}

// refuseFeatureCoverageHeader refuses a document whose header carries an HCL
// feature coverage account: SQL sources have their own declaration
// vocabulary, and ignoring a captured HCL account would replace its explicit
// claims with that vocabulary.
func refuseFeatureCoverageHeader(data []byte) error {
	for body := range schemacoverage.HeaderComments(string(data)) {
		if strings.HasPrefix(body, schemaext.CoverageHeaderMarker) {
			return fmt.Errorf("%w: SQL schema sources cannot decode a feature coverage header; read the HCL source instead", ptaherr.ErrUnsupportedFeature)
		}
	}
	return nil
}

// convert parses data in the shared grammar, with the owners' syntax, and
// converts its statements onto document, a fresh one for a file read alone.
func convert(
	ctx context.Context, data []byte, dialect string, document *Document, owners sqlext.Set,
	relations featureselect.RelationRuntime,
) (schemamodel.Database, *ast.StatementList, error) {
	if document == nil {
		document = NewDocument(nil)
	}
	options := []parser.Option{parser.WithDialect(dialect), parser.WithSQL(owners)}
	if document.base != nil {
		options = append(options, parser.WithDeclared(document.base.FeatureObjects))
	}
	parsed := parser.NewParser(string(data), options...)
	statements, err := parsed.ParseContext(ctx)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	document.notes, document.relations = parsed, relations
	defer func() { document.notes, document.relations = nil, nil }()
	database, err := toDatabase(ctx, statements, dialect, document)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	return database, statements, nil
}

// describe adds to database what data's `ptah:not-described` header says it
// leaves undescribed, and the claims a document of the dialect makes: the
// dialect's own SQL and the owners of the dialect, each handed the header
// entries it reads as limits on its claim.
func describe(database *schemamodel.Database, data []byte, dialect string, opts Options) error {
	target := ownerTarget(dialect)
	var owned []schemacoverage.Object
	extension := func(object schemacoverage.Object) (bool, error) {
		if opts.Owners.ReadsHeader(target, object.Kind) {
			owned = append(owned, object)
			return true, nil
		}
		return false, nil
	}
	header, err := schemacoverage.DecodeHeader(string(data), opts.Vocabulary, extension)
	if err != nil {
		return err
	}
	database.NotDescribed = database.NotDescribed.Merge(header)
	// What a document of the dialect states about an owner's models is the
	// owner's claim, such as a ClickHouse materialized view's refresh
	// schedule, which its CREATE statement states, the YDB objects a YQL
	// document declares except those its header leaves unmanaged, the
	// virtual tables a SQLite document declares unless its header declines
	// them, a Spanner table's row deletion policy, or the block size of a
	// MySQL index. The owners' claims combine, since a Spanner document
	// carries both its row deletion policies and its extensions.
	claim, claimed, err := opts.Owners.Coverage(target, sqlext.CoverageRequest{Limits: owned, Declared: database.FeatureObjects})
	if err != nil || !claimed {
		return err
	}
	database.FeatureCoverage = claim
	return nil
}
