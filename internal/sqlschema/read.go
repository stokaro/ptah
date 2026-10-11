package sqlschema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/dialect/spanner/spannersource"
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
		if err := OwnRowSecurity(&loaded.Database, dialect); err != nil {
			return schemamodel.Database{}, nil, err
		}
		return loaded.Database, nil, nil
	}
	alone := document == nil
	database, statements, err := convert(ctx, data, dialect, document, opts.Owners)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	if err := describe(&database, data, dialect, opts); err != nil {
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
) (schemamodel.Database, *ast.StatementList, error) {
	parsed := parser.NewParser(string(data), parser.WithDialect(dialect), parser.WithSQL(owners))
	statements, err := parsed.ParseContext(ctx)
	if err != nil {
		return schemamodel.Database{}, nil, err
	}
	if document == nil {
		document = NewDocument(nil)
	}
	document.notes = parsed
	defer func() { document.notes = nil }()
	database, err := toDatabase(statements, dialect, document)
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
	var virtual sqliteVirtualLimits
	extension := func(object schemacoverage.Object) (bool, error) {
		if opts.Owners.ReadsHeader(target, object.Kind) {
			owned = append(owned, object)
			return true, nil
		}
		if target == platform.SQLite {
			return virtual.consume(object)
		}
		return false, nil
	}
	header, err := schemacoverage.DecodeHeader(string(data), opts.Vocabulary, extension)
	if err != nil {
		return err
	}
	database.NotDescribed = database.NotDescribed.Merge(header)
	switch target {
	case platform.Spanner:
		// Spanner SQL spells a row deletion policy as a table clause, so a
		// table without one requests none.
		database.FeatureCoverage, err = spannersource.Coverage()
	case platform.MySQL, platform.MariaDB:
		// An index states its KEY_BLOCK_SIZE in its own definition, so an
		// index without one declares none.
		database.FeatureCoverage, err = mysqlsource.BlockSizeCoverage()
	case platform.SQLite:
		// SQLite SQL spells a virtual table as CREATE VIRTUAL TABLE, so a
		// table declared with CREATE TABLE is an ordinary one, and a document
		// that leaves a live virtual table out asks for it to go, unless its
		// header declines virtual tables.
		database.FeatureCoverage, err = virtual.coverage()
	}
	if err != nil {
		return err
	}
	// What a document of the dialect states about an owner's models is the
	// owner's claim, such as a ClickHouse materialized view's refresh
	// schedule, which its CREATE statement states, or the YDB objects a YQL
	// document declares except those its header leaves unmanaged.
	claim, claimed, err := opts.Owners.Coverage(target, owned)
	if err != nil {
		return err
	}
	if claimed {
		database.FeatureCoverage = claim
	}
	return nil
}
