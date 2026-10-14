package sqlschema

import (
	"context"
	"fmt"
	"strings"

	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/schemamerge"
	"ptah.run/internal/sqldecl"
)

// Load is what [ReadLoad] reads from the documents of one load.
type Load struct {
	// Database is what the whole load declares, with what each document's
	// header says about its coverage.
	Database schemamodel.Database
	// Files is what each document declares, in the request's order, for a
	// schema directory's check that a later file does not declare again what
	// an earlier one did.
	Files []sqlext.FileDeclarations
}

// ReadLoad reads the documents of one load of a target whose documents an
// owner's grammar reads, as YQL is for YDB, in one call to the owner.
//
// The owner reads the documents as one script, so a later document may change
// what an earlier one declared, and only that: a document of the load cannot
// change what a source in another format declares. The reader adds what each
// document's `ptah:not-described` header leaves undescribed and the owner's
// claim, limited by the header entries the owner reads; the coverage of a load
// of several documents is the union of theirs, as for a schema directory.
//
// request.Target is the dialect the documents are written for. A refusal of
// one document is a [*sqlext.FileError] naming it.
func ReadLoad(ctx context.Context, request sqlext.GrammarRequest, opts Options) (Load, error) {
	if !opts.Owners.Selected() {
		return Load{}, sqlext.ErrUnselected
	}
	dialect := request.Target
	request.Target = ownerTarget(dialect)
	grammar, ok := opts.Owners.Grammar(request.Target)
	if !ok {
		return Load{}, fmt.Errorf("no SQL owner reads %s documents as a load", dialect)
	}
	for _, file := range request.Files {
		if err := refuseFeatureCoverageHeader([]byte(file.Text)); err != nil {
			return Load{}, &sqlext.FileError{Path: file.Path, Err: err}
		}
	}
	result, err := grammar.ReadSQL(ctx, request)
	if err != nil {
		return Load{}, err
	}
	if err := checkAnswered(request.Files, result.Files); err != nil {
		return Load{}, err
	}
	database := result.Database
	if err := describeLoad(&database, request.Files, dialect, opts); err != nil {
		return Load{}, err
	}
	schemamodel.Finalize(&database)
	return Load{Database: database, Files: result.Files}, nil
}

// checkAnswered refuses an owner's answer that does not list each requested
// file once, in the request's order: the directory check would otherwise
// compare one file's declarations under another's name.
func checkAnswered(requested []sqlext.SourceFile, answered []sqlext.FileDeclarations) error {
	if len(answered) != len(requested) {
		return fmt.Errorf("the SQL owner answered %d files for a load of %d", len(answered), len(requested))
	}
	for i, file := range requested {
		if answered[i].Path != file.Path {
			return fmt.Errorf("the SQL owner answered file %d as %q, not %q", i+1, answered[i].Path, file.Path)
		}
	}
	return nil
}

// describeLoad sets the coverage of a load's database from each document's
// header and the owners' claims, replacing whatever the owner set. A document
// read alone is described as [ReadWith] describes one; several are merged in
// order as a schema directory merges its files, so the claim of the whole is
// the union of what each leaves undescribed.
func describeLoad(database *schemamodel.Database, files []sqlext.SourceFile, dialect string, opts Options) error {
	database.NotDescribed = schemacoverage.Set{}
	database.FeatureCoverage = schemaext.Coverage{}
	if len(files) == 1 {
		if err := describe(database, []byte(files[0].Text), dialect, opts); err != nil {
			return &sqlext.FileError{Path: files[0].Path, Err: err}
		}
		return nil
	}
	var described schemamodel.Database
	for _, file := range files {
		header := schemamodel.Database{FeatureObjects: database.FeatureObjects}
		if err := describe(&header, []byte(file.Text), dialect, opts); err != nil {
			return &sqlext.FileError{Path: file.Path, Err: err}
		}
		header.FeatureObjects = schemaext.Objects{}
		if err := schemamerge.Append(&described, &header); err != nil {
			return &sqlext.FileError{Path: file.Path, Err: err}
		}
	}
	database.NotDescribed = described.NotDescribed
	database.FeatureCoverage = described.FeatureCoverage
	return nil
}

// ConvertScript converts the documents of a load as one script run in file
// order, and says what each declares. The owner whose grammar reads the
// target's documents, as the YDB owner reads YQL, parses each document and
// converts the statements of its own that the shared conversion does not
// model; the shared conversion converts the rest, such as tables, columns,
// indexes and views. request.Target is the dialect the documents are written
// for.
//
// It is the owner's in-process use of this package's conversion, and plugin
// debt: a provider served over the protocol returns its reading of a load as
// [sqlext.GrammarResult] data and cannot reach this package
// (stokaro/ptah#4204).
//
// A document read alone is converted on its own, and the documents of a larger
// load are merged in order, so a later REVOKE takes back an earlier GRANT. A
// refusal of one document, its parse included, is a [*sqlext.FileError] naming
// it, and the documents after it are not read. The result describes no
// coverage; [ReadLoad] adds it.
func ConvertScript(ctx context.Context, request sqlext.GrammarRequest, script Converter) (sqlext.GrammarResult, error) {
	merged := &schemamodel.Database{}
	document := NewDocument(merged)
	document.script, document.databasePath = script, request.DatabasePath
	result := sqlext.GrammarResult{Files: make([]sqlext.FileDeclarations, 0, len(request.Files))}
	for _, file := range request.Files {
		statements, err := script.Parse(ctx, file)
		if err != nil {
			return sqlext.GrammarResult{}, &sqlext.FileError{Path: file.Path, Err: err}
		}
		// A grammar target consults no owner's patterns, so no other owner's
		// conversion takes part.
		database, err := convertStatements(ctx, statements, nil, request.Target, document, sqlext.None(), nil)
		if err != nil {
			return sqlext.GrammarResult{}, &sqlext.FileError{Path: file.Path, Err: err}
		}
		schemamodel.Finalize(&database)
		result.Files = append(result.Files, sqldecl.Of(file.Path, &database, statements))
		if len(request.Files) == 1 {
			result.Database = database
			return result, nil
		}
		if err := schemamerge.Append(merged, &database); err != nil {
			return sqlext.GrammarResult{}, &sqlext.FileError{Path: file.Path, Err: err}
		}
	}
	result.Database = *merged
	return result, nil
}

// ReadsAsLoad reports whether an owner among owners reads the documents of
// dialect with its own grammar, as the YDB owner reads YQL, so a load of them
// is read by [ReadLoad] rather than one file at a time.
func ReadsAsLoad(dialect string, owners sqlext.Set) bool {
	_, ok := owners.Grammar(ownerTarget(dialect))
	return ok
}

// ownerTarget names the target whose owners a document of dialect consults:
// the canonical name of a built-in target, and otherwise the spelling itself,
// so a target a provider registers is found as the parser finds it.
func ownerTarget(dialect string) string {
	if target := platform.NormalizeDialect(dialect); target != "" {
		return target
	}
	return strings.ToLower(strings.TrimSpace(dialect))
}
