package sqlext

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Grammar reads a target whose documents are not written in the shared
// grammar, as YQL is for YDB. It reads every document of one load in one
// call, in the order the load runs them as one script, so the script's own
// rules about a later file changing what an earlier one declared stay with the
// owner. A document read alone is a load of one file.
type Grammar interface {
	ReadSQL(ctx context.Context, request GrammarRequest) (GrammarResult, error)
}

// SourceFile is one document of a load.
type SourceFile struct {
	// Path names the document as the load does, which a refusal names it
	// by.
	Path string
	// Text is the document's text.
	Text string
}

// GrammarRequest is every document of one load of a target.
type GrammarRequest struct {
	// Target is the target the documents are written for.
	Target string
	// Files are the documents in the order the load runs them.
	Files []SourceFile
	// DatabasePath is the database root a document's absolute paths are read
	// against, or empty where the load names none.
	DatabasePath string
}

// GrammarResult is an owner's reading of a [GrammarRequest].
type GrammarResult struct {
	// Database is what the whole script declares, with each file's changes
	// to what an earlier file declared applied. It holds the common model;
	// the owner's objects and facets inside it are values of the owner's
	// models. The reader adds what each document's header says about its
	// coverage.
	Database schemamodel.Database
	// Files holds what each file declares, one entry per requested file in
	// the request's order, for the reader's check that a later file of a
	// schema directory does not declare again what an earlier one did.
	Files []FileDeclarations
	// Coverage is the owner's claim about its models for the whole load,
	// made by the same reading that produced Database, as an owner served by
	// another process returns it. The reader combines it with what the
	// extension's Coverage claims for each document. The zero value adds
	// nothing.
	Coverage schemaext.Coverage
}

// FileDeclarations is what one file of a load declares, in the reader's
// terms for its directory check: an object kind and the name the file gives
// the object, such as "table" and "app.users".
type FileDeclarations struct {
	// Path names the file, as the request did.
	Path string
	// Objects are the objects the file leaves declared once it has run.
	Objects []Declaration
	// Guards are the objects the file declares under a clause that admits
	// them existing already, such as IF NOT EXISTS.
	Guards []Declaration
	// Tables are the file's CREATE TABLE and DROP TABLE statements in the
	// order the file runs them.
	Tables []TableStatement
}

// Declaration is one object a file declares.
type Declaration struct {
	// Kind is the object's kind, such as "table", "view" or "index".
	Kind string
	// Name is the object's name as the file writes it, qualified where the
	// file qualifies it.
	Name string
}

// TableStatement is one CREATE TABLE or DROP TABLE a file runs.
type TableStatement struct {
	// Name is the table's name as the statement writes it.
	Name string
	// Drop reports a DROP TABLE.
	Drop bool
	// Guarded reports a CREATE TABLE under IF NOT EXISTS.
	Guarded bool
}

// FileError is a refusal of one file of a load.
type FileError struct {
	// Path names the refused file, as the request did.
	Path string
	// Err says why.
	Err error
}

// Error names the file and the reason.
func (e *FileError) Error() string {
	return e.Path + ": " + e.Err.Error()
}

// Unwrap returns the reason.
func (e *FileError) Unwrap() error {
	return e.Err
}
