// Package sqlext is the contract between the SQL schema frontend and the
// feature owners whose syntax a SQL document can carry.
//
// The frontend, Ptah's SQL reader, parses the grammar every target shares. An
// owner registers the syntax it adds to that grammar as data: each [Pattern]
// names the [Site] where the syntax may stand, the words it opens with, the
// words that may or may not follow them, and the words that end it. The
// frontend finds every span with its own lexer and hands the spans an owner
// claimed to the owner's [Parser], in one call per document, which returns one
// [Value] per span. The frontend applies the values. Nothing here is called
// once per token or holds a callback into the frontend, so an owner can answer
// from another process. A [Set] freezes the owners one parse selects; the
// frontend imports no owner.
package sqlext

import (
	"context"
	"errors"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
)

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner passes [None] instead, so a forgotten selection
// is refused rather than read as a decision to leave an owner's syntax
// unread.
var ErrUnselected = errors.New("no SQL schema owners were selected")

// ErrUndeclared marks the refusal of a statement that changes an object no
// document of its load declares, such as an ALTER TABLE of a table nobody
// created. A reader that loaded sources in another format beside the load
// tests for it to say that the object may be theirs, which no document of the
// load can change. The refusal keeps its own text.
var ErrUndeclared = errors.New("the statement changes an object its load does not declare")

// Site is the place in a shared statement where an owner's syntax may stand.
// Each site says where a span that opens there ends at the latest; a
// pattern's Stops may end it earlier.
type Site string

const (
	// TableElement is an element of a CREATE TABLE column list, where a
	// column or a table constraint stands. A span ends at the next top-level
	// comma or at the parenthesis that closes the list. Its [Value] declares
	// an index or named child objects of the table.
	TableElement Site = "table-element"
	// AlterOperation is one operation of an ALTER TABLE statement. A span
	// ends at the next top-level comma or at the end of the statement. Its
	// [Value] is the owner's operation, and may declare an index of the
	// table or named child objects too.
	AlterOperation Site = "alter-operation"
	// ViewClause is a clause of a CREATE MATERIALIZED VIEW statement between
	// the view's name and AS. A span ends at a top-level AS or at the end of
	// the statement. Its [Value] carries the owner's facets of the view.
	ViewClause Site = "view-clause"
	// TablePrefix is an owned modifier between CREATE and TABLE. A span ends
	// before TABLE or the next owner's prefix. Its Value carries facets of the
	// table, and its Claim.Subject names the table after the frontend reads it.
	TablePrefix Site = "table-prefix"
	// TableOption is an option of a CREATE TABLE statement after its column
	// list, among the table's options. A span ends at the next top-level
	// comma or at the end of the statement. Its [Value] carries the owner's
	// facets of the table.
	TableOption Site = "table-option"
	// ColumnOption is an option of a column definition after its type, among
	// the column's constraints, in CREATE TABLE and in the column definitions
	// of ALTER TABLE. A span ends at the next top-level comma, at the
	// parenthesis that closes the column list, at the end of the statement,
	// or at the next word that opens a column constraint the frontend reads
	// or another ColumnOption pattern of the document's owners. Its [Value]
	// carries the owner's facets of the column.
	ColumnOption Site = "column-option"
	// IndexOption is an option of a key after its parts: of a PRIMARY KEY,
	// UNIQUE, KEY, INDEX, FULLTEXT or SPATIAL element of CREATE TABLE or of
	// ALTER TABLE ADD, and of CREATE INDEX. It may stand inside a version
	// guard among the options, `/*!50100 ... */`, where the guard's body is
	// read as options. A span ends at the next top-level comma, at the
	// parenthesis that closes the column list, at the end of the statement or
	// of the guard, at the next version guard, or at the next word that opens
	// a key option the frontend reads or another IndexOption pattern of the
	// document's owners. Its
	// [Value] carries the owner's facets of the key's index, or of the
	// PRIMARY KEY constraint. A UNIQUE element with such an option is read as
	// a unique index, as a key's options belong to its index.
	IndexOption Site = "index-option"
	// RoutineOption is an owned clause of a CREATE FUNCTION or CREATE
	// PROCEDURE header. A comma-separated WITH list supplies whole options.
	// Adjacent function clauses end at the next shared clause or owner pattern.
	// Its Value carries facets of the routine.
	RoutineOption Site = "routine-option"
	// Statement is a whole top-level statement. A span ends at the semicolon
	// that ends the statement or at the end of the document. Its [Value] is
	// the owner's statement, and may declare objects of the owner or amend
	// objects declared before it, or it is a table the statement declares.
	Statement Site = "statement"
)

// Pattern is one piece of syntax an owner adds at a site. The frontend
// compares words without regard to case.
type Pattern struct {
	// Name identifies the pattern within its extension. Each claim carries
	// it, so the owner knows which of its patterns matched.
	Name string
	// Site is where the syntax stands.
	Site Site
	// Words are the words the syntax opens with.
	Words []string
	// AfterName, when it is not empty, are the words that must follow the
	// qualified name of the object a statement opens with, after Words, such
	// as AS in `CREATE TYPE app.addr AS (`. The frontend reads the name with
	// its own grammar for one: an identifier, then a dot and an identifier as
	// often as the document writes them. Next and NotNext then apply to the
	// word after AfterName. Statement, TableElement and AlterOperation
	// patterns take it, including `CONSTRAINT name EXCLUDE` elements.
	AfterName []string
	// Next, when it is not empty, lists the words one of which must follow
	// Words, or AfterName where the pattern has it.
	Next []string
	// NotNext lists the words that must not follow Words, or AfterName where
	// the pattern has it.
	NotNext []string
	// Stops are the top-level words, or punctuation such as "(", that end
	// the span before the site would.
	Stops []string
	// Targeted restricts the pattern to documents that name one of the
	// extension's targets. A pattern without it is consulted on a document
	// that names no target as well, where every owner's syntax is read.
	Targeted bool
}

// Claim is one span of a document the frontend matched to a pattern.
type Claim struct {
	// Pattern is the matched pattern's name.
	Pattern string
	// Site is the matched pattern's site.
	Site Site
	// Subject names the object the syntax belongs to, as the document
	// writes it: the table of a table element, an ALTER TABLE operation or a
	// table option, the view of a view clause, the column of a column option,
	// and the key of an index option, empty for a key the document leaves
	// unnamed.
	Subject string
	// Text is the span's source text, its opening words included.
	Text string
	// Definition numbers the element a ColumnOption, IndexOption or
	// RoutineOption claim belongs to, a column definition, a key or a
	// routine header, from 1 in document order. The claims of one element
	// share it, so an owner can combine several options of one element into
	// one facet: it answers the facet on one claim and an empty value, which
	// adds nothing, on the others. It is zero at every other site.
	Definition int
	// IndexType is the type an IndexOption claim's key declares before its
	// parts, FULLTEXT or SPATIAL in upper case, and empty for any other key
	// and at every other site.
	IndexType string
	// Offset is the byte offset of the span in the document, which an error
	// the owner returns about the span is positioned against.
	Offset int
}

// Index is an index an owner's syntax declares on its subject.
type Index struct {
	// Name is the index's name as the document writes it.
	Name string
	// Expression is what the index indexes, as source text: one value, even
	// when it is a tuple or a call with arguments.
	Expression string
	// Facets are the owner's settings of the index.
	Facets schemaext.Facets
}

// TableFacets are an owner's facets of a common table a statement sets, such
// as the row-level security switch `ALTER TABLE t ENABLE ROW LEVEL SECURITY`
// sets on t.
type TableFacets struct {
	// Name is the table's name as the statement writes it, qualified where
	// the statement qualifies it.
	Name string
	// Facets contains at least one setting of the table. Each setting has
	// one of the extension's Facets kinds.
	Facets schemaext.Facets
}

// TableDeclaration is a table an owner's statement declares and defines by
// the owner's facets alone, with no column list, such as a SQLite virtual
// table, which its module defines.
type TableDeclaration struct {
	// Name is the table's name as the statement writes it, qualified where
	// the statement qualifies it.
	Name string
	// IfNotExists reports a declaration under IF NOT EXISTS.
	IfNotExists bool
	// Facets are the owner's settings that define the table.
	Facets schemaext.Facets
}

// Value is the owner's reading of one claim.
type Value struct {
	// Index is the index a table element or an ALTER TABLE operation
	// declares. A table element requires an index or Objects, never both.
	Index *Index
	// Constraint is an owner-defined index constraint. It replaces Index and
	// Objects at a table element or ALTER TABLE addition. The frontend settles
	// its name with common constraints before declaring a schema object.
	Constraint *ast.OwnedIndexConstraint
	// Operation is the owner's payload of an ALTER TABLE operation, which an
	// alter operation requires.
	Operation ast.ExtensionPayload
	// Facets are the owner's settings of the subject, such as a materialized
	// view's refresh schedule, a table's row deletion policy or a column's
	// character set, each of one of the extension's Facets kinds.
	Facets schemaext.Facets
	// Statement is the owner's payload of a statement. A statement carries
	// it or a Table, never both. The parse places it where the statement
	// stood, for the readers of syntax such as lint.
	Statement ast.ExtensionPayload
	// Table is the common table a statement declares, which the parse places
	// where the statement stood, as the CREATE TABLE of that name, flag and
	// facets, so every reader of a table reads it. A statement carries it or
	// a Statement, never both, and a statement that declares a table declares
	// and amends no object besides it.
	Table *TableDeclaration
	// Objects are the objects a statement, table element or ALTER TABLE
	// operation declares, each of one of the extension's Kinds. Elements and
	// operations require children of their subject table and take no Index
	// beside them. The reader refuses duplicate child declarations.
	Objects []schemaext.Object
	// Replaces are new values of objects declared before the statement, in
	// the document or in [Request.Declared], such as the comment a COMMENT ON
	// statement gives an object a CREATE declared. Each is of one of the
	// extension's Kinds. The reader replaces each and refuses one nothing
	// declared. An owner refuses a statement that amends an object it cannot
	// find with an error wrapping [ErrUndeclared].
	Replaces []schemaext.Object
	// TableFacets sets the owner's facets of a table the load declares. The
	// reader merges them into the table, refuses a facet the table already
	// holds, and refuses a table the load does not declare with an error
	// wrapping [ErrUndeclared]. Only a [Statement] value carries it, beside
	// the owner's statement.
	TableFacets *TableFacets
	// TableReferences name tables the statement requires the load to declare
	// at this position, even when another claim carries its final facets.
	// Only a statement takes them; syntax-only readers do not resolve them.
	TableReferences []string
	// DescribedBy is the one-based index in Request.Claims of a later
	// statement claim that supplies this statement's combined declaration.
	// Zero means this value describes itself. The referenced value must
	// carry objects, replacements or table facets, and may not defer again.
	// Syntax-only readers keep each statement's own operation payload.
	DescribedBy int
	// UnmodeledReason explains why a valid statement cannot describe a
	// desired schema. A syntax-only reader keeps its payload, while the
	// schema reader refuses it with this reason. Only a statement takes it.
	UnmodeledReason string
	// Declined hands a statement back to the shared grammar: the owner's
	// pattern matched syntax that is not the owner's after all, such as a
	// COMMENT ON TYPE of a type another model declares. The frontend reads
	// the claim's text by itself with the shared grammar, as if no pattern
	// had matched, and places what it reads where the statement stood. Only
	// a [Statement] claim may decline, and a declining value carries no other
	// field.
	Declined bool
}

// DroppedTable identifies a common DROP TABLE boundary in the document.
// An owner uses it to keep declarations from different table lifetimes apart.
type DroppedTable struct {
	// Name is the table reference as the DROP statement writes it.
	Name string
	// Offset is the statement's byte offset in the document.
	Offset int
}

// Document supplies the context for one parse, independently of its owners.
type Document struct {
	// Declared are the objects earlier files of the load declare.
	Declared schemaext.Objects
	// DroppedTables are common DROP TABLE boundaries in source order. A
	// statement naming several tables contributes one entry per table.
	DroppedTables []DroppedTable
}

// Request is every claim of one document an extension's patterns matched.
type Request struct {
	// Target is the target the document names, or empty for a document that
	// names none.
	Target string
	// Claims are the claims, in the order the document holds them.
	Claims []Claim
	// Declared are the objects of the extension's Kinds that the documents
	// read before this one declared, such as the earlier files of a schema
	// directory, which a statement may amend. A document read alone has
	// none.
	Declared []schemaext.Object
	// DroppedTables are the common table removals in the document, in source
	// order. An owner can end a table's pending declarations at its removal
	// without parsing the shared DROP TABLE grammar itself.
	DroppedTables []DroppedTable
}

// Result is an owner's reading of a [Request].
type Result struct {
	// Values holds one value for each claim, in the request's order.
	Values []Value
}

// Parser reads the claims of one document. An owner refuses syntax it cannot
// read with an error that names the offending position in the document.
type Parser interface {
	ParseSQL(ctx context.Context, request Request) (Result, error)
}

// Extension is what one feature owner contributes to the SQL frontend.
type Extension struct {
	// Owner names the owner, as its provider ID does.
	Owner string
	// Targets are the canonical names of the targets whose documents carry
	// the owner's syntax.
	Targets []string
	// Patterns are the syntax the owner adds.
	Patterns []Pattern
	// Parser reads the spans the patterns claim. It is required with
	// Patterns.
	Parser Parser
	// Comments applies resolved comments to objects of Kinds in a file batch.
	Comments CommentEditor
	// Grammar reads the whole of every document of Targets, which are then
	// not written in the shared grammar. It excludes Patterns, and one
	// target takes one grammar.
	Grammar Grammar
	// Header are the kinds a document's `ptah:not-described` header may
	// name that the owner reads as limits on its own claim, rather than as
	// the common coverage vocabulary, such as a YDB topic the document
	// leaves unmanaged.
	Header []coverage.Kind
	// Kinds are the object kinds the owner's syntax declares and amends.
	// A statement's objects must be of these kinds, and a request carries
	// the objects of these kinds that earlier documents declared.
	Kinds []schemaext.Kind
	// Facets are the facet kinds the owner's values carry as the settings of
	// their subject ([Value.Facets]). A value's facets must be of these
	// kinds. A provider may read syntax on a target another provider
	// registers when every pattern stands at a site that takes facets alone
	// and it serves each of these kinds there; see ptah.run/engine.
	Facets []schemaext.Kind
	// Coverage is the knowledge a document of one of Targets holds about the
	// owner's models, such as the refresh schedule a ClickHouse materialized
	// view states in its CREATE statement. It is handed the entries of the
	// document's header whose kind is one of Header, and the objects the
	// document declares. It is optional; without it the owner claims nothing.
	Coverage func(request CoverageRequest) (schemaext.Coverage, error)
}

// CoverageRequest is the document's input to an owner's coverage claim.
type CoverageRequest struct {
	// Limits are the header's ptah:not-described entries of the owner's
	// Header kinds, in their written order.
	Limits []coverage.Object
	// Declared are the objects the document declares after its statements
	// are applied. A grammar that reads several files receives the whole
	// load's declarations for each file's claim. Owners can use these
	// objects to distinguish omission from a default implied by a declaration.
	Declared schemaext.Objects
}

// Runtime supplies the SQL extensions of one explicit provider selection, so
// a caller reads SQL with the owners it renders and compares with.
// *ptah.run/engine.Runtime implements it.
type Runtime interface {
	SQL() Set
}
