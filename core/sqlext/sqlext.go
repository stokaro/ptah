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
	"ptah.run/core/schemaext"
)

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner passes [None] instead, so a forgotten selection
// is refused rather than read as a decision to leave an owner's syntax
// unread.
var ErrUnselected = errors.New("no SQL schema owners were selected")

// Site is the place in a shared statement where an owner's syntax may stand.
// Each site says where a span that opens there ends at the latest; a
// pattern's Stops may end it earlier.
type Site string

const (
	// TableElement is an element of a CREATE TABLE column list, where a
	// column or a table constraint stands. A span ends at the next top-level
	// comma or at the parenthesis that closes the list. Its [Value] declares
	// an index of the table.
	TableElement Site = "table-element"
	// AlterOperation is one operation of an ALTER TABLE statement. A span
	// ends at the next top-level comma or at the end of the statement. Its
	// [Value] is the owner's operation, and may declare an index of the
	// table too.
	AlterOperation Site = "alter-operation"
	// ViewClause is a clause of a CREATE MATERIALIZED VIEW statement between
	// the view's name and AS. A span ends at a top-level AS or at the end of
	// the statement. Its [Value] carries the owner's facets of the view.
	ViewClause Site = "view-clause"
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
	// Next, when it is not empty, lists the words one of which must follow
	// Words.
	Next []string
	// NotNext lists the words that must not follow Words.
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
	// writes it: the table of a table element or an ALTER TABLE operation,
	// the view of a view clause.
	Subject string
	// Text is the span's source text, its opening words included.
	Text string
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

// Value is the owner's reading of one claim.
type Value struct {
	// Index is the index a table element or an ALTER TABLE operation
	// declares. A table element requires one.
	Index *Index
	// Operation is the owner's payload of an ALTER TABLE operation, which an
	// alter operation requires.
	Operation ast.ExtensionPayload
	// Facets are the owner's settings of the subject, such as a materialized
	// view's refresh schedule.
	Facets schemaext.Facets
}

// Request is every claim of one document an extension's patterns matched.
type Request struct {
	// Target is the target the document names, or empty for a document that
	// names none.
	Target string
	// Claims are the claims, in the order the document holds them.
	Claims []Claim
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
	// Coverage is the knowledge a document of one of Targets holds about the
	// owner's models, such as the refresh schedule a ClickHouse materialized
	// view states in its CREATE statement. It is optional; without it the
	// owner claims nothing.
	Coverage func() (schemaext.Coverage, error)
}

// Runtime supplies the SQL extensions of one explicit provider selection, so
// a caller reads SQL with the owners it renders and compares with.
// *ptah.run/engine.Runtime implements it.
type Runtime interface {
	SQL() Set
}
