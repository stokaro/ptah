package lintext

import (
	"context"
	"fmt"
)

// Source is one migration file's executable SQL as a host hands it to a
// splitter.
type Source struct {
	// Name identifies the file in diagnostics. It is not interpreted.
	Name string `json:"name"`
	// SQL is the text to split.
	SQL string `json:"sql"`
}

// Statement is one statement of a source, in the forms rules read. Every
// form is the splitter's, because each depends on the dialect: where a
// statement ends, what is a comment, how a name is quoted.
type Statement struct {
	// Start and End are the statement's byte range in the source's SQL, from
	// its first token to its last, the terminating semicolon excluded unless
	// the dialect makes it part of the statement.
	Start int `json:"start"`
	End   int `json:"end"`
	// Line is the 1-based line of the statement's first token.
	Line int `json:"line"`
	// SQL is the statement text as written, SQL[Start:End] of the source.
	SQL string `json:"sql"`
	// Canonical is the display form: comments removed, every whitespace run
	// collapsed to one space, everything but string literals and quoted
	// identifiers uppercased.
	Canonical string `json:"canonical"`
	// Words is the token sequence rules scan: comments and whitespace
	// dropped, bare words uppercased, punctuation as entries of its own,
	// string literals and quoted identifiers verbatim as single entries.
	Words []string `json:"words"`
	// SourceWords is Words before the uppercasing, as the author spelled
	// each token.
	SourceWords []string `json:"source_words"`
	// Routine is the routine the statement defines, nil when it defines none
	// or the splitter cannot parse one.
	Routine *Routine `json:"routine,omitempty"`
	// Comments are the comments directly above the statement, in order:
	// those after the line the previous statement ends on, with no blank
	// line between them and the statement. A host reads its suppression
	// directives from them.
	Comments []string `json:"comments,omitempty"`
}

// Routine is what a rule may read of the routine a statement defines.
type Routine struct {
	// BodyWords is the body tokenized the way [Statement.Words] is.
	BodyWords []string `json:"body_words"`
}

// SplitRequest asks a target's owner to split a lint run's sources, all of
// them in one call.
type SplitRequest struct {
	// Target is the canonical name of the target the run names.
	Target string `json:"target"`
	// Sources are the run's files, in the order the run reads them.
	Sources []Source `json:"sources"`
}

// SplitResult holds each source's statements, in request order.
type SplitResult struct {
	// Files holds one statement list per source, in source order.
	Files [][]Statement `json:"files"`
}

// Splitter splits migration SQL for one dialect. A host calls it once per
// lint run with every file of the run.
type Splitter interface {
	// SplitStatements returns the statements of every source, or an error
	// and no result. A target the splitter does not serve wraps
	// ptaherr.ErrUnsupportedDialect.
	SplitStatements(ctx context.Context, request SplitRequest) (SplitResult, error)
}

// ValidateSplit reports a result that does not answer request: a statement
// count per source other than one list per source, or a statement whose span
// falls outside its source, overlaps the one before it, disagrees with its
// text, or has no line. Each failure wraps [ErrInvalid].
func ValidateSplit(request SplitRequest, result SplitResult) error {
	if len(result.Files) != len(request.Sources) {
		return fmt.Errorf("%w: a split of %d sources returned %d", ErrInvalid, len(request.Sources), len(result.Files))
	}
	for i, statements := range result.Files {
		source := request.Sources[i]
		end := 0
		for j, statement := range statements {
			if statement.Start < end || statement.End < statement.Start || statement.End > len(source.SQL) ||
				source.SQL[statement.Start:statement.End] != statement.SQL || statement.Line < 1 {
				return fmt.Errorf("%w: statement %d of %s does not match its source", ErrInvalid, j, source.Name)
			}
			end = statement.End
		}
	}
	return nil
}
