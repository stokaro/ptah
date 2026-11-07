// Package sourcepos turns a byte offset in a document into the line and column
// its author reads.
//
// A parser knows where it stopped as an offset into the text it holds. It states
// that with [ptah.run/core/ptaherr.At] and writes no position in its sentence.
// The one place that holds the document, the parser's own entry point or the
// lint run that split a file into statements, asks this package for the line and
// column, so every reader of SQL counts them alike.
package sourcepos

import (
	"errors"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ptaherr"
)

// Locate returns the 1-based line and column of offset in text.
//
// A line ends at LF, at CRLF or at a lone CR, the three endings an editor opens
// a file with. Column counts characters: a multi-byte character is one column
// and a tab is one, and a byte that is not valid UTF-8 is one. A display cell
// would depend on the reader's tab stops and font, and a byte would differ from
// the editor's cursor on every line holding a multi-byte character.
//
// An offset before the text is its start and one past it is its end, which after
// a final line ending is column 1 of the line after it. The newlines of a string
// or comment count like any other: lines belong to the text, not to its tokens.
func Locate(text string, offset int) (line, column int) {
	offset = max(0, min(offset, len(text)))
	line = 1
	lineStart := 0
	for i := range offset {
		if ch := text[i]; ch != '\n' && (ch != '\r' || strings.HasPrefix(text[i+1:], "\n")) {
			continue
		}
		line++
		lineStart = i + 1
	}
	return line, utf8.RuneCountInString(text[lineStart:offset]) + 1
}

// Shift moves the offsets err's chain states by base, and returns err. A parser
// that reads a piece of a document, as a statement read again by itself, calls it
// with the piece's offset in the document, so the offset of a refusal stays one
// the document's reader can locate.
//
// It writes to the errors in the chain, which the piece's parser has just made
// and nothing else holds.
func Shift(err error, base int) error {
	for rest := err; ; {
		at, ok := errors.AsType[*ptaherr.ParseError](rest)
		if !ok {
			return err
		}
		at.Offset += base
		rest = at.Err
	}
}

// Resolve returns err at the line and column of the offset its chain states, or
// err itself when the chain states none. origin maps an offset in the text the
// parser read to one in text, the document as written.
//
// A chain can state two offsets, a statement's and then a clause's inside it.
// The innermost is the more exact one. The result's text is err's, and err stays
// its cause, so errors.Is and errors.As see what they saw before.
func Resolve(err error, text string, origin func(int) int) error {
	offset, found := innermost(err)
	if !found {
		return err
	}
	offset = origin(offset)
	line, column := Locate(text, offset)
	return &ptaherr.ParseError{Line: line, Column: column, Offset: offset, Err: err}
}

func innermost(err error) (offset int, found bool) {
	for {
		at, ok := errors.AsType[*ptaherr.ParseError](err)
		if !ok {
			return offset, found
		}
		offset, found = at.Offset, true
		err = at.Err
	}
}
