package sqlutil

import (
	"fmt"
	"regexp"
	"strings"
)

// CommentText returns text made safe to sit inside one SQL comment: every
// character that ends a line, or that a client may read as ending one, is
// written as an escape (`\n`, `\r`, ` `), and so is every other control
// character except the tab. The result holds no line break, so a `--` comment
// that carries it cannot end early and let the rest of the text run as SQL.
// Text without such characters is returned unchanged.
//
// A backslash is not escaped, so a literal `\n` in the text and an escaped line
// break read the same. The result is for a reader of the file and is not meant
// to be decoded.
//
// Declared text reaches comments from many places: a table or column COMMENT, an
// object name, the value of a property a target cannot keep. Each of them is
// user input, and each goes through this function or [LineComment] before it is
// written next to a statement.
func CommentText(text string) string {
	if !strings.ContainsFunc(text, unsafeInComment) {
		return text
	}
	var out strings.Builder
	for _, r := range text {
		switch {
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case unsafeInComment(r) && r < 0x100:
			fmt.Fprintf(&out, `\x%02x`, r)
		case unsafeInComment(r):
			fmt.Fprintf(&out, `\u%04x`, r)
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}

// LineComment returns text as a `-- ` line comment without a trailing newline.
// Empty text gives the bare `--`. Line breaks in text are escaped by
// [CommentText], and a word a directive reader acts on is broken by
// [NeutralizeDirectives], so the result is one line that no reader takes for
// anything but a comment.
func LineComment(text string) string {
	if text == "" {
		return "--"
	}
	return "-- " + NeutralizeDirectives(CommentText(text))
}

// directiveWord matches the words that make a comment line an instruction to a
// reader of the file: the `+ptah` and `+goose` markers, and the `atlas:`,
// `migrate:` and `ptah:` families (`atlas:txmode`, `atlas:delimiter`,
// `atlas:checkpoint`, `atlas:nolint`, `atlas:txtar`, `atlas:import`,
// `migrate:up`, `ptah:schema:...`). The readers disagree on where in a comment
// they look, from the start of the line to anywhere in it, so the match is
// anywhere and ignores case.
var directiveWord = regexp.MustCompile(`(?i)\+ptah|\+goose|(?:atlas|migrate|ptah):`)

// NeutralizeDirectives returns text with every word a directive reader acts on
// broken by one space: `+ptah` becomes `+ ptah`, and `atlas:` becomes `atlas :`.
// A comment Ptah writes from declared text is documentation, so a declared
// comment of `+ptah no_transaction` or `atlas:txmode none` must not be read back
// as the directive it spells. Text without such a word is returned unchanged.
//
// This is the one place that lists those words. The readers are in
// migration/migrationfile, internal/clientdelimiter, migration/lint, the
// checkpoint and txtar readers, and the SQL schema annotation parser.
// migration/migrationfile has a test that drives its readers, the migrator's
// check reader and the delimiter reader with each directive and reads a comment
// made by [LineComment] back as a comment.
func NeutralizeDirectives(text string) string {
	return directiveWord.ReplaceAllStringFunc(text, func(word string) string {
		if word[0] == '+' {
			return "+ " + word[1:]
		}
		return word[:len(word)-1] + " :"
	})
}

// unsafeInComment reports whether r is a control character other than the tab
// or a Unicode line or paragraph separator.
func unsafeInComment(r rune) bool {
	switch {
	case r == '\t':
		return false
	case r < 0x20, r >= 0x7f && r <= 0x9f:
		return true
	}
	return r == ' ' || r == ' '
}
