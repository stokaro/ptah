package sqlutil

import (
	"fmt"
	"strings"
)

// CommentText returns text made safe to sit inside one SQL comment: every
// character that ends a line, or that a client may read as ending one, is
// written as an escape (`\n`, `\r`, ` `), and so is every other control
// character except the tab. The result holds no line break, so a `--` comment
// that carries it cannot end early and let the rest of the text run as SQL.
// Text without such characters is returned unchanged.
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
// [CommentText], so the result is always one line.
func LineComment(text string) string {
	if text == "" {
		return "--"
	}
	return "-- " + CommentText(text)
}

// BlockCommentText returns text made safe to sit inside a `/* */` comment: it is
// escaped by [CommentText], and neither the closing `*/` nor the opening `/*` is
// left in it (PostgreSQL nests block comments, so an opener the text carries
// would swallow the real closer). Each of the two is written with a space
// inside, `* /` and `/ *`.
func BlockCommentText(text string) string {
	text = CommentText(text)
	text = strings.ReplaceAll(text, "*/", "* /")
	return strings.ReplaceAll(text, "/*", "/ *")
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
