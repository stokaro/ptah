package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
)

// A comment node is a planner's note or warning, and every dialect writes it
// the same way: a plain line comment with nothing after the text. A closing
// " --" turned into "--;" once a script writer put a semicolon after the note
// (stokaro/ptah#3903). An empty comment is a separator, written as a bare "--"
// rather than a marker with trailing space.
//
// The rows are every dialect the renderer accepts, so a dialect added later is
// held to the same form.
func TestRenderSQL_CommentNodeIsAPlainLineComment(t *testing.T) {
	for _, dialect := range builtin.SupportedDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			out, err := builtin.RenderSQL(dialect,
				ast.NewComment("NOTE: verify if RLS should be disabled"),
				ast.NewComment(""),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, "-- NOTE: verify if RLS should be disabled\n--\n")
		})
	}
}
