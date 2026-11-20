package bufwriter_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine/builtin/internal/dialects/internal/bufwriter"
)

func TestWriter_CommentLinesStayOneLine(t *testing.T) {
	t.Run("a string argument", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		w.WriteLinef("-- %s: table %s is not supported", "SQLITE", "a\nDROP TABLE victim;\n-- end")
		c.Assert(w.Output(), qt.Equals, "-- SQLITE: table a\\nDROP TABLE victim;\\n-- end is not supported\n")
	})
	t.Run("a carriage return", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		w.WriteLinef("-- %s", "a\r\nDROP TABLE victim;")
		c.Assert(w.Output(), qt.Equals, "-- a\\r\\nDROP TABLE victim;\n")
	})
	t.Run("an error argument", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		w.WriteLinef("-- %s: %v", "POSTGRES", errors.New("bad\nDROP TABLE victim;"))
		c.Assert(w.Output(), qt.Equals, "-- POSTGRES: bad\\nDROP TABLE victim;\n")
	})
}

// TestWriter_CommentLinesCarryNoDirective holds a comment line to text no
// migration file reader takes for a directive. The whole line is neutralized,
// so a directive word in the format is broken as well as one in an argument.
func TestWriter_CommentLinesCarryNoDirective(t *testing.T) {
	tests := []struct {
		name   string
		format string
		args   []any
		want   string
	}{
		{name: "a Ptah directive in an argument", format: "-- %s", args: []any{"+ptah no_transaction"}, want: "-- + ptah no_transaction\n"},
		{name: "an Atlas directive in an argument", format: "-- table %s is skipped", args: []any{"atlas:txmode none"}, want: "-- table atlas :txmode none is skipped\n"},
		{name: "a directive in the format", format: "-- +ptah %s", args: []any{"no_transaction"}, want: "-- + ptah no_transaction\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var w bufwriter.Writer

			w.WriteLinef(test.format, test.args...)

			c.Assert(w.Output(), qt.Equals, test.want)
		})
	}
}

// TestWriter_StatementLinesKeepDirectiveWords is the control: a line that is
// not a comment is SQL and keeps its bytes.
func TestWriter_StatementLinesKeepDirectiveWords(t *testing.T) {
	c := qt.New(t)
	var w bufwriter.Writer

	w.WriteLinef("COMMENT ON TABLE t IS '%s';", "+ptah atlas:txmode none")

	c.Assert(w.Output(), qt.Equals, "COMMENT ON TABLE t IS '+ptah atlas:txmode none';\n")
}

// nilStringer reads a field, so calling String on a nil pointer panics, which
// fmt prints as <nil>.
type nilStringer struct{ text string }

func (s *nilStringer) String() string { return s.text }

type nilError struct{ text string }

func (e *nilError) Error() string { return e.text }

func TestWriter_NilArgumentsPrintAsFmtPrintsThem(t *testing.T) {
	t.Run("a nil Stringer", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		var value *nilStringer
		w.WriteLinef("-- %s", value)
		c.Assert(w.Output(), qt.Equals, "-- <nil>\n")
	})
	t.Run("a nil error", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		var value *nilError
		w.WriteLinef("-- %v", value)
		c.Assert(w.Output(), qt.Equals, "-- <nil>\n")
	})
}

func TestWriter_StatementsKeepTheirLines(t *testing.T) {
	t.Run("a leading comment and a statement written together", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		w.WriteLine("-- note\nDO $$ BEGIN END $$;")
		c.Assert(w.Output(), qt.Equals, "-- note\nDO $$ BEGIN END $$;\n")
	})
	t.Run("a statement line with text arguments", func(t *testing.T) {
		c := qt.New(t)
		var w bufwriter.Writer
		w.WriteLinef("COMMENT ON TABLE %s IS %s;", "t", "'a\nb'")
		c.Assert(w.Output(), qt.Equals, "COMMENT ON TABLE t IS 'a\nb';\n")
	})
}
