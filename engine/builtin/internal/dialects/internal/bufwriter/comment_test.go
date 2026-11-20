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
