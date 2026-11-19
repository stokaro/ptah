package backslashmode_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/backslashmode"
)

var (
	defaultMode = capability.ForDialect("mysql")
	nbe         = defaultMode.With(capability.NoBackslashEscapes, true)
)

// Text written for later loses the session's mode and keeps every other key.
func TestForLater(t *testing.T) {
	c := qt.New(t)
	later := backslashmode.ForLater(nbe)
	c.Assert(later.Has(capability.NoBackslashEscapes), qt.IsFalse)
	c.Assert(later.Equal(defaultMode), qt.IsTrue)
	c.Assert(nbe.Has(capability.NoBackslashEscapes), qt.IsTrue)
	c.Assert(backslashmode.ForLater(defaultMode).Equal(defaultMode), qt.IsTrue)
}

// Statements are read differently only where the modes disagree and a
// statement holds a backslash.
func TestReadDifferently(t *testing.T) {
	backslash := []string{"CREATE TABLE t (a int);", "ALTER TABLE t ALTER COLUMN a SET DEFAULT 'a\\b';"}
	plain := []string{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 'it''s';"}
	tests := []struct {
		name              string
		writtenFor, runOn capability.Capabilities
		statements        []string
		want              bool
	}{
		{name: "default mode text on an NBE session", writtenFor: defaultMode, runOn: nbe, statements: backslash, want: true},
		{name: "NBE text on a default mode session", writtenFor: nbe, runOn: defaultMode, statements: backslash, want: true},
		{name: "modes differ without a backslash", writtenFor: defaultMode, runOn: nbe, statements: plain},
		{name: "both NBE", writtenFor: nbe, runOn: nbe, statements: backslash},
		{name: "both default mode", writtenFor: defaultMode, runOn: defaultMode, statements: backslash},
		{name: "no statements", writtenFor: defaultMode, runOn: nbe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(backslashmode.ReadDifferently(test.writtenFor, test.runOn, test.statements), qt.Equals, test.want)
		})
	}
}

// Describe names the mode as a refusal states it.
func TestDescribe(t *testing.T) {
	c := qt.New(t)
	c.Assert(backslashmode.Describe(nbe), qt.Equals, "names NO_BACKSLASH_ESCAPES")
	c.Assert(backslashmode.Describe(defaultMode), qt.Equals, "does not name NO_BACKSLASH_ESCAPES")
}

// A session reads a backslash as an escape only where its dialect does and
// its sql_mode does not name NO_BACKSLASH_ESCAPES.
func TestEscapes(t *testing.T) {
	tests := []struct {
		name           string
		dialectEscapes bool
		caps           capability.Capabilities
		want           bool
	}{
		{name: "the default sql_mode", dialectEscapes: true, caps: defaultMode, want: true},
		{name: "no capabilities", dialectEscapes: true, want: true},
		{name: "NO_BACKSLASH_ESCAPES", dialectEscapes: true, caps: nbe},
		{name: "a dialect without escapes", caps: defaultMode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(backslashmode.Escapes(test.dialectEscapes, test.caps), qt.Equals, test.want)
		})
	}
}
