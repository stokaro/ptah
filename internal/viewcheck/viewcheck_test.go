package viewcheck_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/viewcheck"
)

// TestParse_HappyPath pins every spelling of the three levels and that the
// value Spell writes reads back as the level it was written from.
func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		value         string
		wantWithCheck bool
		wantLocal     bool
		wantSpelling  string
	}{
		{name: "absent", value: "", wantSpelling: "false"},
		{name: "false", value: "false", wantSpelling: "false"},
		{name: "true is the cascaded level", value: "true", wantWithCheck: true, wantSpelling: "true"},
		{name: "cascaded, in upper case with space", value: " CASCADED ", wantWithCheck: true, wantSpelling: "true"},
		{name: "local", value: "local", wantWithCheck: true, wantLocal: true, wantSpelling: "local"},
		{name: "local in upper case", value: "LOCAL", wantWithCheck: true, wantLocal: true, wantSpelling: "local"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			withCheck, local, err := viewcheck.Parse(test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(withCheck, qt.Equals, test.wantWithCheck)
			c.Assert(local, qt.Equals, test.wantLocal)
			c.Assert(viewcheck.Spell(withCheck, local), qt.Equals, test.wantSpelling)
		})
	}
}

// TestParse_FailurePath pins that a value naming no level is refused rather
// than read as no check option.
func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "a misspelled level", value: "locale", wantErr: `invalid view check option "locale": want true, false, cascaded or local`},
		{name: "a YAML 1.1 boolean", value: "yes", wantErr: `invalid view check option "yes": want true, false, cascaded or local`},
		{name: "none", value: "none", wantErr: `invalid view check option "none": want true, false, cascaded or local`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			withCheck, local, err := viewcheck.Parse(test.value)

			c.Assert(err, qt.ErrorIs, viewcheck.ErrInvalidLevel)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(withCheck, qt.IsFalse)
			c.Assert(local, qt.IsFalse)
		})
	}
}

// TestSpell_LocalWithoutACheckOption pins that LOCAL without a check option
// is written as no check option, as the model reads it.
func TestSpell_LocalWithoutACheckOption(t *testing.T) {
	c := qt.New(t)

	c.Assert(viewcheck.Spell(false, true), qt.Equals, "false")
}
