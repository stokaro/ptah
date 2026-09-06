package renovateregex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/renovateregex"
)

// TestCheck_RefusesABackreference is stokaro/ptah#2339, which is the whole
// reason this gate exists.
//
// `\k<depName>` is valid in JavaScript and absent from RE2, so Renovate refused
// the configuration and stopped opening pull requests. `renovate-config-validator`
// reported success for the same file, because it falls back to JavaScript's
// engine when the `re2` native module is not built.
func TestCheck_RefusesABackreference(t *testing.T) {
	c := qt.New(t)

	result := renovateregex.Check(renovateregex.Config{CustomManagers: []renovateregex.Manager{{
		MatchStrings: []string{`(?<depName>[a-z]+)\s+\k<depName>`},
	}}})

	c.Assert(result.OK(), qt.IsFalse)
	c.Assert(result.Findings, qt.HasLen, 1)
	c.Assert(result.Findings[0], qt.Contains, "does not compile under RE2")
	// The pattern is quoted back, because the finding has to say WHICH one.
	c.Assert(result.Findings[0], qt.Contains, `\k<depName>`)
}

// TestCheck_AcceptsRenovatesNamedGroupSpelling is the control the backreference
// row needs.
//
// `(?<name>` and `(?P<name>` are the same construct, and a check that could not
// read the first would report every one of this repository's patterns as
// broken -- passing the test above for the wrong reason.
func TestCheck_AcceptsRenovatesNamedGroupSpelling(t *testing.T) {
	c := qt.New(t)

	result := renovateregex.Check(renovateregex.Config{CustomManagers: []renovateregex.Manager{{
		MatchStrings: []string{`(?<depName>[a-z/]+):(?<currentValue>[0-9.]+)`},
	}}})

	c.Assert(result.OK(), qt.IsTrue)
	c.Assert(result.Checked, qt.Equals, 1)
}

// TestCheck_RefusesAVacuousRun keeps a configuration whose managers were
// renamed out from under the gate from reporting the same success as one that
// compiled everything.
func TestCheck_RefusesAVacuousRun(t *testing.T) {
	tests := []struct {
		name    string
		config  renovateregex.Config
		wantHas string
	}{
		{
			name:    "no custom managers at all",
			config:  renovateregex.Config{},
			wantHas: "declares no customManagers",
		},
		{
			name:    "a manager with no patterns",
			config:  renovateregex.Config{CustomManagers: []renovateregex.Manager{{Description: "empty"}}},
			wantHas: "declares no matchStrings",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := renovateregex.Check(test.config)
			c.Assert(result.OK(), qt.IsFalse)
			c.Assert(result.Findings, qt.HasLen, 1)
			c.Assert(result.Findings[0], qt.Contains, test.wantHas)
		})
	}
}

// TestCheck_CountsEveryPatternAcrossManagers is what the reported number means.
// A count that stopped at the first manager would read as coverage it does not
// have.
func TestCheck_CountsEveryPatternAcrossManagers(t *testing.T) {
	c := qt.New(t)

	result := renovateregex.Check(renovateregex.Config{CustomManagers: []renovateregex.Manager{
		{MatchStrings: []string{`a(?<x>[0-9]+)`, `b(?<y>[0-9]+)`}},
		{MatchStrings: []string{`c(?<z>[0-9]+)`}},
	}})

	c.Assert(result.OK(), qt.IsTrue)
	c.Assert(result.Checked, qt.Equals, 3)
}

// TestParse_RefusesAMalformedDocument keeps a broken file from reading as a
// configuration with no managers, which the vacuity rule would then report as
// its own kind of failure and send the reader to the wrong place.
func TestParse_RefusesAMalformedDocument(t *testing.T) {
	c := qt.New(t)

	_, err := renovateregex.Parse([]byte("{not json"))

	c.Assert(err, qt.IsNotNil)
}

// TestRE2Spelling_RewritesOnlyTheGroupOpener is the narrow claim the rewrite
// makes: it must not touch a `<` that is part of a pattern.
func TestRE2Spelling_RewritesOnlyTheGroupOpener(t *testing.T) {
	c := qt.New(t)

	c.Assert(renovateregex.RE2Spelling(`(?<name>x)`), qt.Equals, `(?P<name>x)`)
	c.Assert(renovateregex.RE2Spelling(`a<b`), qt.Equals, `a<b`)
	c.Assert(renovateregex.RE2Spelling(`(?P<already>x)`), qt.Equals, `(?P<already>x)`)
}
