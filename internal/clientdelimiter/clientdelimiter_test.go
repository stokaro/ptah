package clientdelimiter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clientdelimiter"
)

func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		line          string
		wantDelimiter string
		wantForm      clientdelimiter.Form
	}{
		{name: "client form", line: "DELIMITER $$", wantDelimiter: "$$", wantForm: clientdelimiter.Client},
		{name: "client form in lower case", line: "delimiter //", wantDelimiter: "//", wantForm: clientdelimiter.Client},
		{name: "client form with surrounding whitespace", line: "  DELIMITER ;;\n", wantDelimiter: ";;", wantForm: clientdelimiter.Client},
		{name: "atlas form", line: "-- atlas:delimiter $$", wantDelimiter: "$$", wantForm: clientdelimiter.Atlas},
		{name: "atlas form in upper case", line: "-- ATLAS:DELIMITER //", wantDelimiter: "//", wantForm: clientdelimiter.Atlas},
		{name: "quoted delimiter", line: `DELIMITER "$$"`, wantDelimiter: "$$", wantForm: clientdelimiter.Client},
		{name: "escaped newlines", line: `-- atlas:delimiter \n\n`, wantDelimiter: "\n\n", wantForm: clientdelimiter.Atlas},
		{name: "restoring the semicolon", line: "DELIMITER ;", wantDelimiter: ";", wantForm: clientdelimiter.Client},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			delimiter, form, ok := clientdelimiter.Parse(test.line)
			c.Assert(ok, qt.IsTrue)
			c.Assert(delimiter, qt.Equals, test.wantDelimiter)
			c.Assert(form, qt.Equals, test.wantForm)
		})
	}
}

// A line that only resembles a directive selects nothing, so the splitter
// keeps it as SQL and the YQL refusal does not fire on it.
func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{name: "keyword alone", line: "DELIMITER"},
		{name: "keyword with blank delimiter", line: "DELIMITER   "},
		{name: "keyword joined to a word", line: "DELIMITERS $$"},
		{name: "atlas keyword joined to a word", line: "-- atlas:delimiters $$"},
		{name: "another atlas directive", line: "-- atlas:txmode none"},
		{name: "statement that mentions the word", line: "SELECT 'DELIMITER $$'"},
		{name: "empty", line: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			delimiter, form, ok := clientdelimiter.Parse(test.line)
			c.Assert(ok, qt.IsFalse)
			c.Assert(delimiter, qt.Equals, "")
			c.Assert(form, qt.Equals, clientdelimiter.Form(0))
		})
	}
}
