package lint

// White-box testing required: verifies unexported report formatting and failure-threshold helpers that are not exposed through the command API.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	migrationlint "github.com/stokaro/ptah/migration/lint"
)

func TestWriteGitHubActions_EscapesWorkflowCommandCharacters(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	writeGitHubActions(&buf, lintReport{
		Findings: []migrationlint.Finding{{
			Rule:     "DS101",
			Severity: migrationlint.SeverityError,
			File:     "dir/evil,file::name.sql",
			Line:     3,
			Message:  "50% data loss\r\nsecond line",
		}},
	})

	out := buf.String()
	c.Assert(out, qt.Contains, "::error file=dir/evil%2Cfile%3A%3Aname.sql,line=3::")
	c.Assert(out, qt.Contains, "DS101: 50%25 data loss%0D%0Asecond line")
	c.Assert(out, qt.Not(qt.Contains), "evil,file::name")

	buf.Reset()
	writeGitHubActions(&buf, lintReport{Error: "bad\nnews: 100%"})
	c.Assert(buf.String(), qt.Equals, "::error::bad%0Anews: 100%25\n")
}

func TestShouldFail(t *testing.T) {
	c := qt.New(t)

	warning := []migrationlint.Finding{{Severity: migrationlint.SeverityWarning}}
	fatal := []migrationlint.Finding{{Severity: migrationlint.SeverityError}}

	c.Assert(shouldFail(nil, failOnError), qt.IsFalse)
	c.Assert(shouldFail(warning, failOnError), qt.IsFalse)
	c.Assert(shouldFail(fatal, failOnError), qt.IsTrue)
	c.Assert(shouldFail(warning, failOnAny), qt.IsTrue)
	c.Assert(shouldFail(fatal, failOnNone), qt.IsFalse)
}
