package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/servertarget"
	"go.5x5.cz/ptah/internal/sqllint"
)

// TestLintSource_RefusesAVersionThatNamesNoServer keeps the refusal inside the
// library rather than only at the command's flag-validation seam, so a caller
// that reaches LintSource directly cannot receive findings attributed to a
// version that never resolved.
func TestLintSource_RefusesAVersionThatNamesNoServer(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(sqllint.Source{
		Name: "index.sql",
		SQL:  "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);",
	}, sqllint.Options{Dialect: platform.Postgres, Version: "not-a-version"})

	c.Assert(findings, qt.IsNil)
	var unrecognized *servertarget.UnrecognizedVersionError
	c.Assert(err, qt.ErrorAs, &unrecognized)
}

// TestLintSource_ExplicitCapabilitiesOutrankTheVersionString is the control
// for the test above: Options.Capabilities is the pre-resolved set the command
// passes down after resolving once, so a caller that supplies it has already
// answered the question and must not be refused a second time.
func TestLintSource_ExplicitCapabilitiesOutrankTheVersionString(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(sqllint.Source{
		Name: "index.sql",
		SQL:  "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);",
	}, sqllint.Options{
		Dialect:      platform.Postgres,
		Version:      "not-a-version",
		Capabilities: capability.Postgres16(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}
