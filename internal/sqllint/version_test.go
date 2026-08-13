package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/sqllint"
)

// TestResolveTargetVersion covers the four outcomes an operator-supplied
// version can have, because they are four different things to tell the person
// who typed it and only one of them is silence.
//
// Row three is the defect this exists for: before Recognized was published,
// "not-a-version" resolved to the dialect default and the linter reported it
// as the version it had applied.
func TestResolveTargetVersion(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		assert  func(c *qt.C, target sqllint.TargetVersion, err error)
	}{
		{
			name:    "measured release line resolves silently",
			dialect: platform.Postgres,
			version: "PostgreSQL 16.3 (Debian)",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(target.Note, qt.Equals, "")
				c.Assert(target.Capabilities, qt.DeepEquals, capability.Postgres16())
			},
		},
		{
			name:    "no version resolves to the dialect default, silently",
			dialect: platform.Postgres,
			version: "",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(target.Note, qt.Equals, "")
				c.Assert(target.Capabilities, qt.DeepEquals, capability.ForDialect(platform.Postgres))
			},
		},
		{
			name:    "a string that names no server is refused",
			dialect: platform.Postgres,
			version: "not-a-version",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(target.Capabilities, qt.IsNil)
				var unrecognized *sqllint.UnrecognizedVersionError
				c.Assert(err, qt.ErrorAs, &unrecognized)
				c.Assert(unrecognized.Version, qt.Equals, "not-a-version")
				c.Assert(unrecognized.Dialect, qt.Equals, platform.Postgres)
				c.Assert(err.Error(), qt.Contains, "not-a-version")
				c.Assert(err.Error(), qt.Contains, "10.11.6-MariaDB")
			},
		},
		{
			name:    "a version above the ladder succeeds and names the line it planned as",
			dialect: platform.Postgres,
			version: "99.0",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(target.Capabilities, qt.DeepEquals, capability.Postgres17())
				c.Assert(target.Note, qt.Contains, "newer than the newest measured release line 18.x")
				c.Assert(target.Note, qt.Contains, "planned as 18.x")
			},
		},
		{
			name:    "a version between measured lines succeeds and says so",
			dialect: platform.MySQL,
			version: "8.0.42-log",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(target.Capabilities, qt.DeepEquals, capability.MySQL8019())
				c.Assert(target.Note, qt.Contains, "is not a measured release line")
				c.Assert(target.Note, qt.Contains, "newest measured line: 26.7")
			},
		},
		{
			name:    "a good version for a dialect with no ladder says it changed nothing",
			dialect: platform.SQLite,
			version: "3.53.0",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(target.Capabilities, qt.DeepEquals, capability.SQLite3())
				c.Assert(target.Note, qt.Contains, "no measured version ladder")
			},
		},
		{
			name:    "a MariaDB banner carrying no version is refused",
			dialect: platform.MariaDB,
			version: "MariaDB something",
			assert: func(c *qt.C, target sqllint.TargetVersion, err error) {
				c.Assert(err, qt.IsNotNil)
				var unrecognized *sqllint.UnrecognizedVersionError
				c.Assert(err, qt.ErrorAs, &unrecognized)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := sqllint.ResolveTargetVersion(tt.dialect, tt.version)

			tt.assert(c, target, err)
		})
	}
}

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
	var unrecognized *sqllint.UnrecognizedVersionError
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
