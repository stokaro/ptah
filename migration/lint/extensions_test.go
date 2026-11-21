package lint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// extensionMigration installs four extensions in the ways the rules read:
// the default version, a named version through CASCADE, a quoted name, and an
// update to a named version.
const extensionMigration = "CREATE TABLE users (id int);\n" +
	"CREATE EXTENSION IF NOT EXISTS pgcrypto;\n" +
	"CREATE EXTENSION earthdistance WITH SCHEMA public VERSION '1.2' CASCADE;\n" +
	"CREATE EXTENSION \"uuid-ossp\";\n" +
	"ALTER EXTENSION PgCrypto UPDATE TO '1.4';\n"

// analyzeExtensions analyzes one up file on dialect with opts.
func analyzeExtensions(c *qt.C, dialect, migration string, opts lint.Options) lint.Analysis {
	c.Helper()
	opts.Dialect = dialect
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_extensions.up.sql": migration,
	}), opts)
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestExtensionRequests_HappyPath pins what the analysis asks a dev database
// about: each extension statement once, with the name folded as the server
// folds it and the version the statement names, and the version of the file
// that asks.
func TestExtensionRequests_HappyPath(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", extensionMigration+"CREATE EXTENSION pgcrypto;\n", lint.Options{})
	c.Assert(analysis.ExtensionRequests(), qt.DeepEquals, []lint.ExtensionRequest{
		{Extension: "earthdistance", Version: "1.2", Cascade: true},
		{Extension: "pgcrypto"},
		{Extension: "pgcrypto", Version: "1.4"},
		{Extension: "uuid-ossp"},
	})
	c.Assert(analysis.ExtensionVersions(), qt.DeepEquals, []int64{1})
}

// TestExtensionRequests_DecodesEverySpelling pins that a name or a version
// written in any form the server decodes is asked about as the server reads
// it: an escape string, a dollar-quoted string, a Unicode escape identifier
// and a Unicode escape string. A reader that skipped these left the install
// unassessed and the run clean.
func TestExtensionRequests_DecodesEverySpelling(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", "CREATE EXTENSION pgcrypto VERSION E'1.3';\n"+
		"CREATE EXTENSION U&\"cit\\0065xt\" VERSION $$1.5$$;\n"+
		"CREATE EXTENSION \"fuzzystrmatch\" VERSION U&'1\\002e1';\n"+
		"ALTER EXTENSION citext UPDATE TO E'1.6';\n", lint.Options{})
	c.Assert(analysis.ExtensionRequests(), qt.DeepEquals, []lint.ExtensionRequest{
		{Extension: "citext", Version: "1.5"},
		{Extension: "citext", Version: "1.6"},
		{Extension: "fuzzystrmatch", Version: "1.1"},
		{Extension: "pgcrypto", Version: "1.3"},
	})
}

// TestExtensionRequests_FailurePath pins the runs that ask nothing: a dialect
// whose descriptor declares no advisory matching, the compatibility surface,
// both rules disabled, a down file, and statements the recognizer does not
// read as an install.
func TestExtensionRequests_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		opts      lint.Options
	}{
		{name: "dialect without advisory matching", dialect: "mysql", migration: "CREATE TABLE t (id int);\n", opts: lint.Options{}},
		{name: "compatibility surface", dialect: "postgres", migration: extensionMigration, opts: lint.Options{Compatibility: lint.CompatibilityProfileAtlas}},
		{
			name: "both rules disabled", dialect: "postgres", migration: extensionMigration,
			opts: lint.Options{Disabled: []string{"security/vulnerable-extension", "coverage/unassessed-extension"}},
		},
		{
			name: "statements that install nothing", dialect: "postgres",
			migration: "ALTER EXTENSION pgcrypto SET SCHEMA app;\nDROP EXTENSION pgcrypto;\nCOMMENT ON EXTENSION pgcrypto IS 'x';\n" +
				"SELECT 'CREATE EXTENSION pgcrypto';\n",
			opts: lint.Options{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeExtensions(c, test.dialect, test.migration, test.opts)
			c.Assert(analysis.ExtensionRequests(), qt.HasLen, 0)
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "coverage/unassessed-extension")
		})
	}
}

// TestExtensionRequests_DownFileAsksNothing pins that a down file's CREATE
// EXTENSION, which restores what the up file removed, is not a request.
func TestExtensionRequests_DownFileAsksNothing(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_extensions.up.sql":   "DROP EXTENSION pgcrypto;\n",
		"0000000001_extensions.down.sql": "CREATE EXTENSION pgcrypto;\n",
	}), lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(analysis.ExtensionRequests(), qt.HasLen, 0)
}

// extensionFindings keeps the findings of the two extension rules.
func extensionFindings(findings []lint.Finding) []lint.Finding {
	return slices.DeleteFunc(slices.Clone(findings), func(finding lint.Finding) bool {
		return finding.Rule != "security/vulnerable-extension" && finding.Rule != "coverage/unassessed-extension"
	})
}

// extensionAnswers answers every request of extensionMigration on a server
// whose pgcrypto is affected by one advisory, whose CASCADE adds cube, and
// whose advisory data covers neither earthdistance nor cube.
func extensionAnswers() *lint.ExtensionAssessments {
	affected := []lint.ExtensionAdvisory{{
		ID: "CVE-2099-6328", Summary: "an advisory this test invents", FixedIn: "17.9",
		URL: "https://www.postgresql.org/support/security/CVE-2099-6328/",
	}}
	uncovered := []string{"installed extension has no reviewed advisory matcher"}
	return &lint.ExtensionAssessments{
		Server: "PostgreSQL 17.8",
		Answers: []lint.ExtensionAnswer{
			{Request: lint.ExtensionRequest{Extension: "pgcrypto"}, Installs: []lint.InstalledExtension{
				{Extension: "pgcrypto", Version: "1.3", Advisories: affected},
			}},
			{Request: lint.ExtensionRequest{Extension: "earthdistance", Version: "1.2", Cascade: true}, Installs: []lint.InstalledExtension{
				{Extension: "earthdistance", Version: "1.2", Unassessed: uncovered},
				{Extension: "cube", Version: "1.5", Unassessed: uncovered},
			}},
			{Request: lint.ExtensionRequest{Extension: "uuid-ossp"}, Installs: []lint.InstalledExtension{
				{Extension: "uuid-ossp", Version: "1.1"},
			}},
		},
	}
}

// TestExtensionRules_ReportWhatTheDevDatabaseInstalls pins one finding per
// extension installed that the data does not call clean, on the line of the
// statement that installs it: the affected pgcrypto on line 2, earthdistance
// and the cube its CASCADE adds on line 3, and the update on line 5, which the
// answers leave out and so is unassessed. uuid-ossp on line 4 is clean.
func TestExtensionRules_ReportWhatTheDevDatabaseInstalls(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", extensionMigration, lint.Options{Extensions: extensionAnswers()})
	c.Assert(findingSites(extensionFindings(analysis.Findings())), qt.DeepEquals, []string{
		"0000000001_extensions.up.sql:2:security/vulnerable-extension",
		"0000000001_extensions.up.sql:3:coverage/unassessed-extension",
		"0000000001_extensions.up.sql:3:coverage/unassessed-extension",
		"0000000001_extensions.up.sql:5:coverage/unassessed-extension",
	})
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}

// TestExtensionRules_Messages pins what each finding says: the extension, the
// version installed, how the statement installs it, the server assessed, and
// the advisory or the reason it was not assessed.
func TestExtensionRules_Messages(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", extensionMigration, lint.Options{Extensions: extensionAnswers()})
	var messages []string
	for _, finding := range extensionFindings(analysis.Findings()) {
		messages = append(messages, finding.Rule+" "+string(finding.Severity)+": "+finding.Message)
	}
	c.Assert(messages, qt.DeepEquals, []string{
		"security/vulnerable-extension warning: pgcrypto 1.3, which this statement installs, is affected on the dev database's server, " +
			"PostgreSQL 17.8: CVE-2099-6328 (fixed in 17.9): an advisory this test invents, " +
			"https://www.postgresql.org/support/security/CVE-2099-6328/. A server at the fixed release or later is not affected; " +
			"the dev database's server is what this run assessed, so check the release the migration will run on",
		"coverage/unassessed-extension info: earthdistance 1.2, which this statement installs, was not fully assessed against " +
			"published advisories on the dev database's server, PostgreSQL 17.8: installed extension has no reviewed advisory matcher",
		"coverage/unassessed-extension info: cube 1.5, which this statement installs through CASCADE, was not fully assessed against " +
			"published advisories on the dev database's server, PostgreSQL 17.8: installed extension has no reviewed advisory matcher",
		"coverage/unassessed-extension info: the dev database's server, PostgreSQL 17.8, did not say what this statement installs " +
			"for extension pgcrypto, so no advisory was checked for it",
	})
}

// TestExtensionRules_WithoutAnswersNameTheUnmetInput pins a run that read no
// dev database: neither rule reports, and both are named as having run
// without the assessment, once for the file that installs an extension.
func TestExtensionRules_WithoutAnswersNameTheUnmetInput(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", extensionMigration, lint.Options{})
	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "security/vulnerable-extension")
	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "coverage/unassessed-extension")
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule+" "+input.Input.String()+" "+input.File)
	}
	slices.Sort(unmet)
	c.Assert(unmet, qt.DeepEquals, []string{
		"coverage/unassessed-extension extension advisory assessment 0000000001_extensions.up.sql",
		"security/vulnerable-extension extension advisory assessment 0000000001_extensions.up.sql",
	})
}

// TestExtensionRules_FailurePath pins the runs that report nothing although
// the answers are there: the compatibility surface, whose reference has no
// check code for vulnerable extensions, and a disabled rule.
func TestExtensionRules_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		opts lint.Options
		rule string
	}{
		{
			name: "compatibility surface", rule: "security/vulnerable-extension",
			opts: lint.Options{Extensions: extensionAnswers(), Compatibility: lint.CompatibilityProfileAtlas},
		},
		{
			name: "compatibility surface leaves the coverage rule out too", rule: "coverage/unassessed-extension",
			opts: lint.Options{Extensions: extensionAnswers(), Compatibility: lint.CompatibilityProfileAtlas},
		},
		{
			name: "disabled", rule: "security/vulnerable-extension",
			opts: lint.Options{Extensions: extensionAnswers(), Disabled: []string{"security/vulnerable-extension"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeExtensions(c, "postgres", extensionMigration, test.opts)
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), test.rule)
			c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
		})
	}
}
