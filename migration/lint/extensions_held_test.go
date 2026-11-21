package lint_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// heldAnswers answers the statements of the held-state tests on a server where
// pgcrypto and the cube CASCADE adds for earthdistance are each affected by an
// advisory, and the data covers neither earthdistance, cube 1.4 nor citext.
func heldAnswers(held map[int64]map[string]string) *lint.ExtensionAssessments {
	advisory := []lint.ExtensionAdvisory{{
		ID: "CVE-2099-6328", Summary: "an advisory this test invents", FixedIn: "17.9",
		URL: "https://www.postgresql.org/support/security/CVE-2099-6328/",
	}}
	uncovered := []string{"installed extension has no reviewed advisory matcher"}
	return &lint.ExtensionAssessments{
		Server: "PostgreSQL 17.8",
		Held:   held,
		Answers: []lint.ExtensionAnswer{
			{Request: lint.ExtensionRequest{Extension: "pgcrypto"}, Installs: []lint.InstalledExtension{
				{Extension: "pgcrypto", Version: "1.3", Advisories: advisory},
			}},
			{Request: lint.ExtensionRequest{Extension: "earthdistance", Cascade: true}, Installs: []lint.InstalledExtension{
				{Extension: "earthdistance", Version: "1.2", Unassessed: uncovered},
				{Extension: "cube", Version: "1.5", Advisories: advisory},
			}},
			{Request: lint.ExtensionRequest{Extension: "cube", Version: "1.4"}, Installs: []lint.InstalledExtension{
				{Extension: "cube", Version: "1.4", Unassessed: uncovered},
			}},
			{Request: lint.ExtensionRequest{Extension: "citext"}, Installs: []lint.InstalledExtension{
				{Extension: "citext", Version: "1.6", Unassessed: uncovered},
			}},
		},
	}
}

// TestExtensionRules_JudgeWhatTheDatabaseHolds pins that a statement is
// judged against what the dev database held when its version started and what
// the statements before it in the file changed: CASCADE reports only what it
// adds, CREATE EXTENSION IF NOT EXISTS of a held extension and an update to
// the held version report nothing, and a drop or a DO block returns the walk
// to installing what a later statement names.
func TestExtensionRules_JudgeWhatTheDatabaseHolds(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		held      map[int64]map[string]string
		want      []string
	}{
		{
			name: "CASCADE reports what it adds, its advisory included", migration: "CREATE EXTENSION earthdistance CASCADE;\n",
			want: []string{"1:coverage/unassessed-extension", "1:security/vulnerable-extension"},
		},
		{
			name: "CASCADE leaves out what an earlier statement installed", migration: "CREATE EXTENSION cube VERSION '1.4';\nCREATE EXTENSION earthdistance CASCADE;\n",
			want: []string{"1:coverage/unassessed-extension", "2:coverage/unassessed-extension"},
		},
		{
			name: "CASCADE leaves out what the database held", migration: "CREATE EXTENSION earthdistance CASCADE;\n",
			held: map[int64]map[string]string{1: {"cube": "1.5"}},
			want: []string{"1:coverage/unassessed-extension"},
		},
		{
			name: "IF NOT EXISTS of a held extension installs nothing", migration: "CREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			held: map[int64]map[string]string{1: {"pgcrypto": "1.3"}},
			want: make([]string, 0),
		},
		{
			name: "IF NOT EXISTS after an earlier create installs nothing", migration: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			want: []string{"1:security/vulnerable-extension"},
		},
		{
			name:      "a drop returns the extension to the walk",
			migration: "CREATE EXTENSION pgcrypto;\nDROP EXTENSION pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			want:      []string{"1:security/vulnerable-extension", "3:security/vulnerable-extension"},
		},
		{
			name: "a cascading drop clears the walk", migration: "DROP EXTENSION IF EXISTS cube CASCADE;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			held: map[int64]map[string]string{1: {"cube": "1.5", "pgcrypto": "1.3"}},
			want: []string{"2:security/vulnerable-extension"},
		},
		{
			name: "a drop lint cannot read clears the walk", migration: "DROP EXTENSION cube CASCADE RESTRICT;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			held: map[int64]map[string]string{1: {"pgcrypto": "1.3"}},
			want: []string{"2:security/vulnerable-extension"},
		},
		{
			name: "an unanswered create is held", migration: "CREATE EXTENSION hstore;\nCREATE EXTENSION IF NOT EXISTS hstore;\n",
			want: []string{"1:coverage/unassessed-extension"},
		},
		{
			name: "a DO block clears the walk", migration: "DO $$ BEGIN DROP EXTENSION pgcrypto; END $$;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			held: map[int64]map[string]string{1: {"pgcrypto": "1.3"}},
			want: []string{"2:security/vulnerable-extension"},
		},
		{
			name: "an update to the held version installs nothing", migration: "ALTER EXTENSION citext UPDATE;\n",
			held: map[int64]map[string]string{1: {"citext": "1.6"}},
			want: make([]string, 0),
		},
		{
			name: "an update from another version installs", migration: "ALTER EXTENSION citext UPDATE;\n",
			held: map[int64]map[string]string{1: {"citext": "1.5"}},
			want: []string{"1:coverage/unassessed-extension"},
		},
		{
			name: "a version the run did not read holds nothing", migration: "CREATE EXTENSION IF NOT EXISTS pgcrypto;\n",
			held: map[int64]map[string]string{2: {"pgcrypto": "1.3"}},
			want: []string{"1:security/vulnerable-extension"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeExtensions(c, "postgres", test.migration, lint.Options{Extensions: heldAnswers(test.held)})
			c.Assert(lineSites(extensionFindings(analysis.Findings())), qt.DeepEquals, test.want)
		})
	}
}

// lineSites returns each finding as line:rule.
func lineSites(findings []lint.Finding) []string {
	sites := make([]string, 0, len(findings))
	for _, site := range findingSites(findings) {
		sites = append(sites, site[len("0000000001_extensions.up.sql:"):])
	}
	return sites
}

// TestExtensionRules_ACascadedInstallCarriesItsAdvisory pins the message of an
// advisory that affects an extension CASCADE installs: it names the extension,
// how the statement installs it and the advisory.
func TestExtensionRules_ACascadedInstallCarriesItsAdvisory(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", "CREATE EXTENSION earthdistance CASCADE;\n", lint.Options{Extensions: heldAnswers(nil)})
	var messages []string
	for _, finding := range analysis.Findings() {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(messages, qt.Contains, "security/vulnerable-extension: cube 1.5, which this statement installs through CASCADE, "+
		"is affected on the dev database's server, PostgreSQL 17.8: CVE-2099-6328 (fixed in 17.9): an advisory this test invents, "+
		"https://www.postgresql.org/support/security/CVE-2099-6328/. A server at the fixed release or later is not affected; "+
		"the dev database's server is what this run assessed, so check the release the migration will run on")
}

// unreadMigration holds extension statements the reader recognizes and
// cannot read: a version written as a number, an update with an option it
// does not take, and the FROM option.
const unreadMigration = "CREATE EXTENSION pgcrypto VERSION 1.3;\n" +
	"ALTER EXTENSION citext UPDATE TO '1.6' CASCADE;\n" +
	"CREATE EXTENSION pgcrypto FROM unpackaged;\n"

// TestExtensionRules_AnUnreadStatementIsUnassessed pins that an extension
// statement the reader cannot read is reported as unassessed, with the
// reason, and never passed over: a statement passed over reads as clean.
func TestExtensionRules_AnUnreadStatementIsUnassessed(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeExtensions(c, "postgres", unreadMigration, lint.Options{Extensions: &lint.ExtensionAssessments{Server: "PostgreSQL 17.8"}})
	var messages []string
	for _, finding := range extensionFindings(analysis.Findings()) {
		messages = append(messages, lineSites([]lint.Finding{finding})[0]+": "+finding.Message)
	}
	unread := "this statement creates or updates an extension, but %s, so no advisory was checked for what it installs"
	c.Assert(messages, qt.DeepEquals, []string{
		"1:coverage/unassessed-extension: " + fmt.Sprintf(unread, "its version 1.3 is neither a name nor a string"),
		"2:coverage/unassessed-extension: " + fmt.Sprintf(unread, `"CASCADE" follows the version, and ALTER EXTENSION ... UPDATE takes nothing there`),
		"3:coverage/unassessed-extension: " + fmt.Sprintf(unread, `"FROM" is not an option CREATE EXTENSION takes`),
	})
	c.Assert(analysis.ExtensionRequests(), qt.HasLen, 0)
	c.Assert(analysis.ExtensionVersions(), qt.DeepEquals, []int64{1}, qt.Commentf("the caller still reads the server for the findings"))
}

// TestExtensionRules_AnUnreadStatementNamesTheUnmetInput pins that a run with
// no dev database names both rules for a file whose only extension statement
// the reader cannot read: the statement still needed the assessment.
func TestExtensionRules_AnUnreadStatementNamesTheUnmetInput(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_extensions.up.sql": "CREATE EXTENSION pgcrypto VERSION 1.3;\n",
	}), lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule)
	}
	c.Assert(unmet, qt.ContentEquals, []string{"coverage/unassessed-extension", "security/vulnerable-extension"})
}
