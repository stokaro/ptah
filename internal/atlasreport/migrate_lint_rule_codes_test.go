package atlasreport_test

import (
	"bytes"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascodes"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/builtintest"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// This file pins how every ptah-compat lint report names a rule. A rule that
// reports an Atlas check is printed under that check's code, in the line shape
// of the community binary: the code is the fragment of the analyzer link after
// the message, and the same code is the JSON Code. A rule Atlas has no check
// for keeps its Ptah name, in brackets after the line number and as the JSON
// Code, because there is no Atlas code to print and no analyzer page to link.
//
// The expected lines and documents are the ones Atlas CE v1.4.0 prints for the
// same migration on SQLite, measured with
// `migrate lint --dir file://m --dev-url 'sqlite://file?mode=memory' --latest 1`.

// compatLintAnalysis analyzes one migration the way `ptah-compat migrate lint`
// does, with every rule the default policy runs.
func compatLintAnalysis(c *qt.C, dialect, sql string) migrationlint.Analysis {
	c.Helper()
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
		"1_change.sql": {Data: []byte(sql)},
	}, migrationlint.Options{
		Compatibility:  migrationlint.CompatibilityProfileAtlas,
		AtlasSelectors: atlascodes.SuppressedByOrName,
		DirFormat:      migrationfile.DirFormatAtlas,
		Dialect:        dialect,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

func compatLintText(c *qt.C, analysis migrationlint.Analysis) string {
	c.Helper()
	var out bytes.Buffer
	c.Assert(atlasreport.WriteMigrateLintText(&out, atlasreport.MigrateLintOptions{Analysis: &analysis}), qt.IsNil)
	return out.String()
}

func compatLintFormat(c *qt.C, analysis migrationlint.Analysis, format string) string {
	c.Helper()
	var out bytes.Buffer
	c.Assert(atlasreport.WriteMigrateLintFormat(&out, format, atlasreport.MigrateLintOptions{Analysis: &analysis}), qt.IsNil)
	return out.String()
}

func TestWriteMigrateLintText_PrintsAnAtlasCheckAsTheCommunityBinaryDoes(t *testing.T) {
	c := qt.New(t)
	analysis := compatLintAnalysis(c, "sqlite", "DROP TABLE users;\n")

	text := compatLintText(c, analysis)

	c.Assert(analysis.Findings()[0].Rule, qt.Equals, "destructive/drop-table")
	c.Assert(text, qt.Contains, "    -- destructive changes detected:\n"+
		"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n"+
		"    -- suggested fix:\n"+
		"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n")
	c.Assert(text, qt.Not(qt.Contains), "destructive/drop-table")
}

// TestWriteMigrateLintText_WrapsTheAnalyzerLinkWithTheMessage holds the link
// to the message width: the community binary wraps the MF103 sentence and
// puts the link on the continuation line.
func TestWriteMigrateLintText_WrapsTheAnalyzerLinkWithTheMessage(t *testing.T) {
	c := qt.New(t)
	analysis := compatLintAnalysis(c, "sqlite", "ALTER TABLE users ADD COLUMN c int NOT NULL;\n")

	text := compatLintText(c, analysis)

	c.Assert(text, qt.Contains, "    -- data dependent changes detected:\n"+
		"      -- L1: Adding a non-nullable \"int\" column \"c\" will fail in case table \"users\" is not empty\n"+
		"         https://atlasgo.io/lint/analyzers#MF103\n")
	c.Assert(text, qt.Not(qt.Contains), "data-dependent/add-not-null-column")
}

// TestWriteMigrateLintText_LinksAnAtlasCheckPtahWordsItsOwnWay covers a rule
// that reports an Atlas check whose sentence Ptah has not measured: the
// message is Ptah's, and the line still carries the check the community
// binary's shape carries, so a reader of that shape finds the code where it
// looks for one.
func TestWriteMigrateLintText_LinksAnAtlasCheckPtahWordsItsOwnWay(t *testing.T) {
	c := qt.New(t)
	analysis := compatLintAnalysis(c, "sqlite", "CREATE UNIQUE INDEX u ON users (a);\n")

	text := compatLintText(c, analysis)

	c.Assert(analysis.Findings()[0].Rule, qt.Equals, "data-dependent/add-unique-index")
	c.Assert(text, qt.Contains, "      -- L1: CREATE UNIQUE INDEX u builds over the rows users already holds")
	c.Assert(text, qt.Contains, " https://atlasgo.io/lint/analyzers#MF101\n")
	c.Assert(text, qt.Not(qt.Contains), "[MF101]")
	c.Assert(text, qt.Not(qt.Contains), "data-dependent/add-unique-index")
}

// TestWriteMigrateLintText_PrintsAPtahOnlyRuleByItsName covers a rule with
// no Atlas check: TRUNCATE has none. The line names the rule and links to no
// analyzer.
func TestWriteMigrateLintText_PrintsAPtahOnlyRuleByItsName(t *testing.T) {
	c := qt.New(t)
	analysis := compatLintAnalysis(c, "postgres", "TRUNCATE users;\n")

	text := compatLintText(c, analysis)

	c.Assert(text, qt.Contains, "      -- L1 [destructive/truncate-table]: TRUNCATE deletes all rows from the table")
	c.Assert(text, qt.Not(qt.Contains), "analyzers#")
}

func TestWriteMigrateLintFormat_CarriesTheAtlasCode(t *testing.T) {
	c := qt.New(t)
	analysis := compatLintAnalysis(c, "sqlite", "DROP TABLE users;\n")

	reports := compatLintFormat(c, analysis, `{{ json (index .Files 0).Reports }}`)
	findings := compatLintFormat(c, analysis, `{{ range (index .Files 0).Findings }}{{ .Rule }}{{ end }}`)

	c.Assert(reports, qt.Equals, `[{"Text":"destructive changes detected","Diagnostics":[{"Pos":0,"Text":"Dropping table \"users\"",`+
		`"Code":"DS102","SuggestedFixes":[{"Message":"Add a pre-migration check to ensure table \"users\" is empty before dropping it"}]}]}]`)
	c.Assert(findings, qt.Equals, "DS102")
}

func TestWriteMigrateLintFormat_CarriesAPtahOnlyRuleName(t *testing.T) {
	c := qt.New(t)
	analysis := compatLintAnalysis(c, "postgres", "TRUNCATE users;\n")

	codes := compatLintFormat(c, analysis, `{{ range (index .Files 0).Reports }}{{ range .Diagnostics }}{{ .Code }}{{ end }}{{ end }}`)
	findings := compatLintFormat(c, analysis, `{{ range (index .Files 0).Findings }}{{ .Rule }}{{ end }}`)

	c.Assert(codes, qt.Equals, "destructive/truncate-table")
	c.Assert(findings, qt.Equals, "destructive/truncate-table")
}

// compatDialectCodeRows are migrations whose rules Atlas gives each engine its
// own code: a foreign key and a check added to a table are PG306 and PG305 on
// PostgreSQL and MY131 and MY144 on MySQL. Each row fails if the run's dialect
// does not reach the code lookup, whichever engine the lookup falls back to.
var compatDialectCodeRows = []struct {
	dialect string
	link    string
	codes   string
}{
	{dialect: "postgres", link: " https://atlasgo.io/lint/analyzers#PG306", codes: "PG306 PG305 "},
	{dialect: "mysql", link: " https://atlasgo.io/lint/analyzers#MY131", codes: "MY131 MY144 "},
}

func TestWriteMigrateLint_NamesASharedRuleByTheRunDialectsCode(t *testing.T) {
	for _, row := range compatDialectCodeRows {
		t.Run(row.dialect, func(t *testing.T) {
			c := qt.New(t)
			analysis := compatLintAnalysis(c, row.dialect,
				"ALTER TABLE users ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES accounts (id);\n"+
					"ALTER TABLE users ADD CONSTRAINT ck CHECK (a > 0);\n")

			text := compatLintText(c, analysis)
			codes := compatLintFormat(c, analysis, `{{ range (index .Files 0).Reports }}{{ range .Diagnostics }}{{ .Code }} {{ end }}{{ end }}`)
			findings := compatLintFormat(c, analysis, `{{ range (index .Files 0).Findings }}{{ .Rule }} {{ end }}`)

			c.Assert(text, qt.Contains, row.link)
			c.Assert(codes, qt.Equals, row.codes)
			c.Assert(findings, qt.Equals, row.codes)
		})
	}
}
