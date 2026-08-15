package lintcatalog_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/lintcatalog"
	"go.5x5.cz/ptah/internal/migrationlintgate"
	"go.5x5.cz/ptah/internal/sqllint"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	pageBegin = "<!-- BEGIN GENERATED LINT RULES -->"
	pageEnd   = "<!-- END GENERATED LINT RULES -->"
)

// pagePath is the one document that carries the generated enumeration.
var pagePath = filepath.Join("..", "..", "docs", "site", "src", "content", "docs", "reference", "lint-rules.md")

// codesOf collects the identifiers of a slice of entries.
func codesOf(entries []lintcatalog.Entry) []string {
	codes := make([]string, 0, len(entries))
	for _, entry := range entries {
		codes = append(codes, entry.Code)
	}
	return codes
}

// registeredMigrationCodes is what migration lint would report a finding under.
func registeredMigrationCodes() []string {
	rules := lint.Rules()
	codes := make([]string, 0, len(rules))
	for _, rule := range rules {
		codes = append(codes, rule.Code)
	}
	return codes
}

// generatedBlock is the enumeration as the generator renders it now.
func generatedBlock(c *qt.C) string {
	var buffer bytes.Buffer
	c.Assert(lintcatalog.WriteMarkdown(&buffer), qt.IsNil)
	return buffer.String()
}

// publishedBlock is the enumeration as the documentation page carries it.
func publishedBlock(c *qt.C) string {
	source, err := os.ReadFile(pagePath)
	c.Assert(err, qt.IsNil)
	page := string(source)
	c.Assert(page, qt.Contains, pageBegin,
		qt.Commentf("%s carries no begin marker; a gate that compares nothing to nothing reports success", pagePath))
	c.Assert(page, qt.Contains, pageEnd,
		qt.Commentf("%s carries no end marker", pagePath))
	_, after, _ := strings.Cut(page, pageBegin+"\n")
	before, _, _ := strings.Cut(after, pageEnd)
	return before
}

// findingCodes lints one fixture under one compatibility profile and returns
// the identifiers it reported.
func findingCodes(c *qt.C, files map[string]string, profile lint.CompatibilityProfile) []string {
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat:     migrator.MigrationDirFormatPtah,
		Compatibility: profile,
	})
	c.Assert(err, qt.IsNil)
	var codes []string
	for _, finding := range analysis.Findings() {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

func TestEntriesEnumerateEveryRegisteredRule(t *testing.T) {
	c := qt.New(t)

	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)

	documented := codesOf(entries)
	slices.Sort(documented)

	registered := append(registeredMigrationCodes(), sqllint.CatalogIDs()...)
	slices.Sort(registered)

	c.Assert(documented, qt.DeepEquals, registered)
}

func TestValidateAcceptsTheShippedCatalog(t *testing.T) {
	c := qt.New(t)

	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	c.Assert(lintcatalog.Validate(entries), qt.IsNil)
}

// TestValidateRejectsADisagreement drives the failure paths the gate exists
// for. Each row is a catalog the code would contradict, and every one of them
// has to be refused: a check that only ever ran against a consistent catalog
// would report success without having established anything.
func TestValidateRejectsADisagreement(t *testing.T) {
	rows := []struct {
		name    string
		entries func() []lintcatalog.Entry
		message string
	}{
		{
			name: "prefix no family declares",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Code: "ZZ101", Summary: "invented"}}
			},
			message: "which no family declares",
		},
		{
			name: "Atlas code the analyzer list does not document",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Code: "PG999", Summary: "invented", AtlasCode: "PG999"}}
			},
			message: "the Atlas analyzer list does not document",
		},
		{
			name: "Ptah rule in an Atlas family without the suffix",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Code: "PG901", Summary: "invented"}}
			},
			message: "does not follow the identifier convention",
		},
		{
			name: "rule with no meaning",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Code: "PG901P"}}
			},
			message: "has no one-line meaning",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			err := lintcatalog.Validate(row.entries())
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.message)
		})
	}
}

// TestConventionSpellsIdentifiersTheWayTheIssueDecided pins the three arms of
// the naming rule, so a change to one of them cannot pass as a refactor.
func TestConventionSpellsIdentifiersTheWayTheIssueDecided(t *testing.T) {
	rows := []struct {
		name    string
		entry   lintcatalog.Entry
		follows bool
		origin  lintcatalog.Origin
		note    string
	}{
		{
			name:    "Atlas check under the Atlas identifier",
			entry:   lintcatalog.Entry{Code: "PG101", AtlasCode: "PG101"},
			follows: true,
			origin:  lintcatalog.OriginAtlas,
		},
		{
			name:    "Atlas check under an identifier of ours",
			entry:   lintcatalog.Entry{Code: "PG106", AtlasCode: "PG102"},
			follows: false,
			origin:  lintcatalog.OriginAtlas,
		},
		{
			name:    "Ptah rule extending an Atlas family carries P",
			entry:   lintcatalog.Entry{Code: "PG112P"},
			follows: true,
			origin:  lintcatalog.OriginPtah,
		},
		{
			name:    "Ptah rule extending an Atlas family without P",
			entry:   lintcatalog.Entry{Code: "PG112"},
			follows: false,
			origin:  lintcatalog.OriginPtah,
		},
		{
			name:    "Ptah rule in a family of ours carries none",
			entry:   lintcatalog.Entry{Code: "SQL001"},
			follows: true,
			origin:  lintcatalog.OriginPtah,
		},
		{
			name:    "Ptah rule in a family of ours must not carry P",
			entry:   lintcatalog.Entry{Code: "SQL001P"},
			follows: false,
			origin:  lintcatalog.OriginPtah,
			note:    "leaves unmarked",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(row.entry.FollowsConvention(), qt.Equals, row.follows)
			c.Assert(row.entry.Origin(), qt.Equals, row.origin)
			c.Assert(row.entry.ConventionNote(), qt.Contains, row.note)
		})
	}
}

// TestGeneratedBlockMatchesThePublishedPage is the drift gate in the form the
// default test run reaches. scripts/check-lint-rules.sh checks the same thing
// with a --write escape hatch; this is what fails a change that never ran it.
func TestGeneratedBlockMatchesThePublishedPage(t *testing.T) {
	c := qt.New(t)

	generated := generatedBlock(c)
	c.Assert(generated, qt.Not(qt.Equals), "")
	c.Assert(publishedBlock(c), qt.Equals, generated,
		qt.Commentf("run scripts/check-lint-rules.sh --write"))
}

// TestCommandTableNamesTheFamilyTheApplyGateReports covers the one claim about
// scope the page makes outside the generated block.
//
// The "Which commands lint" table is prose a reader meets before the rule
// tables, and its `ptah migrations up` row names the family that gate reports.
// Nothing regenerates that row, so without this the gate could narrow to
// another family and leave the summary promising the old one -- the same drift
// the generated block exists to prevent, one section higher up the page.
func TestCommandTableNamesTheFamilyTheApplyGateReports(t *testing.T) {
	c := qt.New(t)

	source, err := os.ReadFile(pagePath)
	c.Assert(err, qt.IsNil)

	c.Assert(string(source), qt.Contains,
		"| `ptah migrations up` | native | blocking `"+migrationlintgate.ReportedFamily+"` findings only |",
		qt.Commentf("the command table on %s no longer names the family the apply gate reports", pagePath))
}

// TestSurfaceColumnMatchesWhatEachProfileReports measures the claim the surface
// column makes rather than repeating it. BC101 is the one rule the
// compatibility profile does not report -- it classifies a rename as a
// destructive change instead -- and a rule the catalog calls reachable from
// both has to appear under both profiles on the same input.
func TestSurfaceColumnMatchesWhatEachProfileReports(t *testing.T) {
	c := qt.New(t)

	renamed := map[string]string{
		"0000000001_rename.up.sql":   "ALTER TABLE users RENAME TO accounts;",
		"0000000001_rename.down.sql": "ALTER TABLE accounts RENAME TO users;",
	}

	native := findingCodes(c, renamed, lint.CompatibilityProfileNative)
	compat := findingCodes(c, renamed, lint.CompatibilityProfileAtlas)

	c.Assert(native, qt.Contains, "BC101")
	c.Assert(compat, qt.Not(qt.Contains), "BC101")
	c.Assert(compat, qt.Contains, "DS101")

	dropped := map[string]string{
		"0000000001_drop.up.sql":   "DROP TABLE users;",
		"0000000001_drop.down.sql": "CREATE TABLE users (id INTEGER);",
	}
	c.Assert(findingCodes(c, dropped, lint.CompatibilityProfileNative), qt.Contains, "DS101")
	c.Assert(findingCodes(c, dropped, lint.CompatibilityProfileAtlas), qt.Contains, "DS101")

	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	var nativeOnly []string
	for _, entry := range entries {
		nativeOnly = append(nativeOnly, entry.Code+"="+entry.SurfaceLabel())
	}
	c.Assert(nativeOnly, qt.Contains, "BC101=native only")
	c.Assert(nativeOnly, qt.Contains, "DS101=both")
}
