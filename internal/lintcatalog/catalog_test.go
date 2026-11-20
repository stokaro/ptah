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
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintcatalog"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/internal/providerpublished"
	"ptah.run/internal/sqllint"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
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
		codes = append(codes, entry.Name)
	}
	return codes
}

// registeredMigrationCodes is what migration lint would report a finding under.
func registeredMigrationCodes() []string {
	runtime := builtintest.Runtime()
	rules := must.Must(lint.Rules(must.Must(providerpublished.LintRuntime(runtime, runtime))))
	codes := make([]string, 0, len(rules))
	for _, rule := range rules {
		codes = append(codes, rule.Name)
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
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		DirFormat:     migrationfile.DirFormatPtah,
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

// messagesFor lints a directory under one compatibility profile and returns the
// messages reported under one identifier, in the order they were reported.
func messagesFor(tb testing.TB, files map[string]string, profile lint.CompatibilityProfile, code string) []string {
	c := qt.New(tb)

	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		DirFormat:     migrationfile.DirFormatPtah,
		Compatibility: profile,
	})
	c.Assert(err, qt.IsNil)

	var messages []string
	for _, finding := range analysis.Findings() {
		if finding.Rule == code {
			messages = append(messages, finding.Message)
		}
	}
	return messages
}

// summaryOf returns the one-line meaning the catalog publishes for a rule.
func summaryOf(tb testing.TB, code string) string {
	c := qt.New(tb)

	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	for _, entry := range entries {
		if entry.Name == code {
			return entry.Summary
		}
	}
	c.Fatalf("no catalog entry for %s", code)
	return ""
}

// TestDropTableSeparatesTheDropFromTheRename measures the two consequences one
// identifier carries.
//
// On the compatibility surface a rename is classified as destructive and
// reports destructive/drop-table, but the table and its rows survive under the new name -- the
// rule's own message says the name is retired, not that anything was deleted.
// The published meaning said the rename "destroys the table and every row in
// it", which is the drop's consequence attached to the wrong path.
//
// What this pins is the distinction in the code, which nothing measured before.
// It does not by itself pin the wording: an assertion that the summary mentions
// a rename passed against the wrong summary too, because that one mentioned a
// rename while describing it wrongly.
func TestDropTableSeparatesTheDropFromTheRename(t *testing.T) {
	c := qt.New(t)

	renamed := messagesFor(t, map[string]string{
		"0000000001_rename.up.sql":   "ALTER TABLE users RENAME TO accounts;",
		"0000000001_rename.down.sql": "ALTER TABLE accounts RENAME TO users;",
	}, lint.CompatibilityProfileAtlas, "destructive/drop-table")

	dropped := messagesFor(t, map[string]string{
		"0000000001_drop.up.sql":   "DROP TABLE users;",
		"0000000001_drop.down.sql": "CREATE TABLE users (id INTEGER);",
	}, lint.CompatibilityProfileAtlas, "destructive/drop-table")

	c.Assert(renamed, qt.HasLen, 1)
	c.Assert(dropped, qt.HasLen, 1)
	c.Assert(renamed[0], qt.Contains, "retires the table name")
	c.Assert(dropped[0], qt.Not(qt.Contains), "retires the table name")
	c.Assert(summaryOf(t, "destructive/drop-table"), qt.Contains, "rename")
}

// TestPG102SummaryKeepsTheVersionDistinction pins the qualification the rule
// makes and the published meaning dropped.
//
// The rule says ALTER TYPE ... ADD VALUE cannot run in a transaction *before
// PostgreSQL 12* and that the value merely stays unusable within the
// transaction afterwards. The meaning said it cannot run inside a transaction
// block, full stop, which sends a reader on PostgreSQL 12 or later toward a
// non-transactional migration they do not need.
func TestPG102SummaryKeepsTheVersionDistinction(t *testing.T) {
	c := qt.New(t)

	messages := messagesFor(t, map[string]string{
		"0000000001_add_value.up.sql":   "ALTER TYPE order_status ADD VALUE 'refunded';",
		"0000000001_add_value.down.sql": "-- an enum value cannot be removed",
	}, lint.CompatibilityProfileNative, "PG102")

	c.Assert(messages, qt.HasLen, 1)
	c.Assert(messages[0], qt.Contains, "before PostgreSQL 12")
	c.Assert(summaryOf(t, "PG102"), qt.Contains, "PostgreSQL 12")
}

func TestEntriesEnumerateEveryRegisteredRule(t *testing.T) {
	c := qt.New(t)

	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)

	documented := codesOf(entries)
	slices.Sort(documented)

	// A name two owners publish for different engines is one entry.
	registered := append(slices.Compact(slices.Sorted(slices.Values(registeredMigrationCodes()))), sqllint.CatalogIDs()...)
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
			name: "rule with no meaning",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Name: "design/invented-rule"}}
			},
			message: "has no one-line meaning",
		},
		{
			name: "a name that breaks the grammar",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Name: "PG901", Summary: "invented"}}
			},
			message: "is not two lowercase hyphenated segments joined by a slash",
		},
		{
			name: "a category nobody declared",
			entries: func() []lintcatalog.Entry {
				return []lintcatalog.Entry{{Name: "nosuch/rule", Summary: "invented"}}
			},
			message: `names a category "nosuch" that is not declared`,
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

// TestGeneratedBlockMatchesThePublishedPage is the drift gate in the form the
// default test run reaches. scripts/check-docsync.sh checks the same thing
// with a --write escape hatch; this is what fails a change that never ran it.
func TestGeneratedBlockMatchesThePublishedPage(t *testing.T) {
	c := qt.New(t)

	desired := generatedBlock(c)
	c.Assert(desired, qt.Not(qt.Equals), "")
	c.Assert(publishedBlock(c), qt.Equals, desired,
		qt.Commentf("run scripts/check-docsync.sh --write"))
}

// TestCommandTableMatchesTheScopesTheCodeDefines covers the claims about scope
// the page makes outside the generated block.
//
// The "Which commands lint" table is prose a reader meets before the rule
// tables. Nothing regenerates it, so without this the gate could narrow to
// another family, or the surface label could be renamed, and the summary would
// go on promising the old one -- the same drift the generated block exists to
// prevent, one section higher up the page.
func TestCommandTableMatchesTheScopesTheCodeDefines(t *testing.T) {
	c := qt.New(t)

	source, err := os.ReadFile(pagePath)
	c.Assert(err, qt.IsNil)
	page := string(source)

	c.Assert(page, qt.Contains,
		"| `ptah migrations up` | native | blocking data-safety findings only (`"+migrationlintgate.ReportedRules()[0]+"` and the rules listed below) |",
		qt.Commentf("the command table on %s no longer names the rules the apply gate reports", pagePath))

	bothLabel := lintcatalog.Entry{Native: true, Compat: true}.SurfaceLabel()
	c.Assert(page, qt.Contains,
		"| `ptah-compat migrate lint` | compatibility | every rule marked `"+bothLabel+"` |",
		qt.Commentf("the command table on %s no longer points at the Surface column's own label", pagePath))
}

// TestSurfaceColumnMatchesWhatEachProfileReports measures the claim the surface
// column makes rather than repeating it. rollout/rename is the one rule the
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

	c.Assert(native, qt.Contains, "rollout/rename")
	c.Assert(compat, qt.Not(qt.Contains), "rollout/rename")
	c.Assert(compat, qt.Contains, "destructive/drop-table")

	dropped := map[string]string{
		"0000000001_drop.up.sql":   "DROP TABLE users;",
		"0000000001_drop.down.sql": "CREATE TABLE users (id INTEGER);",
	}
	c.Assert(findingCodes(c, dropped, lint.CompatibilityProfileNative), qt.Contains, "destructive/drop-table")
	c.Assert(findingCodes(c, dropped, lint.CompatibilityProfileAtlas), qt.Contains, "destructive/drop-table")

	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	var nativeOnly []string
	for _, entry := range entries {
		nativeOnly = append(nativeOnly, entry.Name+"="+entry.SurfaceLabel())
	}
	c.Assert(nativeOnly, qt.Contains, "rollout/rename=native only")
	c.Assert(nativeOnly, qt.Contains, "destructive/drop-table=both")
	c.Assert(nativeOnly, qt.Contains, "naming/identifier-too-long=native only")
	c.Assert(nativeOnly, qt.Contains, "naming/identifier-collision=native only")
}

// TestIdentifierRulesAreNativeOnly measures the native-only claim for naming/identifier-too-long
// and naming/identifier-collision on a directory whose second index PostgreSQL cuts to the name of
// the first: native lint reports both, and the compatibility profile, whose
// `migrate lint` keeps the community binary's findings and exit code, reports
// neither.
func TestIdentifierRulesAreNativeOnly(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 63)
	files := map[string]string{
		"0000000001_index.up.sql": "CREATE TABLE t (a int, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\n" +
			"CREATE INDEX \"" + long + "_beta\" ON t (b);\n",
		"0000000001_index.down.sql": "DROP TABLE t;\n",
	}

	native := postgresFindingCodes(c, files, lint.CompatibilityProfileNative)
	compat := postgresFindingCodes(c, files, lint.CompatibilityProfileAtlas)

	c.Assert(native, qt.Contains, "naming/identifier-too-long")
	c.Assert(native, qt.Contains, "naming/identifier-collision")
	c.Assert(compat, qt.Not(qt.Contains), "naming/identifier-too-long")
	c.Assert(compat, qt.Not(qt.Contains), "naming/identifier-collision")
}

// postgresFindingCodes is [findingCodes] for a PostgreSQL run, which the
// PostgreSQL owner's rules need to be sent at all.
func postgresFindingCodes(c *qt.C, files map[string]string, profile lint.CompatibilityProfile) []string {
	c.Helper()
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		DirFormat:     migrationfile.DirFormatPtah,
		Compatibility: profile,
		Dialect:       "postgres",
	})
	c.Assert(err, qt.IsNil)
	var codes []string
	for _, finding := range analysis.Findings() {
		codes = append(codes, finding.Rule)
	}
	return codes
}

