package atlas_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin what `ptah-compat` SAYS about a `--dir` URL query key it took
// no meaning from, and what it does about one when asked to be strict
// (stokaro/ptah#1013 section 2, second half).
//
// The exit codes here are not new. Measured against the pinned community binary
// v1.3.0 on 2026-08-08, `--dir 'file://m?nonsense=1'` exits 0 on all eight verbs
// that binary registers --dir on, on both tools, and reads the directory exactly
// as no query at all does; stokaro/ptah#1087 and #1135 closed that and it is
// pinned by TestCompatMigrateDirQuery_IgnoresUnknownKeysOnEveryVerb. Those eight
// are not every verb ptah-compat registers --dir on: `checkpoint`, `down`,
// `edit`, `rebase`, `rm` and `test` register it too and refuse any query on it,
// which TestCompatMigrateDirQuery_QueryStaysRefusedOnTheVerbsThatTakeNoQuery_FailurePath
// pins below. What was still
// missing is that Ptah dropped the key in SILENCE, which is the half of the
// compatibility policy matching does not discharge: a misspelled `?fromat=goose`
// selects nothing on either binary, so the directory is read in the native Atlas
// layout while the operator believes it is being read as Goose, and a run that
// says nothing about it lets the typo look like the layout they asked for.
//
// So the default reports on stderr and still exits 0, and PTAH_STRICT_DIR_QUERY
// makes the refusal available to a pipeline that wants a typo to stop it. The
// stdout assertions are the parity guard on the report: several of these verbs
// carry a machine-readable document on stdout and the community binary emits no
// field for this, so a note that reached stdout would corrupt a caller's parse.

// ignoredDirQueryNote is the substring the report is asserted by. It names the
// key rather than only the fact of a note, because a report that cannot say
// which key was dropped is not the capability this keeps.
const ignoredDirQueryNote = `ignoring migration directory URL query key "nonsense"`

// strictDirQueryEnvVar is the environment variable spelling under test. It is
// restated here rather than exported from the command package: a test that
// reads the constant it is pinning cannot catch a rename that breaks every
// operator's environment file.
const strictDirQueryEnvVar = "PTAH_STRICT_DIR_QUERY"

// unsetStrictDirQueryEnv removes PTAH_STRICT_DIR_QUERY for the duration of a
// test. t.Setenv records the caller's original value and restores it on
// cleanup, so registering it first makes the following Unsetenv safe to leave
// in place.
func unsetStrictDirQueryEnv(t *testing.T) {
	t.Helper()
	t.Setenv(strictDirQueryEnvVar, "")
	if err := os.Unsetenv(strictDirQueryEnvVar); err != nil {
		t.Fatalf("unset %s: %v", strictDirQueryEnvVar, err)
	}
}

// dirQueryVerbRow invokes one verb against dir with query appended to the --dir
// URL. Each verb needs different companion flags, so the invocation is per-row
// wiring rather than a branch in a test body.
type dirQueryVerbRow struct {
	name string
	run  func(c *qt.C, dir, query string) (stdout, stderr string, err error)
}

// dirQueryVerbRows lists every verb the pinned community binary registers --dir
// on. Measured on 2026-08-08, its other eight migrate verbs — `checkpoint`,
// `down`, `edit`, `import`, `push`, `rebase`, `rm` and `test` — all answer
// `Error: unknown flag: --dir` there, so it has no --dir query contract outside
// these eight. ptah-compat registers --dir on six of those (every one but
// `import` and `push`) and refuses any query on them, which is a place it is
// stricter rather than a parity gap; see
// TestCompatMigrateDirQuery_QueryStaysRefusedOnTheVerbsThatTakeNoQuery_FailurePath.
func dirQueryVerbRows() []dirQueryVerbRow {
	return []dirQueryVerbRow{
		{
			name: "apply",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "apply",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "apply.db"))
			},
		},
		{
			name: "hash",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "hash", "--dir", "file://"+dir+query)
			},
		},
		{
			name: "validate",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "validate", "--dir", "file://"+dir+query)
			},
		},
		{
			name: "lint",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "lint",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--latest", "1")
			},
		},
		{
			name: "status",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "status",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))
			},
		},
		{
			name: "set",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "set", "20240101000000",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "set.db"))
			},
		},
		{
			name: "new",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "new", "demo", "--dir", "file://"+dir+query)
			},
		},
		{
			name: "diff",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "diff", "dd",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+writeDirQueryTargetSchema(c))
			},
		},
	}
}

// writeDirQueryTargetSchema writes a desired state that differs from the
// fixture directory, so `migrate diff` plans something instead of reporting the
// directory synced.
func writeDirQueryTargetSchema(c *qt.C) string {
	c.Helper()
	target := filepath.Join(c.TempDir(), "target.sql")
	c.Assert(os.WriteFile(
		target,
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	return target
}

// TestCompatMigrateDirQuery_ReportsIgnoredKeyOnEveryVerb pins the report on all
// eight verbs, and pins that it changes nothing a script reads: the exit code
// stays 0 and the note never reaches stdout.
func TestCompatMigrateDirQuery_ReportsIgnoredKeyOnEveryVerb(t *testing.T) {
	for _, tt := range dirQueryVerbRows() {
		t.Run(tt.name, func(t *testing.T) {
			unsetStrictDirQueryEnv(t)
			c := qt.New(t)

			stdout, stderr, err := tt.run(c, writeQueryFixtureDir(c), "?nonsense=1")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stderr, qt.Contains, ignoredDirQueryNote)
			c.Assert(stdout, qt.Not(qt.Contains), "note:")
			c.Assert(stdout, qt.Not(qt.Contains), "nonsense")
		})
	}
}

// TestCompatMigrateDirQuery_ReportKeepsMachineReadableStdoutClean is the
// stronger form of the stdout assertion above: with `--format`, standard output
// is exactly the rendered template and nothing else, so a note that leaked
// there would be visible as a changed value rather than only as a missing
// substring. `migrate status` is the verb pipelines parse with a machine.
func TestCompatMigrateDirQuery_ReportKeepsMachineReadableStdoutClean(t *testing.T) {
	unsetStrictDirQueryEnv(t)
	c := qt.New(t)

	stdout, stderr, err := runCompatExit("migrate", "status",
		"--dir", "file://"+writeQueryFixtureDir(c)+"?nonsense=1",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"),
		"--format", "{{ len .Pending }}")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(strings.TrimSpace(stdout), qt.Equals, "1")
	c.Assert(stderr, qt.Contains, ignoredDirQueryNote)
}

// TestCompatMigrateDirQuery_ReportsNothingWhenEveryKeyIsMeaningful is the
// control arm. Without it the assertion above would also pass on a build that
// printed the note unconditionally, which would be noise on every correct
// invocation rather than a report about a dropped key.
func TestCompatMigrateDirQuery_ReportsNothingWhenEveryKeyIsMeaningful(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "no query at all", query: ""},
		{name: "empty format value selects the atlas layout", query: "?format="},
		{name: "format names the layout the directory is in", query: "?format=atlas"},
		{name: "a repeated format lost a value, not the key", query: "?format=atlas&format=goose"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetStrictDirQueryEnv(t)
			c := qt.New(t)

			_, stderr, err := runCompatExit("migrate", "status",
				"--dir", "file://"+writeQueryFixtureDir(c)+tt.query,
				"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stderr, qt.Not(qt.Contains), "ignoring migration directory URL query")
		})
	}
}

// TestCompatMigrateDirQuery_ForeignFormatStillSelectsTheLayoutBesideAnIgnoredKey
// is the control on the one key that MUST keep its meaning.
//
// The golang-migrate fixture is what makes it a flip rather than an exit-0
// assertion: its atlas.sum covers only 1_init.up.sql, so read as a native Atlas
// directory it is a checksum mismatch. Both binaries exit 1 on it with no query.
// If the report ever grew into "drop the whole query", this row would exit 1
// while the note row above kept passing.
func TestCompatMigrateDirQuery_ForeignFormatStillSelectsTheLayoutBesideAnIgnoredKey(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "ignored key after format", query: "?format=golang-migrate&nonsense=1"},
		{name: "ignored key before format", query: "?nonsense=1&format=golang-migrate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetStrictDirQueryEnv(t)
			c := qt.New(t)

			stdout, stderr, err := runCompatExit("migrate", "validate",
				"--dir", "file://"+writeLintGolangMigrateDir(c)+tt.query)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stderr, qt.Contains, ignoredDirQueryNote)
			c.Assert(stdout, qt.Not(qt.Contains), "note:")
		})
	}
}

// TestCompatMigrateDirQuery_ForeignFormatControlWithoutTheLayoutSelection is the
// other half of the flip above: the same fixture, the same ignored key, and no
// `?format=`. It must stay exit 1, because that is what makes the exit 0 above
// evidence that the format key was honored.
func TestCompatMigrateDirQuery_ForeignFormatControlWithoutTheLayoutSelection(t *testing.T) {
	unsetStrictDirQueryEnv(t)
	c := qt.New(t)

	_, _, err := runCompatExit("migrate", "validate",
		"--dir", "file://"+writeLintGolangMigrateDir(c)+"?nonsense=1")

	c.Assert(err, qt.ErrorMatches, `checksum mismatch`)
}

// TestCompatMigrateDirQuery_StrictEnvRefusesIgnoredKey_FailurePath pins the
// capability the default no longer exercises.
//
// The two writing verbs also assert the directory is unchanged, because a
// refusal that exits 1 after writing is not the refusal an operator asked for.
func TestCompatMigrateDirQuery_StrictEnvRefusesIgnoredKey_FailurePath(t *testing.T) {
	const want = `atlas migrate \w+ --dir: unrecognized migration directory URL query key "nonsense": ` +
		`only \?format= selects the directory layout, and PTAH_STRICT_DIR_QUERY is set`

	for _, tt := range dirQueryVerbRows() {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(strictDirQueryEnvVar, "1")
			c := qt.New(t)
			dir := writeQueryFixtureDir(c)
			before := atlasDirEntryNames(c, dir)

			_, _, err := tt.run(c, dir, "?nonsense=1")

			c.Assert(err, qt.ErrorMatches, want)
			c.Assert(atlasDirEntryNames(c, dir), qt.DeepEquals, before)
		})
	}
}

// TestCompatMigrateDirQuery_StrictEnvKeepsAMeaningfulQueryWorking is the control
// on the refusal: strict mode refuses keys the run took no meaning from, not
// every query. Without it the test above would also pass on a build that refused
// `?format=atlas` under the same variable.
func TestCompatMigrateDirQuery_StrictEnvKeepsAMeaningfulQueryWorking(t *testing.T) {
	t.Setenv(strictDirQueryEnvVar, "1")
	c := qt.New(t)

	_, stderr, err := runCompatExit("migrate", "status",
		"--dir", "file://"+writeQueryFixtureDir(c)+"?format=atlas",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
}

// TestCompatMigrateDirQuery_StrictEnvRejectsInvalidValue_FailurePath pins that a
// typo in the variable is a hard error rather than a silent "off".
//
// It is the same choice PTAH_SKIP_CHECKS makes and for the same reason: a
// misspelled value in a CI environment file must not read as "the check is off"
// to the tool while the operator believes it is on.
//
// The second row is the one that makes the first row's claim true rather than
// accidental. An implementation that resolves the variable only after finding an
// ignored key passes the first row and still exits 0 in silence on every
// invocation without one — which is every CORRECT invocation, so a CI file
// holding `PTAH_STRICT_DIR_QUERY=nope` would be reported on by nothing until the
// day the typo it was set to catch actually happened. PTAH_SKIP_CHECKS is
// resolved on every `migrate apply` regardless of what the run contains, and the
// row below holds this variable to the same contract.
//
// The fixture is built BEFORE the variable is set: writeQueryFixtureDir seeds
// the directory with a `migrate hash` run of this same binary, which under an
// invalid value is refused too, and a fixture that failed to build would redden
// this test for a reason that is not the property under test.
func TestCompatMigrateDirQuery_StrictEnvRejectsInvalidValue_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "an ignored key is present", query: "?nonsense=1"},
		{name: "the URL carries no query at all", query: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetStrictDirQueryEnv(t)
			c := qt.New(t)
			dir := writeQueryFixtureDir(c)
			t.Setenv(strictDirQueryEnvVar, "nope")

			_, _, err := runCompatExit("migrate", "status",
				"--dir", "file://"+dir+tt.query,
				"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))

			c.Assert(err, qt.ErrorMatches,
				`atlas migrate status --dir: invalid boolean value "nope" for PTAH_STRICT_DIR_QUERY`)
		})
	}
}

// dirQueryRefusingVerbRows lists the verbs that register --dir and refuse ANY
// query on it, rather than reading the query and naming the keys it took no
// meaning from. Each verb carries the companion flags it needs, so the refusal
// is the one under test rather than a missing-flag complaint.
func dirQueryRefusingVerbRows() []dirQueryVerbRow {
	return []dirQueryVerbRow{
		{
			name: "checkpoint",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "checkpoint",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"))
			},
		},
		{
			name: "down",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "down",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "down.db"))
			},
		},
		{
			name: "edit",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "edit", "20240101000000_init",
					"--dir", "file://"+dir+query)
			},
		},
		{
			name: "rebase",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "rebase", "20240101000000_init",
					"--dir", "file://"+dir+query)
			},
		},
		{
			name: "rm",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "rm", "20240101000000_init",
					"--dir", "file://"+dir+query)
			},
		},
		{
			name: "test",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				return runCompatExit("migrate", "test",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"))
			},
		},
	}
}

// TestCompatMigrateDirQuery_QueryStaysRefusedOnTheVerbsThatTakeNoQuery_FailurePath
// bounds the report to the verbs it actually reaches.
//
// The eight rows of TestCompatMigrateDirQuery_ReportsIgnoredKeyOnEveryVerb are
// not every verb that registers --dir: these six register it too and refuse any
// query on it before the format is resolved, so the note never fires there and
// PTAH_STRICT_DIR_QUERY governs nothing on them. Measured against the pinned
// community binary v1.3.0 on 2026-08-08, all six answer `unknown flag: --dir`,
// so this is an internal inconsistency of the compatibility surface rather than
// a parity gap — it is stricter than a binary that has no contract here at all,
// which is the allowed direction, and stokaro/ptah#1013 tracks it.
//
// It is a fixture rather than a remark because the documentation now enumerates
// the two sets. A change that made any of these six start ignoring a query, or
// that pointed the eight-verb note at one of them, would move a documented
// boundary while every other test in this file stayed green.
func TestCompatMigrateDirQuery_QueryStaysRefusedOnTheVerbsThatTakeNoQuery_FailurePath(t *testing.T) {
	const want = `atlas migrate \w+ --dir: migration directory URL query parameters are not supported for this command`

	for _, tt := range dirQueryRefusingVerbRows() {
		t.Run(tt.name, func(t *testing.T) {
			unsetStrictDirQueryEnv(t)
			c := qt.New(t)

			_, stderr, err := tt.run(c, writeQueryFixtureDir(c), "?nonsense=1")

			c.Assert(err, qt.ErrorMatches, want)
			c.Assert(stderr, qt.Not(qt.Contains), "ignoring migration directory URL query")
		})
	}
}

// The two tests below hold the DOCUMENTATION to the split the two tests above
// measure, because a sentence that overstates it ships as surely as a line of
// code that does.
//
// `ptah-compat` has two disjoint sets of verbs here: eight read a `--dir` query
// and ignore the keys that select nothing, and six refuse a `--dir` query
// outright. A sentence saying the key "is ignored on every verb" is therefore
// false for six of them, and it was false on the compat command reference —
// the `migrate new` section closed with "as it is on every other verb" while
// the shared query rules eleven paragraphs above already named the six that
// refuse one. Both sentences were on the same page, so a reader could not tell
// which was the contract.
//
// A prose claim needs a fixture for the same reason a code path does: the
// enumeration was correct when it was written and went stale when the verb sets
// moved apart. Precedent for reading a published page from a Go test rather
// than from a docs-site checker: migrate_down_integrity_test.go, which pins
// that the `atlas.sum` enumeration accounts for `migrate down`. It is also the
// wiring that runs — .github/workflows/docs.yml lists each `check:*` script by
// name, and `check:limitations` is in package.json and in no workflow.

// everyVerbClaim matches a claim quantified over all verbs — "every verb",
// "every other verb", "every migrate verb", "every Atlas verb".
//
// It is not a violation on its own. The compat reference legitimately says
// `--dir` "must name a scheme ... on every Atlas verb", which is a property of
// the community binary's own flag and true of all of them. It is a violation
// only in the same sentence as an ignored-key claim, which is what
// [unboundedIgnoredKeyClaims] pairs it with.
var everyVerbClaim = regexp.MustCompile(`(?i)every (other )?(migrate |Atlas )?verb`)

// dirQueryDocPage names a published page that states the ignored-key rule.
//
// A page is governed by being listed here, and each row asserts the page still
// states the rule at all: a checker whose subject quietly left the page reports
// OK forever.
type dirQueryDocPage struct {
	name string
	path []string
}

// dirQueryGovernedDocPages lists every published page carrying the rule. The
// feature matrix is generated from docs/site/scripts/data/feature-matrix-rows.json,
// so governing the page covers the row data behind it too.
func dirQueryGovernedDocPages() []dirQueryDocPage {
	return []dirQueryDocPage{
		{name: "reference_atlas-commands", path: []string{"reference", "atlas-commands.md"}},
		{name: "atlas_migrate-commands", path: []string{"atlas", "migrate-commands.md"}},
		{name: "atlas_comparison", path: []string{"atlas", "comparison.md"}},
		{name: "atlas_overview", path: []string{"atlas", "overview.md"}},
		{name: "atlas_feature-matrix", path: []string{"atlas", "feature-matrix.md"}},
	}
}

// readCompatDocPage returns a published documentation page verbatim.
func readCompatDocPage(c *qt.C, page []string) string {
	c.Helper()
	parts := append([]string{"..", "..", "docs", "site", "src", "content", "docs"}, page...)
	source, err := os.ReadFile(filepath.Join(parts...))
	c.Assert(err, qt.IsNil)
	return string(source)
}

// asOneLine collapses every run of whitespace to a single space, so a claim
// Markdown wrapped over four source lines is matched as the reader meets it
// rather than as it happens to be stored. Without this the stale sentence would
// have to be searched for in exactly the wrapping it had.
func asOneLine(source string) string {
	return strings.Join(strings.Fields(source), " ")
}

// unboundedIgnoredKeyClaims returns the sentences of a page that assert a query
// key is ignored AND quantify that over every verb.
func unboundedIgnoredKeyClaims(page string) []string {
	found := make([]string, 0)
	for sentence := range strings.SplitSeq(asOneLine(page), ". ") {
		found = appendUnboundedIgnoredKeyClaim(found, sentence)
	}
	return found
}

// appendUnboundedIgnoredKeyClaim keeps one sentence when it makes both claims
// at once. It is a helper rather than a filter inside the loop so the test body
// stays free of control flow.
func appendUnboundedIgnoredKeyClaim(found []string, sentence string) []string {
	if !strings.Contains(strings.ToLower(sentence), "ignor") {
		return found
	}
	if !everyVerbClaim.MatchString(sentence) {
		return found
	}
	return append(found, sentence)
}

// TestCompatMigrateDirQuery_DocsBoundTheIgnoredKeyClaim pins that no published
// page claims the ignore holds on every verb, and that each governed page still
// states the rule.
func TestCompatMigrateDirQuery_DocsBoundTheIgnoredKeyClaim(t *testing.T) {
	for _, tt := range dirQueryGovernedDocPages() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			page := readCompatDocPage(c, tt.path)

			c.Assert(strings.ToLower(page), qt.Contains, "ignor",
				qt.Commentf("page no longer states the ignored-key rule, so this row proves nothing"))
			claims := unboundedIgnoredKeyClaims(page)
			c.Assert(claims, qt.HasLen, 0, qt.Commentf("unbounded claims: %q", claims))
		})
	}
}

// migrateNewReferenceSection returns the `ptah-compat migrate new` section of
// the compat command reference: everything from its heading to the next
// third-level heading.
func migrateNewReferenceSection(c *qt.C) string {
	c.Helper()
	page := readCompatDocPage(c, []string{"reference", "atlas-commands.md"})
	_, afterHeading, found := strings.Cut(page, "### `ptah-compat migrate new`")
	c.Assert(found, qt.IsTrue)
	body, _, _ := strings.Cut(afterHeading, "\n### ")
	return asOneLine(body)
}

// TestCompatMigrateDirQuery_MigrateNewReferenceSectionNamesBothVerbSets pins the
// split where the stale sentence stood.
//
// [TestCompatMigrateDirQuery_DocsBoundTheIgnoredKeyClaim] would also pass on a
// section that dropped the sentence entirely, which would leave a reader of the
// one verb that WRITES the directory with no statement at all about what a
// `--dir` query does there. These rows require the bound to be named, not merely
// not overstated.
func TestCompatMigrateDirQuery_MigrateNewReferenceSectionNamesBothVerbSets(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "names the accepting bound", want: "verbs that accept a `--dir` query"},
		{name: "checkpoint", want: "`checkpoint`"},
		{name: "down", want: "`down`"},
		{name: "edit", want: "`edit`"},
		{name: "rebase", want: "`rebase`"},
		{name: "rm", want: "`rm`"},
		{name: "test", want: "`test`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(migrateNewReferenceSection(c), qt.Contains, tt.want)
		})
	}
}

// TestCompatMigrateDirQuery_RejectedFormatValueReportsOnlyItsOwnRefusal pins the
// ordering. A value neither spelling can parse is what the run is refused for,
// and the note about an ignored key beside it would be a second diagnostic the
// community binary does not print.
func TestCompatMigrateDirQuery_RejectedFormatValueReportsOnlyItsOwnRefusal(t *testing.T) {
	unsetStrictDirQueryEnv(t)
	c := qt.New(t)

	_, stderr, err := runCompatExit("migrate", "status",
		"--dir", "file://"+writeQueryFixtureDir(c)+"?format=totally-bogus&nonsense=1",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))

	c.Assert(err, qt.ErrorMatches, `unknown dir format "totally-bogus"`)
	c.Assert(stderr, qt.Not(qt.Contains), "ignoring migration directory URL query")
}
