package migrator

// White-box testing required: the rest of the directive class has no exported
// observable. `-- +ptah lock_timeout`, `-- +ptah statement_timeout`,
// `-- atlas:checkpoint`, `-- atlas:txtar` and `-- atlas:assert oneof` are read
// by unexported parsers whose results reach the exported surface only after a
// database connection and a migration directory exist. Covering their POSITION
// through that surface would test the loader, not the rule, and would leave the
// exported half of the class (directive_position_test.go) as the only measured
// part -- which is how one directive got a different position rule from its
// neighbor in the first place. The scope opt-in is here for the same reason:
// the region it selects is an unexported decision.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/directiveplacement"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// directiveStatement is the placement table's statement, aliased so the
// diagnosis fixtures below read as source rather than as a package reference.
const directiveStatement = directiveplacement.Statement

// internalDirectiveCase mirrors the exported class table for the directives
// with no exported observable.
type internalDirectiveCase struct {
	name      string
	directive string
	honored   map[string]bool
	observe   func(c *qt.C, sql string) bool
}

func TestDirectivePositionForDirectivesWithNoExportedObservable(t *testing.T) {
	tests := []internalDirectiveCase{
		{
			// The timeout scanner keeps a stop condition of its own inside the
			// region, which is what makes it header-scoped under the opt-in
			// too. That is deliberate: the opt-in restores the scope the merged
			// directive map had, and these keys never had it.
			name:      "ptah lock_timeout",
			directive: "-- +ptah lock_timeout=5s",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(c *qt.C, sql string) bool {
				timeouts, err := parseMigrationTimeoutDirectives(sql, directiveScopeHeader)
				c.Assert(err, qt.IsNil)
				return timeouts.HasLockTimeout
			},
		},
		{
			name:      "ptah statement_timeout",
			directive: "-- +ptah statement_timeout=1s",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(c *qt.C, sql string) bool {
				timeouts, err := parseMigrationTimeoutDirectives(sql, directiveScopeHeader)
				c.Assert(err, qt.IsNil)
				return timeouts.HasStatementTimeout
			},
		},
		{
			// Already first-line-only before this change, which is stricter
			// than the shared rule and therefore consistent with it. The row
			// exists so that stays true.
			name:      "atlas checkpoint",
			directive: "-- atlas:checkpoint",
			honored:   directiveplacement.OnlyTheFirstLine(),
			observe: func(c *qt.C, sql string) bool {
				isCheckpoint, err := atlasCheckpointFromSQL("1_x.sql", sql)
				c.Assert(err, qt.IsNil)
				return isCheckpoint
			},
		},
		{
			// `-- atlas:assert oneof` is read from the leading comments of an
			// Atlas check file, so only whitespace and comments may precede it.
			name:      "atlas assert oneof",
			directive: "-- atlas:assert oneof",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(_ *qt.C, sql string) bool {
				return atlasCheckFileMode(sql, "") == checkGroupOneOf
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.honored, qt.HasLen, len(directiveplacement.All), qt.Commentf(
				"every placement needs an answer; a missing key reads as dropped"))

			for _, placement := range directiveplacement.All {
				c.Run(placement.Name, func(c *qt.C) {
					sql := placement.Render(test.directive)

					c.Check(test.observe(c, sql), qt.Equals, test.honored[placement.Name],
						qt.Commentf("source:\n%s", sql))
				})
			}
		})
	}
}

// TestMisplacedTxtarDirectiveIsRefusedRatherThanDropped records the one member
// of the class that was never silent. A `-- atlas:txtar` marker below a section
// header is a hard error, which is the loudest end of "diagnosed, not dropped"
// and the shape the rest of the class now approaches with a warning.
func TestMisplacedTxtarDirectiveIsRefusedRatherThanDropped(t *testing.T) {
	c := qt.New(t)
	sql := "-- migration.sql --\nSELECT 1;\n-- atlas:txtar\n"

	_, _, err := parseAtlasTxtarSQL("1_x.sql", sql)

	c.Assert(err, qt.ErrorMatches, `.*must be the first non-empty line.*`)
}

// TestMisplacedDirectivesAreReportedNotDropped is the diagnosis half.
//
// Silent dropping is what made the `atlas:` side dangerous: the operator writes
// `txmode none`, the run exits 0, and nothing says the file ran inside a
// transaction anyway. Every position the class table marks as dropped must
// therefore produce a report naming the line -- and every position it marks as
// honored must produce none, or a clean run stops being clean.
func TestMisplacedDirectivesAreReportedNotDropped(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantLines []int
		wantTexts []string
	}{
		{
			name:      "ptah directive below the statement",
			sql:       directiveStatement + "-- +ptah no_transaction\n",
			wantLines: []int{2},
			wantTexts: []string{"-- +ptah no_transaction"},
		},
		{
			name:      "atlas directive below the statement",
			sql:       directiveStatement + "-- atlas:txmode none\n",
			wantLines: []int{2},
			wantTexts: []string{"-- atlas:txmode none"},
		},
		{
			name:      "atlas directive after a leading blank line",
			sql:       "\n-- atlas:txmode none\n\n" + directiveStatement,
			wantLines: []int{2},
			wantTexts: []string{"-- atlas:txmode none"},
		},
		{
			name:      "atlas directive indented",
			sql:       "  -- atlas:txmode none\n\n" + directiveStatement,
			wantLines: []int{1},
			wantTexts: []string{"-- atlas:txmode none"},
		},
		{
			name:      "atlas directive after a blank line inside the header",
			sql:       "-- create the table\n\n-- atlas:txmode none\n" + directiveStatement,
			wantLines: []int{3},
			wantTexts: []string{"-- atlas:txmode none"},
		},
		{
			name:      "both families below the statement, in line order",
			sql:       directiveStatement + "-- atlas:txmode none\n-- +ptah no_transaction\n",
			wantLines: []int{2, 3},
			wantTexts: []string{"-- atlas:txmode none", "-- +ptah no_transaction"},
		},
		{
			name:      "honored header directives report nothing",
			sql:       "-- atlas:txmode none\n-- +ptah online_ddl_tool=ghost\n\n" + directiveStatement,
			wantLines: nil,
			wantTexts: nil,
		},
		{
			name:      "a check below the statement is honored, so it is not reported",
			sql:       directiveStatement + `-- +ptah check name="x" assert="SELECT 1"` + "\n",
			wantLines: nil,
			wantTexts: nil,
		},
		{
			name:      "a trailing comment carries no directive and is not reported",
			sql:       "CREATE TABLE t (id INTEGER PRIMARY KEY); -- atlas:txmode none\n",
			wantLines: nil,
			wantTexts: nil,
		},
		{
			name:      "a marker the merged parser would ignore anyway is not reported",
			sql:       directiveStatement + "-- +ptah frobnicate\n",
			wantLines: nil,
			wantTexts: nil,
		},
		{
			name:      "a directive inside a string literal is not a directive",
			sql:       "INSERT INTO notes (body) VALUES ('\n-- +ptah no_transaction\n');\n",
			wantLines: nil,
			wantTexts: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			found := misplacedDirectives(test.sql, "", directiveScopeHeader)

			c.Check(misplacedLines(found), qt.DeepEquals, test.wantLines, qt.Commentf("source:\n%s", test.sql))
			c.Check(misplacedTexts(found), qt.DeepEquals, test.wantTexts, qt.Commentf("source:\n%s", test.sql))
		})
	}
}

func misplacedLines(found []misplacedDirective) []int {
	var lines []int
	for _, one := range found {
		lines = append(lines, one.line)
	}
	return lines
}

func misplacedTexts(found []misplacedDirective) []string {
	var texts []string
	for _, one := range found {
		texts = append(texts, one.text)
	}
	return texts
}

// TestAtlasHeaderBoundaryMatchesTheParserThatUsesIt ties the boundary the
// diagnosis names to the boundary the parser honors.
//
// Two descriptions of one block is how a warning starts pointing at a line the
// parser was perfectly happy with, or stays quiet about one it dropped. The
// equivalence is asserted over the whole placement table rather than argued.
func TestAtlasHeaderBoundaryMatchesTheParserThatUsesIt(t *testing.T) {
	for _, placement := range directiveplacement.All {
		t.Run(placement.Name, func(t *testing.T) {
			c := qt.New(t)
			sql := placement.Render("-- atlas:txmode none")

			mode, has, err := parseAtlasFileTxMode("1_x.sql", sql)
			c.Assert(err, qt.IsNil)
			insideBlock := strings.Index(sql, "atlas:txmode") < atlasDirectiveHeaderLength(sql)

			c.Check(has && mode == MigrationFileTxModeNone, qt.Equals, insideBlock,
				qt.Commentf("source:\n%s", sql))
		})
	}
}

// TestDirectiveScopeOptInRestoresTheFileWideRule proves the capability is kept
// rather than removed, and that a typo in the variable fails rather than
// silently reading as the default.
func TestDirectiveScopeOptInRestoresTheFileWideRule(t *testing.T) {
	tests := []struct {
		name    string
		environ func(testing.TB)
		assert  func(c *qt.C)
	}{
		{
			name:    "unset keeps the header rule",
			environ: envbooltest.Unset(directivesAnywhereEnvVar),
			assert: func(c *qt.C) {
				scope, err := resolveDirectiveScope()
				c.Assert(err, qt.IsNil)
				c.Check(scope, qt.Equals, directiveScopeHeader)
				c.Check(ParseFileDirectives(directiveStatement+"-- +ptah no_transaction\n"), qt.HasLen, 0)
			},
		},
		{
			name:    "a false spelling keeps the header rule",
			environ: envbooltest.Set(directivesAnywhereEnvVar, "false"),
			assert: func(c *qt.C) {
				scope, err := resolveDirectiveScope()
				c.Assert(err, qt.IsNil)
				c.Check(scope, qt.Equals, directiveScopeHeader)
			},
		},
		{
			name:    "the opt-in honors a directive below the statement",
			environ: envbooltest.Set(directivesAnywhereEnvVar, "1"),
			assert: func(c *qt.C) {
				sql := directiveStatement + "-- +ptah no_transaction\n"

				scope, err := resolveDirectiveScope()
				c.Assert(err, qt.IsNil)
				c.Check(scope, qt.Equals, directiveScopeFile)
				c.Check(ParseFileDirectives(sql), qt.DeepEquals, map[string]string{"no_transaction": "true"})
				c.Check(parseMigrationFileTxMode("1_x.sql", sql).mode, qt.Equals, MigrationFileTxModeNone)
				c.Check(misplacedDirectives(sql, "", directiveScopeFile), qt.HasLen, 0)

				// The opt-in restores the merged map's scope and nothing else:
				// the timeout keys were header-scoped before it and stay so.
				timeouts, err := parseMigrationTimeoutDirectives(
					directiveStatement+"-- +ptah lock_timeout=5s\n", directiveScopeFile)
				c.Assert(err, qt.IsNil)
				c.Check(timeouts.HasLockTimeout, qt.IsFalse)
			},
		},
		{
			name:    "the opt-in leaves the atlas spelling on the community rule",
			environ: envbooltest.Set(directivesAnywhereEnvVar, "1"),
			assert: func(c *qt.C) {
				sql := directiveStatement + "-- atlas:txmode none\n"

				c.Check(parseMigrationFileTxMode("1_x.sql", sql).mode, qt.Equals, MigrationFileTxModeUnspecified)
				c.Check(misplacedDirectives(sql, "", directiveScopeFile), qt.HasLen, 1)
			},
		},
		{
			name:    "a typo is a configuration error, not the default",
			environ: envbooltest.Set(directivesAnywhereEnvVar, "yes"),
			assert: func(c *qt.C) {
				_, err := resolveDirectiveScope()

				c.Check(err, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_DIRECTIVES_ANYWHERE`)
			},
		},
		{
			name:    "an empty value is a configuration error too",
			environ: envbooltest.Set(directivesAnywhereEnvVar, ""),
			assert: func(c *qt.C) {
				_, err := resolveDirectiveScope()

				c.Check(err, qt.ErrorMatches, `invalid boolean value "" for PTAH_DIRECTIVES_ANYWHERE`)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.environ(t)

			test.assert(c)
		})
	}
}

// TestDirectiveHeaderLengthStopsAtTheFirstExecutableLine pins the region every
// `+ptah` parser now shares.
func TestDirectiveHeaderLengthStopsAtTheFirstExecutableLine(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want int
	}{
		{name: "empty", sql: "", want: 0},
		{name: "only comments", sql: "-- a\n-- b\n", want: 10},
		{name: "comment then statement", sql: "-- a\nSELECT 1;\n", want: 5},
		{name: "blank line does not end it", sql: "-- a\n\n-- b\nSELECT 1;\n", want: 11},
		{name: "indentation does not end it", sql: "  -- a\nSELECT 1;\n", want: 7},
		{name: "a statement first means no header", sql: "SELECT 1;\n-- a\n", want: 0},
		{name: "a block comment first means no header", sql: "/* a */\n-- b\nSELECT 1;\n", want: 0},
		{name: "no trailing newline", sql: "-- a", want: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Check(directiveHeaderLength(test.sql), qt.Equals, test.want)
		})
	}
}
