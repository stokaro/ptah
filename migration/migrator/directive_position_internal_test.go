package migrator

// White-box testing required: the rest of the directive class has no exported
// observable. `-- +ptah lock_timeout`, `-- +ptah statement_timeout`,
// `-- atlas:checkpoint`, `-- atlas:txtar` and `-- atlas:assert oneof` are read
// by unexported parsers whose results reach the exported surface only after a
// database connection and a migration directory exist. Covering their POSITION
// through that surface would test the loader, not the rule, and would leave the
// exported half of the class (directive_position_test.go) as the only measured
// part -- which is how one directive got a different position rule from its
// neighbor in the first place.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/directiveplacement"
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
			// The timeout scanner reads the same region [directiveRegion]
			// hands every other `-- +ptah` parser, so these keys and the
			// transaction mode answer to one boundary rather than to two that
			// happened to agree.
			name:      "ptah lock_timeout",
			directive: "-- +ptah lock_timeout=5s",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(c *qt.C, sql string) bool {
				timeouts, err := parseMigrationTimeoutDirectives(sql)
				c.Assert(err, qt.IsNil)
				return timeouts.HasLockTimeout
			},
		},
		{
			name:      "ptah statement_timeout",
			directive: "-- +ptah statement_timeout=1s",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(c *qt.C, sql string) bool {
				timeouts, err := parseMigrationTimeoutDirectives(sql)
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

			found := misplacedDirectives(test.sql, "")

			c.Check(misplacedLines(found), qt.DeepEquals, test.wantLines, qt.Commentf("source:\n%s", test.sql))
			c.Check(misplacedTexts(found), qt.DeepEquals, test.wantTexts, qt.Commentf("source:\n%s", test.sql))
		})
	}
}

// TestMalformedDirectiveValueIsRefusedWhereverItSits is the severity half of
// the rule, and the half the two families answer differently.
//
// Position and value are independent facts. A recognized `-- +ptah` key with a
// value nobody can read is a typo the operator wants to hear about, and
// demoting it to a position warning would let two failures mask each other.
//
// The `atlas:` spelling gets no equivalent, by measurement rather than by
// preference: on the pinned community binary, `migrate apply` over a SQLite
// directory carrying `-- atlas:txmode bogus` exits 1 when the line is the
// header and 0 when it sits below the statement. Refusing the second here would
// exit non-zero where the binary accepts.
func TestMalformedDirectiveValueIsRefusedWhereverItSits(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		assert func(c *qt.C, err error)
	}{
		{
			name:   "malformed bool below the statement",
			sql:    directiveStatement + "-- +ptah no_transaction=maybe\n",
			assert: refusedWith(`invalid \+ptah no_transaction value "maybe": expected true or false \(on line 2, .*\)`),
		},
		{
			name:   "malformed lock timeout below the statement",
			sql:    directiveStatement + "-- +ptah lock_timeout=soon\n",
			assert: refusedWith(`invalid \+ptah lock_timeout value: .* \(on line 2, .*\)`),
		},
		{
			name:   "malformed statement timeout below the statement",
			sql:    directiveStatement + "-- +ptah statement_timeout=0s\n",
			assert: refusedWith(`invalid \+ptah statement_timeout value: .* \(on line 2, .*\)`),
		},
		{
			name:   "bare lock timeout below the statement",
			sql:    directiveStatement + "-- +ptah lock_timeout\n",
			assert: refusedWith(`invalid \+ptah directive "lock_timeout" \(on line 2, .*\)`),
		},
		{
			name:   "bare statement timeout below the statement",
			sql:    directiveStatement + "-- +ptah statement-timeout\n",
			assert: refusedWith(`invalid \+ptah directive "statement-timeout" \(on line 2, .*\)`),
		},
		{
			// A bare timeout does not stop being malformed because a field the
			// merged parser CAN read shares its line. The same two fields in
			// the header are refused by parseTimeoutDirectiveFields, which
			// walks every field rather than giving up once one parsed.
			name:   "bare lock timeout beside a recognized field below the statement",
			sql:    directiveStatement + "-- +ptah no_transaction lock_timeout\n",
			assert: refusedWith(`invalid \+ptah directive "lock_timeout" \(on line 2, .*\)`),
		},
		{
			// The neighbor here is a key nobody reads, which is the shape that
			// makes the dependence on a SEPARATE field clearest: it changes
			// nothing about whether `statement_timeout` has a value.
			name:   "bare statement timeout beside an unknown key below the statement",
			sql:    directiveStatement + "-- +ptah online_ddl_tool=ghost statement_timeout\n",
			assert: refusedWith(`invalid \+ptah directive "statement_timeout" \(on line 2, .*\)`),
		},
		{
			// An ordered check's arguments are ParseChecks' grammar, not
			// field-split pairs, and the header parser skips the body whole.
			// Field-splitting it here would refuse below the statement exactly
			// what the header accepts.
			name:   "a timeout word inside an ordered check below the statement is not an error",
			sql:    directiveStatement + `-- +ptah check assert="SELECT 1" lock_timeout` + "\n",
			assert: notRefused,
		},
		{
			name:   "a well-formed directive below the statement is not an error",
			sql:    directiveStatement + "-- +ptah no_transaction\n",
			assert: notRefused,
		},
		{
			// The merged parser does not recognize a bare token that is not
			// no_transaction, so there is no grammar for it to be wrong
			// against. Refusing it would turn every `-- +ptah TODO` note below
			// a statement into a failed migration.
			name:   "an unrecognized bare token below the statement is not an error",
			sql:    directiveStatement + "-- +ptah revisit_this\n",
			assert: notRefused,
		},
		{
			// Nothing consumes an unknown key=value pair, so nothing can say
			// its value is malformed.
			name:   "an unknown key below the statement is not an error",
			sql:    directiveStatement + "-- +ptah future_knob=whatever\n",
			assert: notRefused,
		},
		{
			name:   "the atlas spelling below the statement is reported, not refused",
			sql:    directiveStatement + "-- atlas:txmode bogus\n",
			assert: notRefused,
		},
		{
			name:   "a malformed value in the header is not this function's finding",
			sql:    "-- +ptah no_transaction=maybe\n\n" + directiveStatement,
			assert: notRefused,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			test.assert(c, misplacedDirectiveError(test.sql, ""))
		})
	}
}

func TestMySQLHashCommentKeepsFollowingDirectiveInHeader(t *testing.T) {
	c := qt.New(t)
	sql := "# generated migration\n-- +ptah no_transaction\nSELECT 1;\n"

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		parsed := parseMigrationFileTxModeForDialect("1_x.sql", sql, dialect)
		c.Check(parsed.err, qt.IsNil)
		c.Check(parsed.mode, qt.Equals, MigrationFileTxModeNone)
		c.Check(misplacedDirectives(sql, dialect), qt.HasLen, 0)
	}

	parsed := parseMigrationFileTxModeForDialect("1_x.sql", sql, platform.Postgres)
	c.Check(parsed.err, qt.IsNil)
	c.Check(parsed.mode, qt.Equals, MigrationFileTxModeUnspecified)
}

// refusedWith and notRefused are the two verdicts a row can carry, so the table
// body needs no branch of its own.
func refusedWith(pattern string) func(c *qt.C, err error) {
	return func(c *qt.C, err error) {
		c.Check(err, qt.ErrorMatches, pattern)
	}
}

func notRefused(c *qt.C, err error) {
	c.Check(err, qt.IsNil)
}

// TestMalformedHeaderDirectiveKeepsItsOwnDiagnosis proves the row above does
// not mean "a malformed header value is fine": it is refused by the parser that
// honors it, with no position clause, because its position is correct.
func TestMalformedHeaderDirectiveKeepsItsOwnDiagnosis(t *testing.T) {
	c := qt.New(t)

	got := parseMigrationFileTxMode("1_x.sql", "-- +ptah no_transaction=maybe\n\n"+directiveStatement)

	c.Check(got.err, qt.ErrorMatches, `invalid \+ptah no_transaction value "maybe": expected true or false`)
	c.Check(got.source, qt.Equals, migrationFileTxModeSourcePtah)
}

// TestBareTimeoutBesideAnotherFieldIsRefusedInTheHeaderToo is the other half of
// the three rows above.
//
// "Refused wherever it sits" is a claim about two parsers, and asserting only
// the below-the-statement half would leave the gap open in the direction it was
// actually found: the misplaced scan gave up once ANY field on the line parsed,
// while the header scan walks every field. Both verdicts are measured on the
// same bytes so a change to either side reddens.
func TestBareTimeoutBesideAnotherFieldIsRefusedInTheHeaderToo(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		assert func(c *qt.C, err error)
	}{
		{
			name:   "bare lock timeout beside a recognized field",
			line:   "-- +ptah no_transaction lock_timeout",
			assert: refusedWith(`invalid \+ptah directive "lock_timeout"`),
		},
		{
			name:   "bare statement timeout beside an unknown key",
			line:   "-- +ptah online_ddl_tool=ghost statement_timeout",
			assert: refusedWith(`invalid \+ptah directive "statement_timeout"`),
		},
		{
			name:   "a timeout word inside an ordered check",
			line:   `-- +ptah check assert="SELECT 1" lock_timeout`,
			assert: notRefused,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parseMigrationTimeoutDirectives(test.line + "\n\n" + directiveStatement)

			test.assert(c, err)
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

			c.Check(directiveHeaderLength(test.sql, ""), qt.Equals, test.want)
		})
	}
}
