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
	"slices"
	"strings"
	"testing"
	"time"

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
// the rule.
//
// Position and value are independent facts. A recognized `-- +ptah` key with a
// value nobody can read is a typo the operator wants to hear about, and
// demoting it to a position warning would let two failures mask each other.
//
// TestMisplacedDirectiveErrorFindsNothingToRefuse carries the other side, and
// the two are one rule: what this function refuses is exactly what that one
// leaves alone.
func TestMalformedDirectiveValueIsRefusedWhereverItSits(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "malformed bool below the statement",
			sql:     directiveStatement + "-- +ptah no_transaction=maybe\n",
			wantErr: `invalid \+ptah no_transaction value "maybe": expected true or false \(on line 2, .*\)`,
		},
		{
			name:    "malformed lock timeout below the statement",
			sql:     directiveStatement + "-- +ptah lock_timeout=soon\n",
			wantErr: `invalid \+ptah lock_timeout value: .* \(on line 2, .*\)`,
		},
		{
			name:    "malformed statement timeout below the statement",
			sql:     directiveStatement + "-- +ptah statement_timeout=0s\n",
			wantErr: `invalid \+ptah statement_timeout value: .* \(on line 2, .*\)`,
		},
		{
			name:    "bare lock timeout below the statement",
			sql:     directiveStatement + "-- +ptah lock_timeout\n",
			wantErr: `invalid \+ptah directive "lock_timeout" \(on line 2, .*\)`,
		},
		{
			name:    "bare statement timeout below the statement",
			sql:     directiveStatement + "-- +ptah statement-timeout\n",
			wantErr: `invalid \+ptah directive "statement-timeout" \(on line 2, .*\)`,
		},
		{
			// A bare timeout does not stop being malformed because a field the
			// merged parser CAN read shares its line. The same two fields in
			// the header are refused by parseTimeoutDirectiveFields, which
			// walks every field rather than giving up once one parsed.
			name:    "bare lock timeout beside a recognized field below the statement",
			sql:     directiveStatement + "-- +ptah no_transaction lock_timeout\n",
			wantErr: `invalid \+ptah directive "lock_timeout" \(on line 2, .*\)`,
		},
		{
			// The neighbor here is a key nobody reads, which is the shape that
			// makes the dependence on a SEPARATE field clearest: it changes
			// nothing about whether `statement_timeout` has a value.
			name:    "bare statement timeout beside an unknown key below the statement",
			sql:     directiveStatement + "-- +ptah online_ddl_tool=ghost statement_timeout\n",
			wantErr: `invalid \+ptah directive "statement_timeout" \(on line 2, .*\)`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Check(misplacedDirectiveError(test.sql, ""), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestMisplacedDirectiveErrorFindsNothingToRefuse is the boundary of the
// refusal above: a line this function has no grammar to judge, or whose value
// another parser owns, must pass through it silently.
//
// The `atlas:` row is measurement rather than preference: on the pinned
// community binary, `migrate apply` over a SQLite directory carrying
// `-- atlas:txmode bogus` exits 1 when the line is the header and 0 when it
// sits below the statement. Refusing the second here would exit non-zero where
// the binary accepts.
func TestMisplacedDirectiveErrorFindsNothingToRefuse(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			// An ordered check's arguments are ParseChecks' grammar, not
			// field-split pairs, and the header parser skips the body whole.
			// Field-splitting it here would refuse below the statement exactly
			// what the header accepts.
			name: "a timeout word inside an ordered check below the statement",
			sql:  directiveStatement + `-- +ptah check assert="SELECT 1" lock_timeout` + "\n",
		},
		{
			name: "a well-formed directive below the statement",
			sql:  directiveStatement + "-- +ptah no_transaction\n",
		},
		{
			// The merged parser does not recognize a bare token that is not
			// no_transaction, so there is no grammar for it to be wrong
			// against. Refusing it would turn every `-- +ptah TODO` note below
			// a statement into a failed migration.
			name: "an unrecognized bare token below the statement",
			sql:  directiveStatement + "-- +ptah revisit_this\n",
		},
		{
			// Nothing consumes an unknown key=value pair, so nothing can say
			// its value is malformed.
			name: "an unknown key below the statement",
			sql:  directiveStatement + "-- +ptah future_knob=whatever\n",
		},
		{
			name: "the atlas spelling below the statement, which is reported instead",
			sql:  directiveStatement + "-- atlas:txmode bogus\n",
		},
		{
			name: "a malformed value in the header, which is its own parser's finding",
			sql:  "-- +ptah no_transaction=maybe\n\n" + directiveStatement,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Check(misplacedDirectiveError(test.sql, ""), qt.IsNil)
		})
	}
}

// headerScanDialects is every target [directiveHeaderLength] can be asked
// about, in the same order [go.5x5.cz/ptah/internal/ptahdirective] enumerates
// them for its conservative scan. A dialect missing from here would silently
// drop out of the tables below.
var headerScanDialects = []string{
	platform.Postgres,
	platform.MySQL,
	platform.MariaDB,
	platform.SQLite,
	platform.ClickHouse,
	platform.CockroachDB,
	platform.YugabyteDB,
	platform.SQLServer,
	platform.Spanner,
}

// TestHashCommentHeaderFollowsTheLexerRatherThanAList is the header half of the
// dialect rule.
//
// A `#` line is a comment for every target except SQL Server, and the migrator
// learns that from [go.5x5.cz/ptah/internal/dialectlexer.Options] -- the same
// options the file will be tokenized with. Naming the dialects here instead
// produced exactly one wrong row per dialect the list forgot: ClickHouse leaves
// hash comments enabled and was left out, so a ClickHouse file opening with `#`
// ended its header on line 1 and ran without the `no_transaction` its author
// wrote. SQL Server is the negative control and the reason no row here is
// vacuous.
func TestHashCommentHeaderFollowsTheLexerRatherThanAList(t *testing.T) {
	sql := "# generated migration\n-- +ptah no_transaction\n\n" + directiveStatement

	tests := []struct {
		name          string
		dialect       string
		wantMode      MigrationFileTxMode
		wantMisplaced []int
	}{
		{
			// The dialect is unresolved until a connection exists, and a
			// migration file is loaded before one does. Reading a SHORTER
			// header here refuses a correctly placed directive as misplaced,
			// and no later dialect resolution can take that back.
			name:     "unresolved dialect",
			dialect:  "",
			wantMode: MigrationFileTxModeNone,
		},
		{name: "postgres", dialect: platform.Postgres, wantMode: MigrationFileTxModeNone},
		{name: "mysql", dialect: platform.MySQL, wantMode: MigrationFileTxModeNone},
		{name: "mariadb", dialect: platform.MariaDB, wantMode: MigrationFileTxModeNone},
		{name: "sqlite", dialect: platform.SQLite, wantMode: MigrationFileTxModeNone},
		{name: "clickhouse", dialect: platform.ClickHouse, wantMode: MigrationFileTxModeNone},
		{name: "cockroachdb", dialect: platform.CockroachDB, wantMode: MigrationFileTxModeNone},
		{name: "yugabytedb", dialect: platform.YugabyteDB, wantMode: MigrationFileTxModeNone},
		{name: "spanner", dialect: platform.Spanner, wantMode: MigrationFileTxModeNone},
		{
			// The one target whose options disable hash comments. `# ...` is
			// not a comment there, so the directive really does sit below the
			// first non-comment line and the report is the correct answer.
			name:          "sqlserver",
			dialect:       platform.SQLServer,
			wantMode:      MigrationFileTxModeUnspecified,
			wantMisplaced: []int{2},
		},
	}

	c := qt.New(t)
	c.Assert(tests, qt.HasLen, len(headerScanDialects)+1, qt.Commentf(
		"every target plus the unresolved dialect needs a row; a missing one reads as agreement"))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed := parseMigrationFileTxModeForDialect("1_x.sql", sql, test.dialect)

			c.Check(parsed.err, qt.IsNil)
			c.Check(parsed.mode, qt.Equals, test.wantMode)
			c.Check(misplacedLines(misplacedDirectives(sql, test.dialect)), qt.DeepEquals, test.wantMisplaced)
		})
	}
}

// TestUnresolvedDirectiveHeaderIsTheLongestAnyTargetWouldRead states the
// property the row above only samples.
//
// Load time reads the header with no dialect, and its verdict is final: a
// directive it places outside the header is refused before a connection exists,
// so the unresolved scan must never cut the header shorter than the dialect
// that will execute the file would. Equality with the longest, rather than
// merely "not shorter", is what keeps the no-dialect options from quietly
// becoming a tenth set of comment rules of their own.
func TestUnresolvedDirectiveHeaderIsTheLongestAnyTargetWouldRead(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "hash comment above the directive", sql: "# c\n-- +ptah no_transaction\n\n" + directiveStatement},
		{name: "hash comment is the whole header", sql: "# c\n" + directiveStatement},
		{name: "dash dash with no space after it", sql: "--+ptah no_transaction\n\n" + directiveStatement},
		{name: "an ordinary header", sql: "-- c\n-- +ptah no_transaction\n\n" + directiveStatement},
		{name: "a statement first", sql: directiveStatement + "-- +ptah no_transaction\n"},
		{name: "a block comment first", sql: "/* c */\n-- +ptah no_transaction\n\n" + directiveStatement},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			longest := 0
			for _, dialect := range headerScanDialects {
				longest = max(longest, directiveHeaderLength(test.sql, dialect))
			}

			c.Check(directiveHeaderLength(test.sql, ""), qt.Equals, longest, qt.Commentf("source:\n%s", test.sql))
		})
	}
}

// TestCommentTerminatorSurvivesTheHeaderScan is the trailing half of the rule
// the hash table above covers on the leading half.
//
// MySQL and MariaDB start a `--` comment only when a whitespace or control
// character follows the second dash, and on a separator line -- `-- `, `-- \r\n`,
// `--\t`, or `--` with nothing but its newline -- the character that satisfies
// that rule IS the trailing whitespace or the line terminator itself. The scan
// splits lines on `\n`, so it has already removed the terminator; trimming the
// right-hand side as well handed those two dialects a bare `--`, which their own
// lexer options classify as SQL. The header ended on line 1, and the
// `no_transaction` and `lock_timeout` written under it were reported as
// misplaced and never honored -- nothing failed, and the migration ran without
// the transaction mode and the lock timeout its author wrote.
//
// Every target is asked, not only the two with the rule, because the boundary
// must not move for the seven that read `--` as a comment unconditionally.
func TestCommentTerminatorSurvivesTheHeaderScan(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "a trailing space",
			sql:  "-- \n-- +ptah no_transaction\n-- +ptah lock_timeout=5s\n\n" + directiveStatement,
		},
		{
			name: "carriage return line endings",
			sql:  "-- \r\n-- +ptah no_transaction\r\n-- +ptah lock_timeout=5s\r\n\r\n" + directiveStatement,
		},
		{
			name: "a trailing tab",
			sql:  "--\t\n-- +ptah no_transaction\n-- +ptah lock_timeout=5s\n\n" + directiveStatement,
		},
		{
			// The terminator alone satisfies the boundary: a newline is a
			// control character, so MySQL reads `--\n` as an empty comment.
			name: "the line terminator alone",
			sql:  "--\n-- +ptah no_transaction\n-- +ptah lock_timeout=5s\n\n" + directiveStatement,
		},
	}

	targets := append(slices.Clone(headerScanDialects), "")

	c := qt.New(t)
	c.Assert(targets, qt.HasLen, len(headerScanDialects)+1, qt.Commentf(
		"the unresolved dialect decides the load-time verdict and needs a column of its own"))

	for _, test := range tests {
		for _, dialect := range targets {
			t.Run(test.name+" in dialect "+dialect, func(t *testing.T) {
				c := qt.New(t)

				parsed := parseMigrationFileTxModeForDialect("1_x.sql", test.sql, dialect)
				timeouts, err := parseMigrationTimeoutDirectivesForDialect(test.sql, dialect)

				c.Assert(err, qt.IsNil)
				c.Check(directiveHeaderLength(test.sql, dialect), qt.Equals, len(test.sql)-len(directiveStatement),
					qt.Commentf("everything above the statement is header"))
				c.Check(parsed.err, qt.IsNil)
				c.Check(parsed.mode, qt.Equals, MigrationFileTxModeNone)
				c.Check(timeouts.HasLockTimeout, qt.IsTrue)
				c.Check(timeouts.LockTimeout, qt.Equals, 5*time.Second)
				c.Check(misplacedLines(misplacedDirectives(test.sql, dialect)), qt.IsNil)
			})
		}
	}
}

// TestDashDashWithNoBoundaryStillEndsTheMySQLHeader is the control for the test
// above, and the reason the fix is not "a `--` prefix opens the header".
//
// `--x` has no whitespace or control character after the second dash, so MySQL
// and MariaDB read it as two minus operators rather than a comment, and the
// header genuinely ends there. Restoring the terminator must not blunt that:
// the boundary still comes from the lexer options the file will be split with,
// which is what makes the two dialects answer differently from the other seven
// on the very same bytes.
func TestDashDashWithNoBoundaryStillEndsTheMySQLHeader(t *testing.T) {
	sql := "--x\n-- +ptah lock_timeout=5s\n\n" + directiveStatement
	wholeHeader := len(sql) - len(directiveStatement)

	tests := []struct {
		name       string
		dialect    string
		wantHeader int
	}{
		{name: "unresolved dialect", dialect: "", wantHeader: wholeHeader},
		{name: "postgres", dialect: platform.Postgres, wantHeader: wholeHeader},
		{name: "mysql", dialect: platform.MySQL, wantHeader: 0},
		{name: "mariadb", dialect: platform.MariaDB, wantHeader: 0},
		{name: "sqlite", dialect: platform.SQLite, wantHeader: wholeHeader},
		{name: "clickhouse", dialect: platform.ClickHouse, wantHeader: wholeHeader},
		{name: "cockroachdb", dialect: platform.CockroachDB, wantHeader: wholeHeader},
		{name: "yugabytedb", dialect: platform.YugabyteDB, wantHeader: wholeHeader},
		{name: "sqlserver", dialect: platform.SQLServer, wantHeader: wholeHeader},
		{name: "spanner", dialect: platform.Spanner, wantHeader: wholeHeader},
	}

	c := qt.New(t)
	c.Assert(tests, qt.HasLen, len(headerScanDialects)+1, qt.Commentf(
		"every target plus the unresolved dialect needs a row; a missing one reads as agreement"))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			timeouts, err := parseMigrationTimeoutDirectivesForDialect(sql, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Check(directiveHeaderLength(sql, test.dialect), qt.Equals, test.wantHeader)
			c.Check(timeouts.HasLockTimeout, qt.Equals, test.wantHeader > 0)
		})
	}
}

// TestMalformedValueUnderAHashHeaderIsRefusedByItsOwnParser is the load-time
// consequence, measured through the function that loads a file.
//
// A cut-short header does not merely drop a directive: it makes the value check
// answer as the POSITION check, so the operator is told the line sits below the
// first SQL statement and to move it up -- while it is already the second line
// of the file. The refusal itself is right, because the duration really is
// unreadable; the diagnosis and the remedy were not.
func TestMalformedValueUnderAHashHeaderIsRefusedByItsOwnParser(t *testing.T) {
	c := qt.New(t)
	sql := "# generated migration\n-- +ptah lock_timeout=soon\n\n" + directiveStatement

	_, err := migrationFuncFromSQLStringWithMetadata("1_x.sql", sql, statementExecutionHooks{})

	c.Assert(err, qt.ErrorMatches, `invalid \+ptah lock_timeout value: .*`)
	c.Check(err.Error(), qt.Not(qt.Contains), "below the first SQL statement",
		qt.Commentf("the line is the second of the file; the remedy would be inapplicable"))
}

// TestHashCommentDoesNotWidenTheAtlasHeader keeps the two families apart where
// widening one of them could have merged them.
//
// Atlas reads its file directive from the unbroken run of `--` comments that
// starts at byte 0, which a `#` line is not. Extending the `+ptah` header past
// one must therefore leave `-- atlas:txmode` exactly where it was, or ptah would
// honor a directive the pinned community binary drops.
func TestHashCommentDoesNotWidenTheAtlasHeader(t *testing.T) {
	sql := "# generated migration\n-- atlas:txmode none\n\n" + directiveStatement

	for _, dialect := range []string{"", platform.MySQL, platform.ClickHouse} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			c := qt.New(t)

			mode, has, err := parseAtlasFileTxMode("1_x.sql", sql)

			c.Assert(err, qt.IsNil)
			c.Check(has, qt.IsFalse)
			c.Check(mode, qt.Equals, MigrationFileTxModeUnspecified)
			c.Check(misplacedLines(misplacedDirectives(sql, dialect)), qt.DeepEquals, []int{2})
		})
	}
}

// TestMalformedHeaderDirectiveKeepsItsOwnDiagnosis proves that the header row
// of TestMisplacedDirectiveErrorFindsNothingToRefuse does not mean "a malformed
// header value is fine": it is refused by the parser that honors it, with no
// position clause, because its position is correct.
func TestMalformedHeaderDirectiveKeepsItsOwnDiagnosis(t *testing.T) {
	c := qt.New(t)

	got := parseMigrationFileTxMode("1_x.sql", "-- +ptah no_transaction=maybe\n\n"+directiveStatement)

	c.Check(got.err, qt.ErrorMatches, `invalid \+ptah no_transaction value "maybe": expected true or false`)
	c.Check(got.source, qt.Equals, migrationFileTxModeSourcePtah)
}

// TestBareTimeoutBesideAnotherFieldIsRefusedInTheHeaderToo is the other half of
// the two beside-another-field rows in
// TestMalformedDirectiveValueIsRefusedWhereverItSits.
//
// "Refused wherever it sits" is a claim about two parsers, and asserting only
// the below-the-statement half would leave the gap open in the direction it was
// actually found: the misplaced scan gave up once ANY field on the line parsed,
// while the header scan walks every field. Both verdicts are measured on the
// same bytes so a change to either side reddens.
func TestBareTimeoutBesideAnotherFieldIsRefusedInTheHeaderToo(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{
			name:    "bare lock timeout beside a recognized field",
			line:    "-- +ptah no_transaction lock_timeout",
			wantErr: `invalid \+ptah directive "lock_timeout"`,
		},
		{
			name:    "bare statement timeout beside an unknown key",
			line:    "-- +ptah online_ddl_tool=ghost statement_timeout",
			wantErr: `invalid \+ptah directive "statement_timeout"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parseMigrationTimeoutDirectives(test.line + "\n\n" + directiveStatement)

			c.Check(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestTimeoutWordInsideAnOrderedCheckIsNotAFieldInTheHeaderEither is the
// control for the two rows above, and the reason the header parser cannot be
// written as "any bare timeout word is an error": an ordered check's arguments
// are ParseChecks' grammar, so a word that looks like a timeout key inside one
// is not a field at all.
func TestTimeoutWordInsideAnOrderedCheckIsNotAFieldInTheHeaderEither(t *testing.T) {
	c := qt.New(t)

	_, err := parseMigrationTimeoutDirectives(
		`-- +ptah check assert="SELECT 1" lock_timeout` + "\n\n" + directiveStatement)

	c.Check(err, qt.IsNil)
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
