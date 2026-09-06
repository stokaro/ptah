package migrator

// White-box testing required: these directives and decisions have no exported
// observable on the load path. `-- atlas:checkpoint` and `-- atlas:assert
// oneof` are read by unexported loaders, and transaction-mode deferral to the
// target dialect is an internal classification the public execution API only
// shows after a database connection and a migration directory exist. The
// position rule itself lives in migration/migrationfile and is measured there
// against the same placement table.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/directiveplacement"
	"ptah.run/migration/migrationfile"
)

// loaderDirectiveCase mirrors the toolkit's class table for the directives the
// loader owns. See AGENTS.md, "A Table Row Carries Data, Not A Checker".
type loaderDirectiveCase struct {
	name      string
	directive string
	honored   map[string]bool
	observe   func(sql string) (bool, error)
}

func TestDirectivePositionForLoaderOwnedDirectives(t *testing.T) {
	tests := []loaderDirectiveCase{
		{
			// Already first-line-only before this change, which is stricter
			// than the shared rule and therefore consistent with it. The row
			// exists so that stays true.
			name:      "atlas checkpoint",
			directive: "-- atlas:checkpoint",
			honored:   directiveplacement.OnlyTheFirstLine(),
			observe: func(sql string) (bool, error) {
				return atlasCheckpointFromSQL("1_x.sql", sql)
			},
		},
		{
			// `-- atlas:assert oneof` is read from the leading comments of an
			// Atlas check file, so only whitespace and comments may precede it.
			name:      "atlas assert oneof",
			directive: "-- atlas:assert oneof",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(sql string) (bool, error) {
				return atlasCheckFileMode(sql, "") == checkGroupOneOf, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.honored, qt.HasLen, len(directiveplacement.All), qt.Commentf(
				"every placement needs an answer; a missing key reads as dropped"))

			for _, placement := range directiveplacement.All {
				t.Run(placement.Name, func(t *testing.T) {
					c := qt.New(t)
					sql := placement.Render(test.directive)

					honored, err := test.observe(sql)

					c.Assert(err, qt.IsNil, qt.Commentf("source:\n%s", sql))
					c.Check(honored, qt.Equals, test.honored[placement.Name],
						qt.Commentf("source:\n%s", sql))
				})
			}
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
	sql := "# generated migration\n-- +ptah lock_timeout=soon\n\n" + directiveplacement.Statement

	_, err := migrationFuncFromSQLStringWithMetadata("1_x.sql", sql, statementExecutionHooks{})

	c.Assert(err, qt.ErrorMatches, `invalid \+ptah lock_timeout value: .*`)
	c.Check(err.Error(), qt.Not(qt.Contains), "below the first SQL statement",
		qt.Commentf("the line is the second of the file; the remedy would be inapplicable"))
}

// dialectAmbiguousMarker is a `-- +ptah` line that only one dialect's string
// rules leave outside a string literal.
//
// With PostgreSQL's standard-conforming strings the backslash is an ordinary
// character, so the quote after it CLOSES the literal and the next line is a
// real line comment. MySQL treats the backslash as an escape, so the same bytes
// are one multi-line string and the marker is data.
//
// The line can only sit below executable SQL -- a string literal is executable
// SQL, and a marker inside one has to follow the statement that opens it. Under
// the header rule it is therefore inert on every dialect, which is the stronger
// property and the one asserted first. The dialect still decides for ordered
// checks, which are file-wide by design.
func dialectAmbiguousMarker(body string) string {
	return "SELECT 'prefix \\'\n-- +ptah " + body + "\nsuffix';\n"
}

// TestWellFormedDirectiveBelowTheStatementIsInertOnEveryDialect separates the
// two verdicts a misplaced directive earns.
//
// A WELL-FORMED directive below the statement is not honored and not refused --
// refusing it would remove behavior this tree shipped. The malformed sibling in
// the next test is refused. Both are reported.
func TestWellFormedDirectiveBelowTheStatementIsInertOnEveryDialect(t *testing.T) {
	c := qt.New(t)
	wellFormed := dialectAmbiguousMarker("no_transaction")

	loaded, err := migrationFuncFromSQLStringWithMetadata("1_ambiguous.sql", wellFormed, statementExecutionHooks{})
	c.Assert(err, qt.IsNil)
	c.Check(loaded.txMode, qt.Equals, migrationfile.FileTxModeUnspecified)
	c.Check(loaded.txModeErr, qt.IsNil)

	migration := &Migration{Description: "ambiguous"}
	setMigrationUp(migration, loaded)
	// Both dialects, because the position decided before the quoting did.
	mysqlMode := migration.parsedUpTxModeForDialect(platform.MySQL)
	c.Check(mysqlMode.Err, qt.IsNil)
	c.Check(mysqlMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)
	postgresMode := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Check(postgresMode.Err, qt.IsNil)
	c.Check(postgresMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)

	// It is a directive a reader would recognize, so it is reported rather than
	// dropped -- but only on the dialect that sees it outside a string.
	c.Check(migrationfile.MisplacedDirectives(wellFormed, platform.Postgres), qt.HasLen, 1)
	c.Check(migrationfile.MisplacedDirectives(wellFormed, platform.MySQL), qt.HasLen, 0)
}

// TestMigrationTxModeParsingDefersDialectSpecificStringsToTarget is the
// property the tagged contour protects, measured where it is live.
//
// A malformed VALUE is refused wherever the line sits, so the refusal still
// depends entirely on whether the target dialect's lexer sees a directive or a
// string literal. Without a dialect the conservative scan keeps nothing, which
// is why load time defers rather than refuses.
func TestMigrationTxModeParsingDefersDialectSpecificStringsToTarget(t *testing.T) {
	c := qt.New(t)
	invalidForPostgres := dialectAmbiguousMarker("no_transaction=maybe")

	loaded, err := migrationFuncFromSQLStringWithMetadata("1_ambiguous.sql", invalidForPostgres, statementExecutionHooks{})
	c.Assert(err, qt.IsNil)
	c.Assert(loaded.txMode, qt.Equals, migrationfile.FileTxModeUnspecified)
	c.Assert(loaded.txModeErr, qt.IsNil)

	migration := &Migration{Description: "ambiguous"}
	setMigrationUp(migration, loaded)
	mysqlMode := migration.parsedUpTxModeForDialect(platform.MySQL)
	c.Assert(mysqlMode.Err, qt.IsNil)
	c.Assert(mysqlMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)
	postgresMode := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Assert(postgresMode.Err, qt.ErrorMatches,
		`invalid \+ptah no_transaction value "maybe": expected true or false `+
			`\(on line 2, below the first SQL statement, where it would not have been honored\)`)
	c.Assert(postgresMode.Source, qt.Equals, migrationfile.FileTxModeSourcePtah)

	parsed, err := migrationfile.ParseUp("1_ambiguous.sql", invalidForPostgres)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.TxMode, qt.Equals, migrationfile.FileTxModeUnspecified)
}

func TestMigrationTxModeParsingKeepsMisplacedMarkerInertForTargetDialect(t *testing.T) {
	c := qt.New(t)
	markerForPostgres := dialectAmbiguousMarker("no_transaction")
	migration := CreateMigrationFromSQL(1, "target-aware", markerForPostgres, markerForPostgres)

	mysqlMode := migration.parsedUpTxModeForDialect(platform.MySQL)
	c.Assert(mysqlMode.Err, qt.IsNil)
	c.Assert(mysqlMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)
	postgresMode := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Assert(postgresMode.Err, qt.IsNil)
	c.Assert(postgresMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)
	mysqlDownMode := migration.parsedDownTxModeForDialect(platform.MySQL)
	c.Assert(mysqlDownMode.Err, qt.IsNil)
	c.Assert(mysqlDownMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)
	postgresDownMode := migration.parsedDownTxModeForDialect(platform.Postgres)
	c.Assert(postgresDownMode.Err, qt.IsNil)
	c.Assert(postgresDownMode.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)

	migration.UpTxMode = migrationfile.FileTxModeFile
	migration.DownTxMode = migrationfile.FileTxModeFile
	overridden := migration.parsedUpTxModeForDialect(platform.Postgres)
	c.Assert(overridden.Mode, qt.Equals, migrationfile.FileTxModeFile)
	c.Assert(overridden.Err, qt.IsNil)
	overriddenDown := migration.parsedDownTxModeForDialect(platform.Postgres)
	c.Assert(overriddenDown.Mode, qt.Equals, migrationfile.FileTxModeFile)
	c.Assert(overriddenDown.Err, qt.IsNil)
}
