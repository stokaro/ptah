package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/txrequire"
)

func analyze(sql ...string) txrequire.Result {
	statements := make([]txrequire.Statement, 0, len(sql))
	for index, statement := range sql {
		statements = append(statements, txrequire.Statement{Index: index, Line: index + 1, SQL: statement})
	}
	return txrequire.Analyze(platform.Postgres, capability.Postgres16(), statements)
}

// TestAnalyze_ConcurrentIndexAlwaysNeedsAutocommit pins the unconditional rule.
//
// Measured on PostgreSQL 18.4: `BEGIN; CREATE INDEX CONCURRENTLY i ON t (c);`
// is `ERROR: CREATE INDEX CONCURRENTLY cannot run inside a transaction block`.
func TestAnalyze_ConcurrentIndexAlwaysNeedsAutocommit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "create", sql: "CREATE INDEX CONCURRENTLY i ON t (c)"},
		{name: "create unique", sql: "CREATE UNIQUE INDEX CONCURRENTLY i ON t (c)"},
		{name: "drop", sql: "DROP INDEX CONCURRENTLY i"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := analyze("CREATE TABLE t (c int)", test.sql)

			c.Assert(result.RequiresAutocommit(), qt.IsTrue)
			c.Assert(result.Findings, qt.HasLen, 1)
			c.Assert(result.Findings[0].Reason, qt.Equals, txrequire.ReasonConcurrentIndex)
			c.Assert(result.Findings[0].Remedy, qt.Contains, "no_transaction")
		})
	}
}

// TestAnalyze_AnOrdinaryIndexIsTransactional is the control. A rule that
// answered "needs autocommit" for every CREATE INDEX would satisfy the row
// above and would refuse most migrations ever written.
func TestAnalyze_AnOrdinaryIndexIsTransactional(t *testing.T) {
	c := qt.New(t)

	result := analyze("CREATE TABLE t (c int)", "CREATE INDEX i ON t (c)")

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_AConcurrentlyLiteralIsNotAKeyword pins that the scan reads
// tokens rather than text.
//
// The literal keeps its quotes through tokenization, so a value spelled
// CONCURRENTLY cannot impersonate the clause.
func TestAnalyze_AConcurrentlyLiteralIsNotAKeyword(t *testing.T) {
	c := qt.New(t)

	result := analyze("INSERT INTO notes (body) VALUES ('CREATE INDEX CONCURRENTLY i ON t (c)')")

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_AddingToAPreExistingEnumAndUsingItNeedsAutocommit pins the rule
// the file-level analysis exists for.
//
// Measured: `BEGIN; ALTER TYPE mood ADD VALUE 'x'; SELECT 'x'::mood;` is
// `ERROR: unsafe use of new value "x" of enum type mood`, with the hint that
// new enum values must be committed before they can be used.
func TestAnalyze_AddingToAPreExistingEnumAndUsingItNeedsAutocommit(t *testing.T) {
	c := qt.New(t)

	result := analyze(
		"ALTER TYPE mood ADD VALUE 'great'",
		"CREATE TABLE t (m mood DEFAULT 'great')",
	)

	c.Assert(result.RequiresAutocommit(), qt.IsTrue)
	c.Assert(result.Findings[0].Reason, qt.Equals, txrequire.ReasonEnumValueUsed)
	// The statement reported is the one PostgreSQL refuses -- the USE, not the
	// ALTER -- because that is the line an operator has to look at.
	c.Assert(result.Findings[0].Statement.Index, qt.Equals, 1)
	c.Assert(result.Findings[0].Message, qt.Contains, "'great'")
	// Named as the author spelled it. A diagnostic that renames the object is
	// worse than one that omits the name.
	c.Assert(result.Findings[0].Message, qt.Contains, "mood")
	c.Assert(result.Findings[0].Message, qt.Not(qt.Contains), "MOOD")
}

// TestAnalyze_AddingToAnEnumTheFileCreatesIsTransactional is the exception
// that keeps a valid workflow working, and the reason a keyword-only check
// would be wrong.
//
// Measured: `BEGIN; CREATE TYPE m AS ENUM ('a'); ALTER TYPE m ADD VALUE 'b';
// CREATE TABLE t (c m DEFAULT 'b'); COMMIT;` is accepted. PostgreSQL allows
// the new value immediately when the type itself is new in the transaction.
func TestAnalyze_AddingToAnEnumTheFileCreatesIsTransactional(t *testing.T) {
	c := qt.New(t)

	result := analyze(
		"CREATE TYPE m AS ENUM ('a')",
		"ALTER TYPE m ADD VALUE 'b'",
		"CREATE TABLE t (c m DEFAULT 'b')",
	)

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_AddingAValueWithoutUsingItIsTransactional pins the other half of
// the same measurement: the ALTER alone is accepted inside a transaction on
// PostgreSQL 12 and later, so adding a value and stopping there is not a
// reason to leave the transaction.
func TestAnalyze_AddingAValueWithoutUsingItIsTransactional(t *testing.T) {
	c := qt.New(t)

	result := analyze("ALTER TYPE mood ADD VALUE 'great'", "CREATE TABLE unrelated (id int)")

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_PositioningAValueAfterAJustAddedOneIsNotAUse pins that the scan
// distinguishes adding from using, on the fixture where the two are hardest to
// tell apart: the second ALTER names the value the first added.
//
// Measured on PostgreSQL 18.4: `BEGIN; ALTER TYPE af ADD VALUE 'great';
// ALTER TYPE af ADD VALUE 'grand' AFTER 'great'; COMMIT;` is accepted, and the
// ordering takes effect. Reading that as a use would refuse a valid file.
func TestAnalyze_PositioningAValueAfterAJustAddedOneIsNotAUse(t *testing.T) {
	c := qt.New(t)

	result := analyze(
		"ALTER TYPE mood ADD VALUE 'great'",
		"ALTER TYPE mood ADD VALUE 'grand' AFTER 'great'",
	)

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_ABareWordAfterValueIsNotTracked pins that only a quoted literal
// becomes a tracked value.
//
// The direction matters: a bare word entering the tracked set would make every
// later statement containing that common word a false refusal, and a preflight
// that blocks a valid migration is worse than one that misses an invalid one.
func TestAnalyze_ABareWordAfterValueIsNotTracked(t *testing.T) {
	c := qt.New(t)

	result := analyze("ALTER TYPE mood ADD VALUE NULL", "SELECT NULL")

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_ACommentDoesNotShiftTheNamePosition pins why comments are
// dropped before the scan.
//
// It is not that a clause named in prose could be read as one written -- the
// lexer already makes a comment one opaque token, which can never equal a
// keyword or a literal. It is that the type name is read by POSITION: with the
// comment left in, `CREATE TYPE /* c */ m` puts the comment where the name
// should be, the file's own type goes unregistered, and a valid enum workflow
// is refused.
func TestAnalyze_ACommentDoesNotShiftTheNamePosition(t *testing.T) {
	c := qt.New(t)

	result := analyze(
		"CREATE TYPE /* the mood of a row */ m AS ENUM ('a')",
		"ALTER TYPE m ADD VALUE 'b'",
		"CREATE TABLE t (c m DEFAULT 'b')",
	)

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_AddValueIfNotExistsIsRead pins the guarded spelling, which puts
// three words between VALUE and the literal.
func TestAnalyze_AddValueIfNotExistsIsRead(t *testing.T) {
	c := qt.New(t)

	result := analyze(
		"ALTER TYPE mood ADD VALUE IF NOT EXISTS 'great'",
		"CREATE TABLE t (m mood DEFAULT 'great')",
	)

	c.Assert(result.RequiresAutocommit(), qt.IsTrue)
	c.Assert(result.Findings[0].Reason, qt.Equals, txrequire.ReasonEnumValueUsed)
}

// TestAnalyze_ATargetWithoutConcurrentIndexesIsNotToldItHasOne pins the
// capability gate.
//
// The key decides, not the dialect name: a preset that declines concurrent
// indexes cannot produce the statement, and reporting it would answer about
// something that target refuses for its own reasons.
func TestAnalyze_ATargetWithoutConcurrentIndexesIsNotToldItHasOne(t *testing.T) {
	c := qt.New(t)
	without := capability.Postgres16().
		With(capability.CreateIndexConcurrently, false).
		With(capability.DropIndexConcurrently, false)
	statements := []txrequire.Statement{
		{Index: 0, Line: 1, SQL: "CREATE TABLE t (c int)"},
		{Index: 1, Line: 2, SQL: "CREATE INDEX CONCURRENTLY i ON t (c)"},
	}

	result := txrequire.Analyze(platform.Postgres, without, statements)

	c.Assert(result.RequiresAutocommit(), qt.IsFalse)
}

// TestAnalyze_TheCapabilityIsWhatDecides is that gate's control: with the key
// on, the same statements are reported.
func TestAnalyze_TheCapabilityIsWhatDecides(t *testing.T) {
	c := qt.New(t)

	result := analyze("CREATE TABLE t (c int)", "CREATE INDEX CONCURRENTLY i ON t (c)")

	c.Assert(result.RequiresAutocommit(), qt.IsTrue)
}
