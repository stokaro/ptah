package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/parser"
)

// parsedUniqueTable parses one table-body declaration for a named dialect and
// returns the table node.
//
// parsedTable is the same thing pinned to MySQL, which is what most of this
// package needs; here the dialect is the axis under test, so it comes in as a
// row.
func parsedUniqueTable(c *qt.C, dialect, declaration string) *ast.CreateTableNode {
	c.Helper()

	statements, err := parser.NewParser(
		uniqueKeyTable(declaration),
		parser.WithDialect(dialect),
	).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)

	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	return table
}

// TestParse_MySQLUniqueKeyNameSpellings_HappyPath covers stokaro/ptah#2776.
//
// The MySQL-family grammar is `UNIQUE [INDEX|KEY] [name] (key_part, ...)`, and
// the two optional parts are optional independently. The name was read only
// inside the KEY branch, so `UNIQUE uq (a)` -- a name with no keyword before it
// -- was refused outright with `expected '(' for constraint columns`.
//
// `USING {BTREE|HASH}` is read and skipped in the same place, because it can
// stand exactly where the name goes. A reader taking the next identifier for a
// name would call the index in `UNIQUE USING BTREE (a)` by the name `USING`,
// which is a silent rename rather than a refusal.
//
// Every row was measured on MySQL 26.7 and MariaDB 12.3, and both servers agree
// on all of them. The name column is the index name the catalog reports, except
// where it is empty:
//
//	UNIQUE uq (a)                  index uq
//	UNIQUE (a)                     index a
//	UNIQUE USING BTREE (a)         index a       (USING is not a name)
//	UNIQUE uq USING BTREE (a)      index uq
//	UNIQUE KEY uq (a)              index uq
//	UNIQUE KEY (a)                 index a
//	UNIQUE INDEX uq (a)            index uq
//	UNIQUE KEY uq USING BTREE (a)  index uq
//
// An empty name here is not a missing one. Both servers name an unnamed index
// after its first key part, and stokaro/ptah#2713 put that rule in toschema,
// where the dialect is known; a name invented in the parser would put a
// reader's guess where the server's answer belongs. That the derived name is
// the server's own is measured live by
// TestUnnamedInlineUniqueKeyTakesTheNameItsServerWouldGiveIt.
//
// Only BTREE appears above, and that is deliberate rather than an omission.
// Ptah discards the access method, which is invisible for BTREE -- it is the
// default on both engines -- but NOT for HASH: measured on MariaDB 12.3 InnoDB,
// `UNIQUE KEY uq USING HASH (a)` reports `INDEX_TYPE = HASH` and comes back out
// of SHOW CREATE TABLE with the clause intact, while MySQL 26.7 normalizes the
// same declaration to BTREE. The name is read the same way for either spelling,
// which is what this test asserts; what a discarded HASH costs is a question
// for the renderer and the comparison, not for this reader.
func TestParse_MySQLUniqueKeyNameSpellings_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		wantName    string
	}{
		{name: "a bare name", declaration: "UNIQUE uq (a)", wantName: "uq"},
		{name: "no name at all", declaration: "UNIQUE (a)", wantName: ""},
		{name: "USING alone is not a name", declaration: "UNIQUE USING BTREE (a)", wantName: ""},
		{name: "a bare name before USING", declaration: "UNIQUE uq USING BTREE (a)", wantName: "uq"},
		{name: "KEY and a name", declaration: "UNIQUE KEY uq (a)", wantName: "uq"},
		{name: "KEY and no name", declaration: "UNIQUE KEY (a)", wantName: ""},
		{name: "INDEX and a name", declaration: "UNIQUE INDEX uq (a)", wantName: "uq"},
		{name: "KEY, a name and USING", declaration: "UNIQUE KEY uq USING BTREE (a)", wantName: "uq"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				table := parsedUniqueTable(c, dialect, test.declaration)

				c.Assert(table.Indexes, qt.HasLen, 0)
				c.Assert(constraintTypes(table), qt.DeepEquals, []ast.ConstraintType{ast.UniqueConstraint})
				c.Assert(table.Constraints[0].Name, qt.Equals, test.wantName)
				c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
			})
		}
	}
}

// TestParse_UniqueKeyNameAfterKeyOrIndexNeedsNoDialect is one of the two
// regressions this change introduced and reverted, kept so it cannot come back.
//
// The bare name is MySQL-family grammar and is gated on the dialect. Gating the
// name after KEY or INDEX along with it looks like the same rule and is not:
// after those keywords no dialect puts a keyword where the name goes, so that
// name is read whatever the document was declared as.
//
// The existing suite caught it. TestParser_ParseMariaDBComprehensiveDemo builds
// its parser with `parser.NewParser(sql)` and no dialect at all, and it
// declares `UNIQUE KEY `uq_username` (`username`)`; with the gate too wide,
// that name stopped being read. A classifying read has no dialect by
// construction, so this is the ordinary case rather than an odd one, and it
// is asserted here directly rather than left to a fixture whose subject is
// fourteen other things.
func TestParse_UniqueKeyNameAfterKeyOrIndexNeedsNoDialect(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
	}{
		{name: "KEY", declaration: "UNIQUE KEY uq (a)"},
		{name: "INDEX", declaration: "UNIQUE INDEX uq (a)"},
		{name: "KEY and USING", declaration: "UNIQUE KEY uq USING BTREE (a)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedUniqueTable(c, "", test.declaration)

			c.Assert(constraintTypes(table), qt.DeepEquals, []ast.ConstraintType{ast.UniqueConstraint})
			c.Assert(table.Constraints[0].Name, qt.Equals, "uq")
			c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
		})
	}
}

// TestParse_BareUniqueNameIsMySQLFamilyOnly_FailurePath is the far side of that
// gate, and without it nothing here would fail if the gate were deleted.
//
// `UNIQUE uq (a)` is MySQL-family spelling. Reading a bare identifier after
// UNIQUE for every dialect would accept it everywhere, and the position is not
// free elsewhere: PostgreSQL writes `UNIQUE NULLS [NOT] DISTINCT (col)` there.
// The keyword list in the reader stops that one particular word, which is
// exactly the trouble -- a list is not a dialect, and the next reader to add a
// clause would find the name grammar had already eaten it.
//
// The rows assert only that the declaration is refused rather than the sentence
// it is refused with: what makes this a control is that the bare name does not
// reach a dialect that has no such grammar, and the diagnostic belongs to the
// column-list reader that stops afterwards.
func TestParse_BareUniqueNameIsMySQLFamilyOnly_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "no dialect", dialect: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(
				uniqueKeyTable("UNIQUE uq (a)"),
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParse_NullsIsNotReadAsAUniqueKeyName_FailurePath is the second regression
// this change introduced and reverted.
//
// The MySQL family refuses `NULLS [NOT] DISTINCT` on purpose
// (stokaro/ptah#2788): it is PostgreSQL's clause, both servers answer error
// 1064 to it, and accepting it would mean rendering a constraint with different
// null-equality semantics. Reading the identifier after UNIQUE as a name
// consumed NULLS before that refusal was reached, so the parse walked straight
// past it -- the refusal was not weakened, it was skipped.
//
// nulls_distinct_dialect_test.go owns the clause and measures the refusal, and
// this row is deliberately one of its rows. It is here as well because the
// sentence it asserts is the one the NAME grammar can silently delete, and the
// file that would delete it is this one; a reader changing the name rules has
// no reason to open the other file.
func TestParse_NullsIsNotReadAsAUniqueKeyName_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(
				uniqueKeyTable("UNIQUE NULLS NOT DISTINCT (a)"),
				parser.WithDialect(dialect),
			).Parse()

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, dialect+" does not support the NULLS NOT DISTINCT clause")
		})
	}
}

// TestParse_NullsDistinctSurvivesTheNameGrammarOnPostgres_HappyPath is the
// other half of that word, on the dialect that has the clause.
//
// PostgreSQL is where `UNIQUE NULLS NOT DISTINCT (a)` is legal, so a refusal
// cannot be what protects it here -- only the clause still reaching the model
// says the name grammar left it alone. The constraint's NAME is asserted beside
// its NullsDistinct, and that is the assertion the other tests cannot make:
// a reader that took NULLS for a name would leave the clause unread AND call
// the constraint `NULLS`, and asserting the flag alone would not say which
// happened.
func TestParse_NullsDistinctSurvivesTheNameGrammarOnPostgres_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		want        bool
	}{
		{name: "not distinct", declaration: "UNIQUE NULLS NOT DISTINCT (a)", want: false},
		{name: "distinct", declaration: "UNIQUE NULLS DISTINCT (a)", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedUniqueTable(c, platform.Postgres, test.declaration)

			c.Assert(constraintTypes(table), qt.DeepEquals, []ast.ConstraintType{ast.UniqueConstraint})
			c.Assert(table.Constraints[0].Name, qt.Equals, "")
			c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
			c.Assert(table.Constraints[0].NullsDistinct, qt.IsNotNil)
			c.Assert(*table.Constraints[0].NullsDistinct, qt.Equals, test.want)
		})
	}
}
