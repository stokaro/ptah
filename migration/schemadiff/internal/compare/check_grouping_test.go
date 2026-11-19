package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestConstraints_ACheckTheServerRegroupsIsNoChange holds a declared CHECK
// and the clause MySQL 26.10 or MariaDB 12.3 stores for it to no change. The
// clause is what the MySQL reader reports: CHECK_CLAUSE with MySQL's extra
// escaping and encoding taken off.
func TestConstraints_ACheckTheServerRegroupsIsNoChange(t *testing.T) {
	tests := []quotedTextCheck{
		{
			name: "BETWEEN inside AND", dialect: platform.MySQL,
			declared: "id between 1 and 10 and c is not null",
			stored:   "((`id` between 1 and 10) and (`c` is not null))",
		},
		{name: "NOT LIKE", dialect: platform.MySQL, declared: "c NOT LIKE 'x%'", stored: "(not((`c` like _utf8mb4'x%')))"},
		{name: "NOT LIKE", dialect: platform.MariaDB, declared: "c NOT LIKE 'x%'", stored: "`c`  not like 'x%'"},
		{name: "X'41'", dialect: platform.MySQL, declared: "b <> X'41'", stored: "(`b` <> 0x41)"},
		{name: "an introducer apart from its string", dialect: platform.MySQL, declared: "b <> _binary 'ab'", stored: "(`b` <> _binary'ab')"},
		{name: "an introducer apart from its string", dialect: platform.MariaDB, declared: "b <> _binary 'ab'", stored: "`b` <> _binary'ab'"},
		{name: "NOT over AND", dialect: platform.MySQL, declared: "NOT (a = 1 AND b = 2)", stored: "((`a` <> 1) or (`b` <> 2))"},
		{name: "NOT over a column", dialect: platform.MySQL, declared: "NOT a OR b > 0", stored: "((0 = `a`) or (`b` > 0))"},
		{name: "NOT over a column", dialect: platform.MariaDB, declared: "NOT a OR b > 0", stored: "`a` = 0 or `b` > 0"},
		{name: "IN over one member", dialect: platform.MySQL, declared: "a IN (1)", stored: "(`a` = 1)"},
		{name: "a string beyond ASCII", dialect: platform.MySQL, declared: "c <> 'héllo ☃'", stored: "(`c` <> _utf8mb4'héllo ☃')"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := quotedTextCheckDiff(test)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
		})
	}
}

// TestConstraints_ACheckWithAnotherValueIsAChange holds each regrouped form
// of [TestConstraints_ACheckTheServerRegroupsIsNoChange] with one value
// changed to a replaced constraint, so reading the grouping cannot hide an
// edit.
func TestConstraints_ACheckWithAnotherValueIsAChange(t *testing.T) {
	tests := []quotedTextCheck{
		{
			name: "BETWEEN inside AND", dialect: platform.MySQL,
			declared: "id between 1 and 11 and c is not null",
			stored:   "((`id` between 1 and 10) and (`c` is not null))",
		},
		{name: "NOT LIKE against LIKE", dialect: platform.MySQL, declared: "c LIKE 'x%'", stored: "(not((`c` like _utf8mb4'x%')))"},
		{name: "NOT LIKE against LIKE", dialect: platform.MariaDB, declared: "c LIKE 'x%'", stored: "`c`  not like 'x%'"},
		{name: "X'41'", dialect: platform.MySQL, declared: "b <> X'42'", stored: "(`b` <> 0x41)"},
		{name: "an introducer apart from another string", dialect: platform.MySQL, declared: "b <> _binary 'ac'", stored: "(`b` <> _binary'ab')"},
		{name: "an introducer apart from another string", dialect: platform.MariaDB, declared: "b <> _binary 'ac'", stored: "`b` <> _binary'ab'"},
		{name: "NOT over AND against AND", dialect: platform.MySQL, declared: "a = 1 AND b = 2", stored: "((`a` <> 1) or (`b` <> 2))"},
		{name: "NOT over a column against the column", dialect: platform.MySQL, declared: "a OR b > 0", stored: "((0 = `a`) or (`b` > 0))"},
		{name: "IN over two members", dialect: platform.MySQL, declared: "a IN (1, 2)", stored: "(`a` = 1)"},
		{name: "a string beyond ASCII", dialect: platform.MySQL, declared: "c <> 'héllo ☂'", stored: "(`c` <> _utf8mb4'héllo ☃')"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := quotedTextCheckDiff(test)

			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"k"})
		})
	}
}

// generatedText is one generated column's expression on each side, and the
// dialect that reads them.
type generatedText struct {
	name     string
	dialect  string
	declared string
	stored   string
}

// generatedTextDiff compares a declared virtual column with the expression
// the reader reported for it.
func generatedTextDiff(test generatedText) map[string]string {
	return compare.ColumnsWithDialect(
		schemamodel.Field{Name: "d", Type: "varchar(40)", Nullable: true, GeneratedExpression: test.declared, GeneratedKind: "VIRTUAL"},
		catalog.Column{Name: "d", DataType: "varchar", ColumnType: "varchar(40)", IsNullable: "YES", GeneratedKind: "VIRTUAL", GeneratedExpression: new(test.stored)},
		test.dialect,
	).Changes
}

// TestColumns_AGeneratedExpressionHoldingAStringIsNoChange holds a declared
// expression and the one MySQL 26.10 or MariaDB 12.3 stores for it, as the
// reader reports it, to no change.
func TestColumns_AGeneratedExpressionHoldingAStringIsNoChange(t *testing.T) {
	tests := []generatedText{
		{name: "a string", dialect: platform.MySQL, declared: "concat(c, 'x')", stored: "concat(`c`,_utf8mb4'x')"},
		{name: "a quote", dialect: platform.MySQL, declared: "concat(c, 'it''s')", stored: "concat(`c`,_utf8mb4'it\\'s')"},
		{name: "a quote", dialect: platform.MariaDB, declared: "concat(c, 'it''s')", stored: "concat(`c`,'it\\'s')"},
		{name: "a string beyond ASCII", dialect: platform.MySQL, declared: "concat(c, 'é☃')", stored: "concat(`c`,_utf8mb4'é☃')"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(generatedTextDiff(test), qt.HasLen, 0)
		})
	}
}

// TestColumns_AGeneratedExpressionWithAnotherStringIsAChange holds each
// expression of [TestColumns_AGeneratedExpressionHoldingAStringIsNoChange]
// with its string changed to a change of the column.
func TestColumns_AGeneratedExpressionWithAnotherStringIsAChange(t *testing.T) {
	tests := []generatedText{
		{name: "a string", dialect: platform.MySQL, declared: "concat(c, 'y')", stored: "concat(`c`,_utf8mb4'x')"},
		{name: "a quote", dialect: platform.MySQL, declared: "concat(c, 'its')", stored: "concat(`c`,_utf8mb4'it\\'s')"},
		{name: "a quote", dialect: platform.MariaDB, declared: "concat(c, 'its')", stored: "concat(`c`,'it\\'s')"},
		{name: "a string beyond ASCII", dialect: platform.MySQL, declared: "concat(c, 'é☂')", stored: "concat(`c`,_utf8mb4'é☃')"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(generatedTextDiff(test)["generated"], qt.Not(qt.Equals), "")
		})
	}
}
