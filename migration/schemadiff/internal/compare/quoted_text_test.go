package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// The comparison folds whitespace and case outside a string and keeps a
// string as written, so where a string ends is the dialect's rule. On
// PostgreSQL and SQLite a backslash is an ordinary character outside an
// E'...' string, and 'p\' is a whole string. On MySQL a backslash escapes the
// next character. Read with the wrong rule, the backslash hides the end of a
// string, and the spaces inside the next string are folded as SQL.

// quotedTextView is one view body on each side, and the dialect that reads
// them.
type quotedTextView struct {
	name     string
	dialect  string
	declared string
	stored   string
}

// TestViewDefinitionsWithDialect_StringsThatDifferAreAChange holds bodies
// whose strings differ to a body change.
func TestViewDefinitionsWithDialect_StringsThatDifferAreAChange(t *testing.T) {
	tests := []quotedTextView{
		{
			name: "whitespace in a literal", dialect: platform.Postgres,
			declared: "SELECT t || ' a b' AS x FROM t", stored: "SELECT t || ' a  b' AS x FROM t",
		},
		{
			name: "whitespace in a literal after a backslash", dialect: platform.Postgres,
			declared: `SELECT t || 'p\' || ' a b' AS x FROM t`, stored: `SELECT t || 'p\' || ' a  b' AS x FROM t`,
		},
		{
			name: "whitespace in a literal after an escape string", dialect: platform.Postgres,
			declared: `SELECT t || E'p\'' || ' a b' AS x FROM t`, stored: `SELECT t || E'p\'' || ' a  b' AS x FROM t`,
		},
		{
			name: "an alias spelled inside a literal", dialect: platform.Postgres,
			declared: "SELECT t || 'a' AS x FROM t", stored: "SELECT t || 'a as a' AS x FROM t",
		},
		{
			name: "a comparison spelled inside a literal", dialect: platform.Postgres,
			declared: "SELECT '(a = b)' AS x FROM t", stored: "SELECT 'a = b' AS x FROM t",
		},
		{
			name: "whitespace in a literal after an escaped quote", dialect: platform.MySQL,
			declared: `SELECT concat(c, 'it\'s', ' a b') AS x FROM t`, stored: `SELECT concat(c, 'it\'s', ' a  b') AS x FROM t`,
		},
		{
			name: "whitespace in a literal", dialect: platform.SQLite,
			declared: "SELECT t || ' a b' AS x FROM t", stored: "SELECT t || ' a  b' AS x FROM t",
		},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: test.declared},
				catalog.View{Name: "v", Body: test.stored},
				test.dialect,
			)

			c.Assert(diff.Changes["body"], qt.Not(qt.Equals), "")
		})
	}
}

// TestViewDefinitionsWithDialect_OneBodyIsNoChange holds bodies that differ
// only outside their strings to no change.
func TestViewDefinitionsWithDialect_OneBodyIsNoChange(t *testing.T) {
	tests := []quotedTextView{
		{
			name: "whitespace after a literal ending in a backslash", dialect: platform.Postgres,
			declared: `SELECT t || 'p\' ||  ' a  b' AS x FROM t`, stored: `SELECT t || 'p\' || ' a  b' AS x FROM t`,
		},
		{
			name: "case after an escape string", dialect: platform.Postgres,
			declared: `SELECT t || E'p\'' || ' A  b' AS x FROM T`, stored: `select t || E'p\'' || ' A  b' as x from t`,
		},
		{
			name: "whitespace after an escaped quote", dialect: platform.MySQL,
			declared: `SELECT concat(c,  'it\'s  x') AS x FROM t`, stored: `SELECT concat(c, 'it\'s  x') AS x FROM t`,
		},
		{
			name: "an alias that repeats the column", dialect: platform.Postgres,
			declared: "SELECT t FROM t", stored: "SELECT t AS t FROM t",
		},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: test.declared},
				catalog.View{Name: "v", Body: test.stored},
				test.dialect,
			)

			c.Assert(diff.Changes, qt.HasLen, 0)
		})
	}
}

// quotedTextCheck is one CHECK on each side, and the dialect that reads them.
type quotedTextCheck struct {
	name     string
	dialect  string
	declared string
	stored   string
}

// quotedTextCheckDiff compares one table holding the declared CHECK with one
// holding the stored clause, with no server spelling.
func quotedTextCheckDiff(check quotedTextCheck) *difftypes.SchemaDiff {
	declared := &schemamodel.Database{
		Tables:      []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields:      []schemamodel.Field{{StructName: "T", Name: "c", Type: "TEXT"}},
		Constraints: []schemamodel.Constraint{{StructName: "T", Name: "k", Table: "t", Type: "CHECK", CheckExpression: check.declared}},
	}
	live := &catalog.Database{
		Tables:      []catalog.Table{{Name: "t"}},
		Constraints: []catalog.Constraint{{Name: "k", TableName: "t", Type: "CHECK", CheckClause: new(check.stored)}},
	}
	diff := &difftypes.SchemaDiff{}
	compare.ConstraintsWithSemantics(declared, live, diff,
		&config.CompareOptions{Dialect: check.dialect}, identifier.ForDialect(check.dialect))
	return diff
}

// TestConstraints_CheckStringsThatDifferAreAChange holds CHECK clauses whose
// strings differ to a replaced constraint.
func TestConstraints_CheckStringsThatDifferAreAChange(t *testing.T) {
	tests := []quotedTextCheck{
		{name: "a backslash ending a literal", dialect: platform.Postgres, declared: "c <> 'p'", stored: `(c <> 'p\'::text)`},
		{name: "a backslash ending a literal", dialect: platform.SQLite, declared: "c <> 'p'", stored: `c <> 'p\'`},
		{name: "a backslash in an escape string", dialect: platform.Postgres, declared: `c <> E'p\\'`, stored: `c <> E'p\''`},
		{name: "another literal", dialect: platform.MySQL, declared: "c <> 'abd'", stored: "(`c` <> _utf8mb4'abc')"},
		{name: "a quote against none", dialect: platform.MySQL, declared: "c <> 'its'", stored: "(`c` <> _utf8mb4'it\\'s')"},
		{name: "one backslash against two", dialect: platform.MySQL, declared: `c <> 'p\\'`, stored: "(`c` <> _utf8mb4'p\\\\\\\\')"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := quotedTextCheckDiff(test)

			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"k"})
		})
	}
}

// TestConstraints_OneCheckStringIsNoChange holds CHECK clauses that differ
// only outside their strings, or in how a string spells its value, to no
// change. The MySQL and MariaDB rows hold the clause as the reader reports it.
func TestConstraints_OneCheckStringIsNoChange(t *testing.T) {
	tests := []quotedTextCheck{
		{name: "a backslash ending a literal", dialect: platform.Postgres, declared: `c <> 'p\'`, stored: `(c <> 'p\'::text)`},
		{name: "a literal after a backslash", dialect: platform.Postgres, declared: `c <> 'p\' ||  ' a  b'`, stored: `(c <> 'p\' || ' a  b')`},
		{name: "an escaped quote in an escape string", dialect: platform.Postgres, declared: `c <>  E'p\'' ||  ' a  b'`, stored: `c <> E'p\'' || ' a  b'`},
		{name: "a character set introducer", dialect: platform.MySQL, declared: "c <> 'abc'", stored: "(`c` <> _utf8mb4'abc')"},
		{name: "a doubled quote", dialect: platform.MySQL, declared: "c <> 'it''s'", stored: "(`c` <> _utf8mb4'it\\'s')"},
		{name: "an escaped backslash", dialect: platform.MySQL, declared: `c <> 'p\\'`, stored: "(`c` <> _utf8mb4'p\\\\')"},
		{name: "an escaped tab", dialect: platform.MySQL, declared: `c <> 'x\ty'`, stored: "(`c` <> _utf8mb4'x\ty')"},
		{name: "a double-quoted string", dialect: platform.MySQL, declared: `c <> "d'q"`, stored: "(`c` <> _utf8mb4'd\\'q')"},
		{name: "a backslash before a percent sign", dialect: platform.MariaDB, declared: `c <> 'x\%y'`, stored: "`c` <> 'x\\\\%y'"},
		{name: "an escaped quote that ends a value", dialect: platform.MariaDB, declared: "c <> 'x'''", stored: "`c` <> 'x\\''"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := quotedTextCheckDiff(test)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
		})
	}
}

// TestTriggerDefinitions_ConditionStringsThatDifferAreAChange holds WHEN
// conditions whose strings differ to a condition change.
func TestTriggerDefinitions_ConditionStringsThatDifferAreAChange(t *testing.T) {
	c := qt.New(t)

	diff := compare.TriggerDefinitions(
		clauseTrigger{event: "UPDATE", forEach: "ROW", when: "(new.t <> 'p')"}.declared(),
		clauseTrigger{event: "UPDATE", forEach: "ROW", when: `(new.t <> 'p\'::text)`}.read(),
		platform.Postgres,
		identifier.ForDialect(platform.Postgres),
	)

	c.Assert(diff.Changes["when"], qt.Not(qt.Equals), "")
}

// TestTriggerDefinitions_BodyStringsThatDifferAreAChange holds SQLite trigger
// bodies whose strings differ to a body change.
func TestTriggerDefinitions_BodyStringsThatDifferAreAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{
			name:     "whitespace in a literal",
			declared: "BEGIN INSERT INTO log (m) VALUES (NEW.t || ' a b'); END",
			stored:   "BEGIN INSERT INTO log (m) VALUES (NEW.t || ' a  b'); END",
		},
		{
			name:     "whitespace in a literal after a backslash",
			declared: `BEGIN INSERT INTO log (m) VALUES (NEW.t || 'x\' || ' a b'); END`,
			stored:   `BEGIN INSERT INTO log (m) VALUES (NEW.t || 'x\' || ' a  b'); END`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				schemamodel.Trigger{Name: "tr", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: test.declared},
				catalog.Trigger{Name: "tr", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: test.stored},
				platform.SQLite,
				identifier.ForDialect(platform.SQLite),
			)

			c.Assert(diff.Changes["body"], qt.Not(qt.Equals), "")
		})
	}
}
