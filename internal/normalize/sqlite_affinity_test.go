package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestSQLiteAffinity_FollowsTheEnginesOwnRules pins the five rules SQLite
// documents, in the order it applies them.
//
// The order is the rule rather than an implementation detail: `INT` is tested
// before `CHAR`, so `INTCHAR` is INTEGER and not TEXT. A rule set that tested
// them the other way would disagree with the engine on exactly the names nobody
// expects (stokaro/ptah#2040).
func TestSQLiteAffinity_FollowsTheEnginesOwnRules(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		// Rule 1: anything containing INT.
		{name: "integer", declared: "INTEGER", want: "INTEGER"},
		{name: "int", declared: "INT", want: "INTEGER"},
		{name: "bigint", declared: "BIGINT", want: "INTEGER"},
		{name: "unsigned big int", declared: "UNSIGNED BIG INT", want: "INTEGER"},
		{
			// The order made visible. Rule 2 would answer TEXT.
			name: "int wins over char", declared: "INTCHAR", want: "INTEGER",
		},
		// Rule 2: CHAR, CLOB or TEXT.
		{name: "varchar with a width", declared: "VARCHAR(80)", want: "TEXT"},
		{name: "character varying", declared: "CHARACTER VARYING(80)", want: "TEXT"},
		{name: "text", declared: "TEXT", want: "TEXT"},
		{name: "clob", declared: "CLOB", want: "TEXT"},
		{name: "nvarchar", declared: "NVARCHAR(10)", want: "TEXT"},
		// Rule 3: BLOB, or no declared type at all.
		{name: "blob", declared: "BLOB", want: "BLOB"},
		{name: "no type", declared: "", want: "BLOB"},
		{name: "only spaces", declared: "   ", want: "BLOB"},
		// Rule 4: REAL, FLOA or DOUB.
		{name: "real", declared: "REAL", want: "REAL"},
		{name: "double", declared: "DOUBLE", want: "REAL"},
		{name: "double precision", declared: "DOUBLE PRECISION", want: "REAL"},
		{name: "float", declared: "FLOAT", want: "REAL"},
		// Rule 5: everything else.
		{name: "numeric", declared: "NUMERIC(10,2)", want: "NUMERIC"},
		{name: "decimal", declared: "DECIMAL(10,5)", want: "NUMERIC"},
		{name: "boolean", declared: "BOOLEAN", want: "NUMERIC"},
		{name: "date", declared: "DATE", want: "NUMERIC"},
		{name: "datetime", declared: "DATETIME", want: "NUMERIC"},
		{
			// A name the engine has never heard of. It falls through to
			// NUMERIC there too, which is the one place this answer is weaker
			// than string equality.
			name: "a custom type", declared: "MY_OWN_TYPE", want: "NUMERIC",
		},
		{name: "case does not matter", declared: "varchar(80)", want: "TEXT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(normalize.SQLiteAffinity(test.declared), qt.Equals, test.want)
		})
	}
}

// TestSQLiteAffinity_SeparatesTheTypesTheEngineSeparates is the half that keeps
// the fold from being a blanket silence.
//
// `BOOLEAN` and `INTEGER` are the pair that matters: they look interchangeable
// and SQLite gives them different affinities, so a comparison that folded them
// together would call a real change no change.
func TestSQLiteAffinity_SeparatesTheTypesTheEngineSeparates(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		same  bool
	}{
		{name: "varchar and text are one type", left: "VARCHAR(80)", right: "TEXT", same: true},
		{name: "and so are char and clob", left: "CHAR(4)", right: "CLOB", same: true},
		{name: "boolean is not integer", left: "BOOLEAN", right: "INTEGER"},
		{name: "numeric is not real", left: "NUMERIC(10,2)", right: "REAL"},
		{name: "blob is not text", left: "BLOB", right: "TEXT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			left := normalize.SQLiteAffinity(test.left)
			right := normalize.SQLiteAffinity(test.right)

			c.Assert(left == right, qt.Equals, test.same,
				qt.Commentf("%s -> %s, %s -> %s", test.left, left, test.right, right))
		})
	}
}
