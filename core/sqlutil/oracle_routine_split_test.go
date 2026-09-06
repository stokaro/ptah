package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
)

// TestSplitSQLStatements_KeepsAnOraclePLSQLRoutineWhole pins the boundary a
// PL/SQL body needs, and the declaration section is the case that decides it.
//
// The header of `FUNCTION f RETURN NUMBER IS x NUMBER := 0; BEGIN ... END;`
// ends at IS, and what follows the semicolon after the declaration is still the
// same statement. Waiting for BEGIN split that routine into four fragments,
// each of which the server refuses on its own.
//
// The trigger row is here because it is the shape that already worked: a
// trigger header carries no IS, its body is opened by BEGIN, and it must keep
// splitting the way it did before Oracle routines were rendered at all.
func TestSplitSQLStatements_KeepsAnOraclePLSQLRoutineWhole(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a function whose body starts at BEGIN",
			sql:  "CREATE OR REPLACE FUNCTION f(p IN NUMBER) RETURN NUMBER IS\nBEGIN\n  RETURN p * 2;\nEND;",
			want: []string{"CREATE OR REPLACE FUNCTION f(p IN NUMBER) RETURN NUMBER IS\nBEGIN\n  RETURN p * 2;\nEND;"},
		},
		{
			name: "a function with a declaration section",
			sql:  "CREATE OR REPLACE FUNCTION f RETURN NUMBER IS\n  x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;",
			want: []string{"CREATE OR REPLACE FUNCTION f RETURN NUMBER IS\n  x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;"},
		},
		{
			name: "a procedure",
			sql:  "CREATE OR REPLACE PROCEDURE p(a IN NUMBER) IS\nBEGIN\n  NULL;\nEND;",
			want: []string{"CREATE OR REPLACE PROCEDURE p(a IN NUMBER) IS\nBEGIN\n  NULL;\nEND;"},
		},
		{
			name: "a body carrying END IF, which does not close the routine",
			sql: "CREATE OR REPLACE FUNCTION f(p IN NUMBER) RETURN NUMBER IS\nBEGIN\n" +
				"  IF p > 0 THEN\n    RETURN 1;\n  END IF;\n  RETURN 0;\nEND;",
			want: []string{"CREATE OR REPLACE FUNCTION f(p IN NUMBER) RETURN NUMBER IS\nBEGIN\n" +
				"  IF p > 0 THEN\n    RETURN 1;\n  END IF;\n  RETURN 0;\nEND;"},
		},
		{
			name: "a nested block, whose END does not close the routine either",
			sql: "CREATE OR REPLACE FUNCTION f RETURN NUMBER IS\nBEGIN\n  BEGIN\n    NULL;\n  END;\n" +
				"  RETURN 1;\nEND;",
			want: []string{"CREATE OR REPLACE FUNCTION f RETURN NUMBER IS\nBEGIN\n  BEGIN\n    NULL;\n  END;\n" +
				"  RETURN 1;\nEND;"},
		},
		{
			name: "the statement after a routine is its own",
			sql: "CREATE OR REPLACE FUNCTION f RETURN NUMBER IS\n  x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;\n" +
				"DROP FUNCTION g;",
			want: []string{
				"CREATE OR REPLACE FUNCTION f RETURN NUMBER IS\n  x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;",
				"DROP FUNCTION g",
			},
		},
		{
			// A trigger keeps its terminator for the same reason a function
			// does, and it reaches the body through BEGIN rather than through
			// IS. Measured on 23.26.2.0.0: a trigger created without the
			// semicolon after END is INVALID, the driver reports nothing, and
			// USER_TRIGGERS reports it ENABLED -- so nothing downstream sees
			// that it will never fire.
			name: "a trigger, whose body is opened by BEGIN",
			sql: "CREATE TRIGGER t BEFORE INSERT ON items FOR EACH ROW\nBEGIN\n" +
				"  :NEW.n := 1;\nEND;",
			want: []string{"CREATE TRIGGER t BEFORE INSERT ON items FOR EACH ROW\nBEGIN\n  :NEW.n := 1;\nEND;"},
		},
		{
			// A trigger header carries a standalone AS of its own, in
			// REFERENCING, and it opens nothing. A CALL body has no BEGIN and
			// no END, so a rule that treated that AS as the body opener would
			// keep every semicolon after it and swallow the rest of the file.
			name: "a trigger whose REFERENCING clause carries AS, with a CALL body",
			sql: "CREATE TRIGGER t AFTER INSERT ON items REFERENCING NEW AS n FOR EACH ROW " +
				"CALL log_it();\nDROP TABLE audit;",
			want: []string{
				"CREATE TRIGGER t AFTER INSERT ON items REFERENCING NEW AS n FOR EACH ROW CALL log_it()",
				"DROP TABLE audit",
			},
		},
		{
			name: "an ordinary statement pair still splits",
			sql:  "CREATE TABLE t (id NUMBER);\nCREATE INDEX i ON t (id);",
			want: []string{"CREATE TABLE t (id NUMBER)", "CREATE INDEX i ON t (id)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(test.sql, platform.Oracle), qt.DeepEquals, test.want)
		})
	}
}

// TestSplitSQLStatements_TheOracleRoutineOpenerIsOracleOnly is the control the
// test above needs.
//
// IS and AS open a body in PL/SQL and not in the other dialects this splitter
// serves. A rule applied everywhere would swallow the statements after any
// `CREATE FUNCTION ... AS` on a target where that is not a body opener, and the
// evidence would be a migration that stopped applying half its file.
func TestSplitSQLStatements_TheOracleRoutineOpenerIsOracleOnly(t *testing.T) {
	// `AS` here opens nothing in PostgreSQL: the body is quoted, and the
	// statements after it are separate.
	const sql = "CREATE FUNCTION f() RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql;\nDROP TABLE t;"

	c := qt.New(t)
	c.Assert(sqlutil.SplitSQLStatementsForDialect(sql, platform.Postgres), qt.HasLen, 2)
}
