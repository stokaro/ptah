package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

func analyzeRoutine(c *qt.C, dialect, sql string) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{"1_routine.sql": sql}), lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestSQLInjectionRule_ReportsAStatementBuiltFromAValue pins SA101 to the
// three spellings and the parts that make each unsafe.
func TestSQLInjectionRule_ReportsAStatementBuiltFromAValue(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{
			name:    "PL/pgSQL EXECUTE with a concatenated identifier",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text) RETURNS void AS $$ BEGIN EXECUTE 'SELECT count(*) FROM ' || tbl; END $$ LANGUAGE plpgsql;",
			want:    "EXECUTE builds its statement from tbl: a value placed in the text unquoted, so a value that reaches the routine can rewrite the statement (SQL injection). In this routine, quote an identifier with quote_ident() or format('%I')",
		},
		{
			name:    "PL/pgSQL EXECUTE of a variable",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(q text) RETURNS void AS $$ BEGIN EXECUTE q; END $$ LANGUAGE plpgsql;",
			want:    "EXECUTE builds its statement from q: a value placed in the text unquoted",
		},
		{
			name:    "PL/pgSQL format() with %s",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text) RETURNS void AS $$ BEGIN EXECUTE format('SELECT * FROM %s', tbl); END $$ LANGUAGE plpgsql;",
			want:    "format() interpolates a value with %s, which places it in the text unquoted",
		},
		{
			name:    "PL/pgSQL format() mixing %I with %s",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text, v text) RETURNS void AS $$ BEGIN EXECUTE format('UPDATE %I SET a = %s', tbl, v); END $$ LANGUAGE plpgsql;",
			want:    "interpolates a value with %s",
		},
		{
			name:    "PL/pgSQL CONCAT",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text) RETURNS void AS $$ BEGIN EXECUTE concat('DELETE FROM ', tbl); END $$ LANGUAGE plpgsql;",
			want:    "CONCAT() joins a value into the text unquoted",
		},
		{
			name:    "PL/pgSQL EXECUTE with INTO and USING still judges the text",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text, v int) RETURNS int AS $$ DECLARE n int; BEGIN EXECUTE 'SELECT count(*) FROM ' || tbl || ' WHERE id = $1' INTO n USING v; RETURN n; END $$ LANGUAGE plpgsql;",
			want:    "EXECUTE builds its statement from tbl",
		},
		{
			name:    "MySQL PREPARE FROM a user variable",
			dialect: "mysql",
			sql:     "CREATE PROCEDURE p(IN t VARCHAR(64)) BEGIN SET @s = CONCAT('SELECT * FROM ', t); PREPARE stmt FROM @s; EXECUTE stmt; END",
			want:    "PREPARE ... FROM builds its statement from @s: a value placed in the text unquoted, so a value that reaches the routine can rewrite the statement (SQL injection). In this routine, prepare a literal statement with ? placeholders",
		},
		{
			name:    "MariaDB PREPARE FROM CONCAT",
			dialect: "mariadb",
			sql:     "CREATE PROCEDURE p(IN t VARCHAR(64)) BEGIN PREPARE stmt FROM CONCAT('SELECT * FROM ', t); EXECUTE stmt; END",
			want:    "CONCAT() joins a value into the text unquoted",
		},
		{
			name:    "T-SQL EXEC of a variable",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p @t sysname AS BEGIN DECLARE @sql nvarchar(max) = N'SELECT * FROM ' + @t; EXEC (@sql); END",
			want:    "EXEC (...) builds its statement from @sql: a value placed in the text unquoted, so a value that reaches the routine can rewrite the statement (SQL injection). In this routine, pass values as parameters of sp_executesql and wrap an identifier in QUOTENAME()",
		},
		{
			name:    "T-SQL sp_executesql with a built text",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p @t sysname AS BEGIN DECLARE @sql nvarchar(max) = N'SELECT * FROM ' + @t; EXEC sp_executesql @sql; END",
			want:    "EXEC sp_executesql builds its statement from @sql",
		},
		{
			name:    "T-SQL EXEC of a concatenation written inline",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p @t sysname AS BEGIN EXEC (N'SELECT * FROM ' + @t); END",
			want:    "EXEC (...) builds its statement from @t",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeRoutine(c, test.dialect, test.sql)
			c.Assert(rulesOf(analysis.Findings()), qt.Contains, "SA101")
			c.Assert(messageOf(analysis.Findings(), "SA101"), qt.Contains, test.want)
		})
	}
}

// TestSQLInjectionRule_StaysQuietWhereTheTextIsQuotedOrFixed holds the safe
// forms, which are the advice the finding gives.
func TestSQLInjectionRule_StaysQuietWhereTheTextIsQuotedOrFixed(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{
			name:    "a literal statement",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f() RETURNS void AS $$ BEGIN EXECUTE 'REFRESH MATERIALIZED VIEW totals'; END $$ LANGUAGE plpgsql;",
		},
		{
			name:    "a literal with USING placeholders",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(v int) RETURNS void AS $$ BEGIN EXECUTE 'DELETE FROM t WHERE id = $1' USING v; END $$ LANGUAGE plpgsql;",
		},
		{
			name:    "quote_ident and quote_literal",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text, v text) RETURNS void AS $$ BEGIN EXECUTE 'DELETE FROM ' || quote_ident(tbl) || ' WHERE a = ' || quote_literal(v); END $$ LANGUAGE plpgsql;",
		},
		{
			name:    "format() with %I and %L only",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f(tbl text, v text) RETURNS void AS $$ BEGIN EXECUTE format('DELETE FROM %I WHERE a = %L AND b = 100%%', tbl, v); END $$ LANGUAGE plpgsql;",
		},
		{
			name:    "a routine that runs no dynamic SQL",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN (SELECT count(*) FROM t); END $$ LANGUAGE plpgsql;",
		},
		{
			name:    "MySQL PREPARE FROM a literal",
			dialect: "mysql",
			sql:     "CREATE PROCEDURE p(IN v INT) BEGIN PREPARE stmt FROM 'DELETE FROM t WHERE id = ?'; SET @v = v; EXECUTE stmt USING @v; END",
		},
		{
			name:    "T-SQL EXEC of a procedure",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p AS BEGIN EXEC other_proc; EXEC dbo.other_proc @x = 1; END",
		},
		{
			name:    "T-SQL sp_executesql with a literal and parameters",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p @id int AS BEGIN EXEC sp_executesql N'DELETE FROM t WHERE id = @id', N'@id int', @id = @id; END",
		},
		{
			name:    "T-SQL QUOTENAME",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p @t sysname AS BEGIN EXEC (N'SELECT * FROM ' + QUOTENAME(@t)); END",
		},
		{
			name:    "a top-level EXECUTE runs a prepared statement, not a built one",
			dialect: "postgres",
			sql:     "PREPARE q AS SELECT 1;\nEXECUTE q;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeRoutine(c, test.dialect, test.sql)
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "SA101")
		})
	}
}

// TestSQLInjectionRule_NamesItsInputWithoutADialect: a body is parsed only
// once a dialect names its language, so a run without one says the rule
// could have said more rather than reporting less.
func TestSQLInjectionRule_NamesItsInputWithoutADialect(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeRoutine(c, "", "CREATE FUNCTION f(tbl text) RETURNS void AS $$ BEGIN EXECUTE 'SELECT 1 FROM ' || tbl; END $$ LANGUAGE plpgsql;")

	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "SA101")
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule)
	}
	c.Assert(unmet, qt.Contains, "SA101")
}
