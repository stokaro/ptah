package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// routineAttributes are the attributes of a routine this file is about.
type routineAttributes struct {
	Security   string
	Volatility string
	Comment    string
	Settings   []string
}

// TestToDatabase_ARoutineKeepsTheAttributesItStates pins that a routine read
// from SQL keeps the security mode, the volatility, the comment and the
// settings its statement writes.
//
// The MySQL and MariaDB reader kept a routine's characteristics as text and
// read none of them, and the PostgreSQL reader read no SECURITY or SET clause
// of a procedure. Each such routine reached the model with the defaults: a
// routine declared SQL SECURITY DEFINER, or a procedure declared SECURITY
// DEFINER, was created to run with its caller's rights, a DETERMINISTIC
// function was created NOT DETERMINISTIC, and a comment and a pinned
// search_path were dropped.
func TestToDatabase_ARoutineKeepsTheAttributesItStates(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		sql     string
		want    routineAttributes
	}{
		{name: "mysql definer procedure", dialect: "mysql",
			sql:  "CREATE PROCEDURE p() SQL SECURITY DEFINER BEGIN SELECT 1; END",
			want: routineAttributes{Security: "DEFINER", Volatility: "VOLATILE"}},
		{name: "mariadb invoker procedure", dialect: "mariadb",
			sql:  "CREATE PROCEDURE p() SQL SECURITY INVOKER BEGIN SELECT 1; END",
			want: routineAttributes{Security: "INVOKER", Volatility: "VOLATILE"}},
		{name: "mysql deterministic function", dialect: "mysql",
			sql:  "CREATE FUNCTION f() RETURNS INT DETERMINISTIC SQL SECURITY DEFINER RETURN 1",
			want: routineAttributes{Security: "DEFINER", Volatility: "IMMUTABLE"}},
		{name: "mysql no sql function", dialect: "mysql",
			sql:  "CREATE FUNCTION f() RETURNS INT NOT DETERMINISTIC NO SQL SQL SECURITY INVOKER RETURN 1",
			want: routineAttributes{Security: "INVOKER", Volatility: "STABLE"}},
		{name: "mysql comment with a doubled quote", dialect: "mysql",
			sql:  "CREATE PROCEDURE p() COMMENT 'it''s a probe' SQL SECURITY INVOKER BEGIN SELECT 1; END",
			want: routineAttributes{Security: "INVOKER", Volatility: "VOLATILE", Comment: "it's a probe"}},
		{name: "mysql comment written twice", dialect: "mysql",
			sql:  "CREATE PROCEDURE p() COMMENT 'a' COMMENT 'b' SQL SECURITY INVOKER BEGIN SELECT 1; END",
			want: routineAttributes{Security: "INVOKER", Volatility: "VOLATILE", Comment: "b"}},
		{name: "mariadb comment with a backslash escape", dialect: "mariadb",
			sql:  `CREATE FUNCTION f() RETURNS INT DETERMINISTIC SQL SECURITY INVOKER COMMENT 'a\'b' RETURN 1`,
			want: routineAttributes{Security: "INVOKER", Volatility: "IMMUTABLE", Comment: "a'b"}},
		{name: "postgres definer procedure with a search path", dialect: "postgres",
			sql:  "CREATE PROCEDURE p() LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$ BEGIN NULL; END; $$;",
			want: routineAttributes{Security: "DEFINER", Volatility: "VOLATILE", Settings: []string{"search_path=pg_catalog,pg_temp"}}},
		{name: "cockroachdb invoker procedure", dialect: "cockroachdb",
			sql:  "CREATE PROCEDURE p() LANGUAGE plpgsql EXTERNAL SECURITY INVOKER AS $$ BEGIN NULL; END; $$;",
			want: routineAttributes{Security: "INVOKER", Volatility: "VOLATILE"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			routines := routinesOf(c, test.dialect, test.sql)

			c.Assert(routines, qt.HasLen, 1)
			got := routines[0]
			c.Assert(routineAttributes{Security: got.Security, Volatility: got.Volatility, Comment: got.Comment, Settings: got.Settings},
				qt.DeepEquals, test.want)
		})
	}
}

// A MySQL COMMENT that is not followed by a string is not one the server
// reads. The routine is kept as text, and the conversion refuses it by name
// rather than reading it without its comment.
func TestToDatabase_ARoutineKeepsTheAttributesItStates_FailurePath(t *testing.T) {
	c := qt.New(t)
	statements, err := parser.NewParser("CREATE PROCEDURE p() COMMENT 42 BEGIN SELECT 1; END",
		parser.WithDialect("mysql")).Parse()
	c.Assert(err, qt.IsNil)

	database, err := sqlschema.ToDatabase(statements, "mysql")

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*a mysql procedure whose body was kept as text.*`)
	c.Assert(database.Functions, qt.HasLen, 0)
}
