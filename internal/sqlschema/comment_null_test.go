package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// Clearing a comment changes only the named object, including a table member
// whose name another table also uses.
func TestRead_CommentOnNullClearsOnlyNamedObject(t *testing.T) {
	tests := []struct{ target, key string }{
		{"TABLE app.t", "table app.t"},
		{"COLUMN app.t.a", "column " + appTStructName + ".a"},
		{"INDEX app.t_a_idx", "index t_a_idx"},
		{"SCHEMA app", "schema app"},
		{"ROLE reader", "role reader"},
		{"VIEW app.v", "view app.v"},
		{"SEQUENCE app.s", "sequence app.s"},
		{"DOMAIN app.d", "domain app.d"},
		{"TYPE app.c", "composite app.c"},
		{"TYPE app.r", "range app.r"},
		{"TYPE app.mood", "enum app.mood"},
		{"MATERIALIZED VIEW app.mt", "materialized view app.mt"},
		{"FUNCTION app.f(integer, text)", "function app.f(a int, b text default 'x')"},
		{"FUNCTION app.g()", "function app.g()"},
		{"PROCEDURE app.p(int)", "procedure app.p(a int)"},
		{"TRIGGER audit ON app.t", "trigger audit on app.t"},
		{"POLICY own ON app.t", "policy own on app.t"},
		{"CONSTRAINT positive ON app.t", "constraint positive on app.t"},
	}
	var setup strings.Builder
	setup.WriteString(commentFixture)
	setup.WriteString("COMMENT ON TABLE app.u IS 'retained';")
	setup.WriteString("COMMENT ON CONSTRAINT positive ON app.u IS 'retained';")
	setup.WriteString("COMMENT ON TRIGGER audit ON app.u IS 'retained';")
	for _, test := range tests {
		setup.WriteString("COMMENT ON " + test.target + " IS 'retained';")
	}
	for _, test := range tests {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			before, _, err := readWithPolicyOwners([]byte(setup.String()), "postgres")
			c.Assert(err, qt.IsNil)
			want := commentsOf(before)
			c.Assert(want[test.key], qt.Equals, "retained")
			want[test.key] = ""
			database, _, err := readWithPolicyOwners([]byte(setup.String()+"COMMENT ON "+test.target+" IS NULL;"), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(commentsOf(database), qt.DeepEquals, want)
		})
	}
}

func TestRead_CommentOnNullAndLiteralNull(t *testing.T) {
	tests := []struct{ name, edits, want string }{
		{"empty comment", "COMMENT ON TABLE t IS NULL;", ""},
		{"repeated removal", "COMMENT ON TABLE t IS NULL; COMMENT ON TABLE t IS NULL;", ""},
		{"lowercase null", "COMMENT ON TABLE t IS 'set'; COMMENT ON TABLE t IS null;", ""},
		{"literal null", "COMMENT ON TABLE t IS 'NULL';", "NULL"},
		{"dollar quoted null", "COMMENT ON TABLE t IS $c$NULL$c$;", "NULL"},
		{"new text after removal", "COMMENT ON TABLE t IS NULL; COMMENT ON TABLE t IS 'new';", "new"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int);"+test.edits), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.Tables[0].Comment, qt.Equals, test.want)
		})
	}
}

func TestRead_CommentOnNullInLaterFile(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int); COMMENT ON TABLE t IS 'set';"), "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].Comment, qt.Equals, "set")
	added, _, err := sqlschema.ReadOnto([]byte("COMMENT ON TABLE t IS NULL;"), "postgres", sqlschema.NewDocument(&database))
	c.Assert(err, qt.IsNil)
	c.Assert(added.Tables, qt.HasLen, 0)
	c.Assert(database.Tables[0].Comment, qt.Equals, "")
}

func TestRead_CommentOnNullRefusesUndeclaredObject(t *testing.T) {
	c := qt.New(t)
	database, statements, err := sqlschema.Read([]byte("COMMENT ON TABLE absent IS NULL;"), "postgres")
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(statements, qt.IsNil)
}
