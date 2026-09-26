package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// A schema file keeps every overload it declares, which is what pg_dump writes
// for a schema that overloads a function. The read kept the first overload of
// a name and dropped the rest without a word (stokaro/ptah#3672).
func TestRead_KeepsEveryOverload(t *testing.T) {
	c := qt.New(t)
	file := `CREATE FUNCTION app.f(a int) RETURNS int LANGUAGE sql AS $$SELECT 1$$;
CREATE FUNCTION app.f(a int, b text) RETURNS int LANGUAGE sql AS $$SELECT 2$$;
CREATE PROCEDURE app.f(a text) LANGUAGE sql AS $$SELECT 3$$;`

	database, _, err := sqlschema.Read([]byte(file), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 3)
	parameters := []string{database.Functions[0].Parameters, database.Functions[1].Parameters, database.Functions[2].Parameters}
	c.Assert(parameters, qt.DeepEquals, []string{"a int", "a int, b text", "a text"})
	c.Assert(database.Functions[2].IsProcedure(), qt.IsTrue)
}

// A trigger's function takes no arguments, and only that overload of its name
// is folded into the trigger: an overload with arguments is a function of its
// own and stays one.
func TestRead_ATriggerAdoptsOnlyItsOwnOverload(t *testing.T) {
	c := qt.New(t)
	file := `CREATE TABLE t (id int PRIMARY KEY);
CREATE FUNCTION ptah_trigger_t_touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
RETURN NEW;
END;
$$;
CREATE FUNCTION ptah_trigger_t_touch(a int) RETURNS int LANGUAGE sql AS $$SELECT 1$$;
CREATE TRIGGER touch BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION ptah_trigger_t_touch();`

	database, _, err := sqlschema.Read([]byte(file), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Triggers, qt.HasLen, 1)
	c.Assert(database.Triggers[0].Body, qt.Equals, "RETURN NEW;")
	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.Functions[0].Parameters, qt.Equals, "a int")
}

// Within one file, COMMENT ON names an overload by its argument types, as
// PostgreSQL does, and a bare name that could be either overload is refused
// rather than applied to one of them.
func TestRead_CommentOnAnOverloadInTheSameFile(t *testing.T) {
	c := qt.New(t)
	file := `CREATE FUNCTION app.f(a int) RETURNS int LANGUAGE sql AS $$SELECT 1$$;
CREATE FUNCTION app.f(a int, b text) RETURNS int LANGUAGE sql AS $$SELECT 2$$;
COMMENT ON FUNCTION app.f(integer, text) IS 'second';`

	database, _, err := sqlschema.Read([]byte(file), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 2)
	c.Assert([]string{database.Functions[0].Comment, database.Functions[1].Comment}, qt.DeepEquals, []string{"", "second"})
}

func TestRead_CommentOnABareOverloadedNameIsRefused(t *testing.T) {
	c := qt.New(t)
	file := `CREATE FUNCTION app.f(a int) RETURNS int LANGUAGE sql AS $$SELECT 1$$;
CREATE FUNCTION app.f(a int, b text) RETURNS int LANGUAGE sql AS $$SELECT 2$$;
COMMENT ON FUNCTION app.f IS 'which';`

	_, _, err := sqlschema.Read([]byte(file), platform.Postgres)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*COMMENT ON FUNCTION app.f names more than one declared overload; write its argument types to name one`)
}
