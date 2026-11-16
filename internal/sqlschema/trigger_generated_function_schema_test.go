package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// A description names the function Ptah generated for a trigger with its
// schema when the read found it outside the schema the read resolves names in.
// Read back, the function is still the trigger's own: it folds into the
// trigger's body, and the trigger keeps the schema it is held in, so a plan
// made from the file neither declares it as a function of its own nor
// recreates it where a bare name resolves.
func TestRead_ATriggerAdoptsItsGeneratedFunctionInAnotherSchema(t *testing.T) {
	c := qt.New(t)
	file := `CREATE SCHEMA app;
CREATE TABLE app.items (id int PRIMARY KEY);
CREATE FUNCTION "app"."ptah_trigger_app__items_t2"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
RETURN NEW;
END;
$$;
CREATE TRIGGER t2 BEFORE UPDATE ON app.items FOR EACH ROW EXECUTE FUNCTION "app"."ptah_trigger_app__items_t2"();`

	database, _, err := sqlschema.Read([]byte(file), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 0)
	c.Assert(database.Triggers, qt.HasLen, 1)
	c.Assert(database.Triggers[0].Body, qt.Equals, "RETURN NEW;")
	c.Assert(database.Triggers[0].ExecuteFunction, qt.Equals, "")
	c.Assert(database.Triggers[0].FunctionReference, qt.Equals, "app.ptah_trigger_app__items_t2")
}

// A function of the user's own that carries Ptah's prefix, run by a trigger
// whose generated name it is not, is a function declared on its own. Folded
// into the trigger's body, its SECURITY DEFINER and its setting would be
// dropped without a word.
func TestRead_ATriggerKeepsAFunctionOfItsOwnWithPtahsPrefix(t *testing.T) {
	c := qt.New(t)
	file := `CREATE TABLE items (id int PRIMARY KEY);
CREATE FUNCTION ptah_trigger_audit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
RETURN NEW;
END;
$$;
CREATE TRIGGER t BEFORE UPDATE ON items FOR EACH ROW EXECUTE FUNCTION ptah_trigger_audit();`

	database, _, err := sqlschema.Read([]byte(file), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.Functions[0].Name, qt.Equals, "ptah_trigger_audit")
	c.Assert(database.Triggers, qt.HasLen, 1)
	c.Assert(database.Triggers[0].ExecuteFunction, qt.Equals, "ptah_trigger_audit")
	c.Assert(database.Triggers[0].Body, qt.Equals, "")
}
