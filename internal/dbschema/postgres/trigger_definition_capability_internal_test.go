package postgres

// White-box testing required: readTriggersForSchema is package-local, and the
// question is which statement it sends for a given capability set, which no
// exported read exposes on its own.

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestReadTriggersForSchema_WithoutTriggerDefinitions reads triggers from a
// server that has no pg_get_triggerdef, the way CockroachDB 25.4 answers:
// `unknown function: pg_get_triggerdef()` for the whole statement. The read
// asks without it and gets every trigger, its events and its transition
// tables, and no condition, because it had nothing to read one from
// (stokaro/ptah#3707).
func TestReadTriggersForSchema_WithoutTriggerDefinitions(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringWithoutTriggerDefinitions)
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.CockroachDB25())

	triggers, err := reader.readTriggersForSchema(t.Context(), "public")

	c.Assert(err, qt.IsNil)
	c.Assert(triggers, qt.HasLen, 1)
	c.Assert([]string{triggers[0].Name, triggers[0].Event, triggers[0].When, triggers[0].NewTable},
		qt.DeepEquals, []string{"trg_a", "INSERT OR UPDATE", "", "new_rows"})
}

// TestReadTriggersForSchema_WithTriggerDefinitions is the same server with the
// function, as CockroachDB 26.3 has it, which prints the condition without the
// parentheses PostgreSQL writes. The condition is read whole rather than up to
// its first closing parenthesis.
func TestReadTriggersForSchema_WithTriggerDefinitions(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringCockroachTriggerDefinitions)
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.CockroachDB263())

	triggers, err := reader.readTriggersForSchema(t.Context(), "public")

	c.Assert(err, qt.IsNil)
	c.Assert(triggers, qt.HasLen, 1)
	c.Assert(triggers[0].When, qt.Equals, "(new).a > 0:::INT8")
}

// errUnknownTriggerDefinition is what CockroachDB 25.4 answers to any
// statement naming pg_get_triggerdef.
var errUnknownTriggerDefinition = errors.New("ERROR: unknown function: pg_get_triggerdef() (SQLSTATE 42883)")

func answeringWithoutTriggerDefinitions(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(query, "pg_get_triggerdef") {
		return dbtest.QueryResult{}, errUnknownTriggerDefinition
	}
	return oneTrigger(""), nil
}

func answeringCockroachTriggerDefinitions(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "pg_get_triggerdef") {
		return oneTrigger(""), nil
	}
	return oneTrigger("CREATE TRIGGER trg_a BEFORE INSERT OR UPDATE ON public.a FOR EACH ROW " +
		"WHEN (new).a > 0:::INT8 EXECUTE FUNCTION public.f()"), nil
}

// oneTrigger is the trigger reader's answer for one row trigger whose
// definition column carries definition.
func oneTrigger(definition string) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "table_name", "trigger_name",
			"timing", "event", "for_each", "body", "execute_function", "comment",
			"definition", "old_table", "new_table",
		},
		Rows: [][]driver.Value{
			{"public", "a", "trg_a", "BEFORE", "INSERT OR UPDATE", "ROW", "BEGIN RETURN NEW; END", "f", "",
				definition, "", "new_rows"},
		},
	}
}
