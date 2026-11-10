//go:build integration

package integration_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// A PostgreSQL database can hold objects that no schema source describes.
// Drift must name them even when the desired table matches the live table.
func TestPostgreSQLDriftNamesUnmodeledObjectsE2E(t *testing.T) {
	c := qt.New(t)
	const table = "CREATE TABLE kept (id integer PRIMARY KEY);"
	database := historyDatabase(c,
		table,
		"CREATE AGGREGATE keep_a(integer) (SFUNC = int4pl, STYPE = integer)",
		"CREATE COLLATION keep_c (provider = libc, locale = 'C')",
		"CREATE EXTENSION postgres_fdw",
		"CREATE SERVER keep_s FOREIGN DATA WRAPPER postgres_fdw",
		"CREATE FOREIGN TABLE keep_f (id integer) SERVER keep_s",
		"CREATE FUNCTION keep_event() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$",
		"CREATE EVENT TRIGGER keep_e ON ddl_command_end EXECUTE FUNCTION keep_event()",
		"CREATE PUBLICATION keep_p",
	)
	schema := historySchemaFile(c, table)
	got := historyDrift(c, database, schema, "--format", "json")
	var report struct {
		NotCompared []catalog.UnmodeledObject `json:"not_compared"`
	}
	c.Assert(json.Unmarshal([]byte(got.Stdout), &report), qt.IsNil,
		qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", got.ExitCode, got.Stdout, got.Stderr))
	namesByKind := make(map[string]string, len(report.NotCompared))
	for _, object := range report.NotCompared {
		namesByKind[object.Kind] = object.Name
	}
	c.Assert(namesByKind, qt.HasLen, 5)
	for _, kind := range []string{"aggregate", "collation", "event trigger", "foreign table", "publication"} {
		c.Assert(strings.HasPrefix(namesByKind[kind], "keep_"), qt.IsTrue,
			qt.Commentf("missing %s from %+v", kind, report.NotCompared))
	}
}
