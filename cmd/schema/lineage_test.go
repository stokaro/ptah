package schema_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `schema lineage` exposes column-to-column dependencies as data, which is what
// makes "what breaks if I drop this column" answerable before the drop
// (stokaro/ptah#1712).

const lineageSchemaSQL = `CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT);
CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT);
CREATE VIEW active_users AS SELECT id, email AS contact FROM users;
CREATE VIEW joined AS SELECT id FROM users JOIN orders ON orders.user_id = users.id;
CREATE FUNCTION all_emails() RETURNS SETOF TEXT LANGUAGE sql AS $$ SELECT email FROM users $$;
CREATE FUNCTION order_count() RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN UPDATE orders SET user_id = 1 WHERE id = 2; RETURN 1; END; $$;
`

// runLineage writes the schema and runs the verb, returning only the verb's
// own output.
func runLineage(c *qt.C, args ...string) string {
	c.Helper()
	path := writeSchemaSQLFile(c, c.TempDir(), "schema.sql", lineageSchemaSQL)
	out, err := runSchema("", append([]string{"lineage", "--schema-file", path}, args...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return out
}

// TestSchemaLineageFollowsAnAliasToItsSource is the answer a table-level edge
// cannot give: which base column feeds which view column.
func TestSchemaLineageFollowsAnAliasToItsSource(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c)

	c.Assert(out, qt.Contains, "users.email")
	c.Assert(out, qt.Contains, "active_users.contact")
}

// TestSchemaLineageReportsWhatItCouldNotResolve is the property that decides
// whether the output can be trusted.
//
// A view whose dependencies went unresolved must not read like a view with
// none: a caller asking "is anything reading this column" would otherwise be
// told no about a column a view reads.
func TestSchemaLineageReportsWhatItCouldNotResolve(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c)

	c.Assert(out, qt.Contains, "not fully resolved")
	c.Assert(out, qt.Contains, "joined")
	c.Assert(out, qt.Contains, "more than one source")
}

// TestSchemaLineageJSONIsMachineReadable covers the "exposed as data" half:
// the document has to parse and carry both halves of the answer.
func TestSchemaLineageJSONIsMachineReadable(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c, "--format", "json")

	var document struct {
		Edges []struct {
			FromTable  string `json:"from_table"`
			FromColumn string `json:"from_column"`
			ToView     string `json:"to_view"`
			ToColumn   string `json:"to_column"`
		} `json:"edges"`
		Undecided []struct {
			View   string `json:"view"`
			Reason string `json:"reason"`
		} `json:"undecided"`
	}
	c.Assert(json.Unmarshal([]byte(jsonPortion(out)), &document), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(document.Edges, qt.Not(qt.HasLen), 0)
	c.Assert(document.Undecided, qt.HasLen, 1)
	c.Assert(document.Undecided[0].View, qt.Equals, "joined")
}

// TestSchemaLineageTracesARoutineBodyItCanResolve covers the routine half.
//
// A routine reads columns the same way a view does, and a caller asking "is
// anything reading this column" gets a wrong answer if routines are invisible
// to the trace (stokaro/ptah#2394).
func TestSchemaLineageTracesARoutineBodyItCanResolve(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c)

	c.Assert(out, qt.Contains, "all_emails")
	c.Assert(out, qt.Contains, "routine(s) not fully resolved")
	c.Assert(out, qt.Contains, "order_count")
	c.Assert(out, qt.Contains, "plpgsql")
}

// TestSchemaLineageReportsWhatARoutineWrites is the other direction the verb
// answers: not "what breaks if I drop this column" but "what changes it".
func TestSchemaLineageReportsWhatARoutineWrites(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c)

	c.Assert(out, qt.Contains, "WRITTEN BY")
	c.Assert(out, qt.Contains, "orders.user_id")
	c.Assert(out, qt.Contains, "update")
}

// TestSchemaLineageJSONCarriesRoutinesUnderTheirOwnKey pins the shape.
//
// The routine lineage is a key the document gained. A reader parsing "edges"
// keeps parsing the view edges it always parsed, and a routine never arrives
// in a field whose name says view (stokaro/ptah#2395).
func TestSchemaLineageJSONCarriesRoutinesUnderTheirOwnKey(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c, "--format", "json")

	var document struct {
		Edges []struct {
			ToView string `json:"to_view"`
		} `json:"edges"`
		Routines struct {
			Edges []struct {
				FromTable  string `json:"from_table"`
				FromColumn string `json:"from_column"`
				ToRoutine  string `json:"to_routine"`
				Kind       string `json:"kind"`
			} `json:"edges"`
			Writes []struct {
				Table     string `json:"table"`
				Column    string `json:"column"`
				ByRoutine string `json:"by_routine"`
				Statement string `json:"statement"`
			} `json:"writes"`
			Undecided []struct {
				Routine string `json:"routine"`
				Reason  string `json:"reason"`
			} `json:"undecided"`
		} `json:"routines"`
	}
	c.Assert(json.Unmarshal([]byte(jsonPortion(out)), &document), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(document.Edges, qt.Not(qt.HasLen), 0)
	c.Assert(document.Routines.Edges, qt.HasLen, 1)
	c.Assert(document.Routines.Edges[0].FromTable, qt.Equals, "users")
	c.Assert(document.Routines.Edges[0].FromColumn, qt.Equals, "email")
	c.Assert(document.Routines.Edges[0].ToRoutine, qt.Equals, "all_emails")
	c.Assert(document.Routines.Edges[0].Kind, qt.Equals, "function")
	c.Assert(document.Routines.Undecided, qt.HasLen, 1)
	c.Assert(document.Routines.Undecided[0].Routine, qt.Equals, "order_count")
	c.Assert(document.Routines.Writes, qt.HasLen, 1)
	c.Assert(document.Routines.Writes[0].Table, qt.Equals, "orders")
	c.Assert(document.Routines.Writes[0].Column, qt.Equals, "user_id")
	c.Assert(document.Routines.Writes[0].ByRoutine, qt.Equals, "order_count")
	c.Assert(document.Routines.Writes[0].Statement, qt.Equals, "update")
}

// TestSchemaLineageRefusesAnUnknownFormat keeps a typo from silently selecting
// the default rendering.
//
// It is also what keeps the validation and the dispatch in step: a format this
// check accepted and the writer did not handle would fall through to the
// table, which is the quiet way a new format ships as a synonym for the old
// one (stokaro/ptah#2576).
func TestSchemaLineageRefusesAnUnknownFormat(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, c.TempDir(), "schema.sql", lineageSchemaSQL)

	out, err := runSchema("", "lineage", "--schema-file", path, "--format", "yaml")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "must be table, json or dot")
}

// jsonPortion trims the loader's progress lines, which precede the document on
// the same stream.
func jsonPortion(out string) string {
	if start := strings.Index(out, "{"); start >= 0 {
		return out[start:]
	}
	return out
}
