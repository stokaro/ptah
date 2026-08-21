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

// TestSchemaLineageRefusesAnUnknownFormat keeps a typo from silently selecting
// the default rendering.
func TestSchemaLineageRefusesAnUnknownFormat(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, c.TempDir(), "schema.sql", lineageSchemaSQL)

	out, err := runSchema("", "lineage", "--schema-file", path, "--format", "yaml")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "must be table or json")
}

// jsonPortion trims the loader's progress lines, which precede the document on
// the same stream.
func jsonPortion(out string) string {
	if start := strings.Index(out, "{"); start >= 0 {
		return out[start:]
	}
	return out
}
