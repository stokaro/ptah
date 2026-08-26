package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// TestAnalyzeFS_ARoutineCreationCarriesItsName pins the name a routine reaches
// the change model with.
//
// A parsed routine node carries the name it was declared with, and the change
// model discarded it: every CREATE FUNCTION and CREATE PROCEDURE arrived as an
// addition of "". A finding about a routine could therefore not say which
// routine, and neither could anything downstream that names what a migration
// touches (stokaro/ptah#1270).
//
// The opaque node is the deliberate exception and has its own row: it is the
// fallback the parser preserves verbatim when no dialect path claimed the
// statement, so there is no parsed name to take.
func TestAnalyzeFS_ARoutineCreationCarriesItsName(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		sql        string
		wantObject string
	}{
		{
			name:    "PostgreSQL function",
			dialect: "postgres",
			sql: "CREATE FUNCTION set_tenant(tenant_id TEXT) RETURNS VOID AS $$\n" +
				"BEGIN\n  PERFORM 1;\nEND;\n$$ LANGUAGE plpgsql;\n",
			wantObject: "set_tenant",
		},
		{
			// A body with no statement terminator inside it. A multi-statement
			// BEGIN ... END body is split before it reaches the parser, which
			// is a separate gap and not what this row is about.
			name:       "MySQL function",
			dialect:    "mysql",
			sql:        "CREATE FUNCTION add_one(n INT) RETURNS INT DETERMINISTIC RETURN n + 1;\n",
			wantObject: "add_one",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(
				fixture(map[string]string{"1_routine.sql": test.sql}),
				lint.Options{DirFormat: migrationfile.DirFormatAtlas, Dialect: test.dialect},
			)

			c.Assert(err, qt.IsNil)
			file := fileByName(c, analysis, "1_routine.sql")
			c.Assert(file.Changes, qt.Not(qt.HasLen), 0,
				qt.Commentf("a routine creation is a schema change"))
			c.Assert(file.Changes[0].Object, qt.Equals, test.wantObject)
		})
	}
}
