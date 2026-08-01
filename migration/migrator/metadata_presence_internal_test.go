package migrator

// White-box testing required: migrationTablePresenceQuery is deliberately
// internal, and deterministic query inspection covers the Spanner branch that
// cannot be exercised through the public API without a live Spanner service.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
)

func Test_migrationTablePresenceQuery_HappyPath(t *testing.T) {
	c := qt.New(t)
	quote := func(value string) string { return `"` + value + `"` }
	tests := []struct {
		name             string
		dialect          string
		configuredSchema string
		connectionSchema string
		table            string
		wantQuery        string
		wantArgs         []any
	}{
		{
			name:      "postgres search path",
			dialect:   platform.Postgres,
			table:     "schema_migrations",
			wantQuery: "SELECT COUNT(*)\nFROM information_schema.tables\nWHERE table_schema = current_schema() AND table_name = ? AND table_type = 'BASE TABLE'",
			wantArgs:  []any{"schema_migrations"},
		},
		{
			name:             "postgres configured schema",
			dialect:          platform.Postgres,
			configuredSchema: "revisions",
			table:            "schema_migrations",
			wantQuery:        "SELECT COUNT(*)\nFROM information_schema.tables\nWHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'",
			wantArgs:         []any{"revisions", "schema_migrations"},
		},
		{
			name:             "spanner connection schema",
			dialect:          platform.Spanner,
			connectionSchema: "public",
			table:            "schema_migrations",
			wantQuery:        "SELECT COUNT(*)\nFROM information_schema.tables\nWHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'",
			wantArgs:         []any{"public", "schema_migrations"},
		},
		{
			name:             "sqlite attached schema",
			dialect:          platform.SQLite,
			configuredSchema: "aux",
			table:            "Schema_Migrations",
			wantQuery:        `SELECT COUNT(*) FROM "aux".sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE`,
			wantArgs:         []any{"Schema_Migrations"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			query, args, err := migrationTablePresenceQuery(
				test.dialect,
				test.configuredSchema,
				test.connectionSchema,
				test.table,
				quote,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(query, qt.Equals, test.wantQuery)
			c.Assert(args, qt.DeepEquals, test.wantArgs)
		})
	}
}

func Test_metadataInformationSchemaName_SpannerUsesConnectionSchema(t *testing.T) {
	c := qt.New(t)

	got := metadataInformationSchemaName(platform.Spanner, "public", "")

	c.Assert(got, qt.Equals, "public")
}

func Test_migrationTablePresenceQuery_FailurePath(t *testing.T) {
	c := qt.New(t)
	query, args, err := migrationTablePresenceQuery("unsupported", "", "", "schema_migrations", func(value string) string {
		return value
	})
	c.Assert(err, qt.ErrorMatches, `unsupported migration metadata dialect "unsupported"`)
	c.Assert(query, qt.Equals, "")
	c.Assert(args, qt.IsNil)
}
