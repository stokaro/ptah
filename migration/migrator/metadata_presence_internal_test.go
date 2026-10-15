package migrator

// White-box testing required: migrationTablePresenceQuery is deliberately
// internal, and deterministic query inspection covers the Spanner branch that
// cannot be exercised through the public API without a live Spanner service.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

func Test_migrationTablePresenceQuery_HappyPath(t *testing.T) {
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
			name:             "oracle connection schema",
			dialect:          platform.Oracle,
			connectionSchema: "PTAH_USER",
			table:            "schema_migrations",
			wantQuery:        "SELECT COUNT(*)\nFROM all_tables\nWHERE owner = ? AND table_name = ?",
			wantArgs:         []any{"PTAH_USER", "schema_migrations"},
		},
		{
			name:             "oracle configured schema",
			dialect:          platform.Oracle,
			configuredSchema: "REVISIONS",
			connectionSchema: "PTAH_USER",
			table:            "schema_migrations",
			wantQuery:        "SELECT COUNT(*)\nFROM all_tables\nWHERE owner = ? AND table_name = ?",
			wantArgs:         []any{"REVISIONS", "schema_migrations"},
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

func Test_migrationTablePresenceQuery_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantErr string
	}{
		{name: "a dialect it does not know", dialect: "unsupported",
			wantErr: `unsupported migration metadata dialect "unsupported"`},
		// YDB has no catalog SQL could ask, and its tables are described
		// through the reader instead, so it has no catalog service.
		{name: "ydb is described, not queried", dialect: platform.YDB,
			wantErr: `unsupported migration metadata dialect "ydb"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			query, args, err := migrationTablePresenceQuery(test.dialect, "", "", "schema_migrations", func(value string) string {
				return value
			})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(query, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}
