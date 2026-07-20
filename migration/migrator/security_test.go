package migrator

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
)

// fmtVerbRe matches any Go fmt verb shape (`%s`, `%d`, `%v`, `%+v`, `%5.3f`,
// `%-10q`, …). The character class at the end is the union of Go's printf
// verbs so a sloppy revert using `%+v` or `%5s` is caught alongside the
// obvious `%s`/`%d` form.
var fmtVerbRe = regexp.MustCompile(`%[#+\- 0]*\d*(?:\.\d+)?[bcdeEfFgGoOpqstTUvxX]`)

// TestMigrationMetadataSQL_UsesPlaceholders is a regression guard for #130.
//
// Migration metadata SQL must use bind placeholders rather than fmt verbs, so
// migration descriptions and error text cannot be interpolated directly into
// the SQL text by the migrator.
func TestMigrationMetadataSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	queries := map[string]struct {
		sql          string
		placeholders int
	}{
		"begin migration":    {sql: (&Migrator{}).beginMigrationSQL(), placeholders: 10},
		"complete migration": {sql: (&Migrator{}).completeMigrationSQL(), placeholders: 6},
		"begin rollback":     {sql: (&Migrator{}).beginRollbackSQL(), placeholders: 5},
		"fail migration":     {sql: (&Migrator{}).failMigrationSQL(), placeholders: 7},
		"force applied":      {sql: (&Migrator{}).forceAppliedMigrationSQL(), placeholders: 8},
		"delete migration":   {sql: (&Migrator{}).deleteMigrationSQL(), placeholders: 1},
	}

	for name, query := range queries {
		c.Assert(strings.Count(query.sql, "?"), qt.Equals, query.placeholders,
			qt.Commentf("%s SQL must contain exactly %d placeholders", name, query.placeholders))

		c.Assert(fmtVerbRe.FindString(query.sql), qt.Equals, "",
			qt.Commentf("%s SQL must not contain any Go fmt verb - values must be bound as driver parameters", name))
	}
}

func TestMigrator_CustomMigrationsTableSQL(t *testing.T) {
	c := qt.New(t)

	m := (&Migrator{}).WithMigrationsTable("infra", "ptah_migrations")

	c.Assert(m.migrationsSchemaStatement(), qt.Equals, `CREATE SCHEMA IF NOT EXISTS "infra"`)
	c.Assert(m.createMigrationsTableSQL(), qt.Contains, `CREATE TABLE IF NOT EXISTS "infra"."ptah_migrations"`)
	c.Assert(m.getVersionSQL(), qt.Equals, `SELECT COALESCE(MAX(version), 0) FROM "infra"."ptah_migrations" WHERE state = 'applied'`)
	c.Assert(m.beginMigrationSQL(), qt.Contains, `INSERT INTO "infra"."ptah_migrations"`)
	c.Assert(m.completeMigrationSQL(), qt.Contains, `UPDATE "infra"."ptah_migrations"`)
	c.Assert(m.deleteMigrationSQL(), qt.Equals, `DELETE FROM "infra"."ptah_migrations" WHERE version = ?`)
}

func TestMetadataTableSchemaName_SQLServerHonorsConnectionSchema(t *testing.T) {
	tests := []struct {
		name             string
		dialect          string
		connectionSchema string
		configuredSchema string
		expected         string
	}{
		{
			name:             "sqlserver url schema",
			dialect:          platform.SQLServer,
			connectionSchema: "tenant_a",
			expected:         "tenant_a",
		},
		{
			name:     "sqlserver default schema",
			dialect:  platform.SQLServer,
			expected: "dbo",
		},
		{
			name:             "explicit schema wins",
			dialect:          platform.SQLServer,
			connectionSchema: "tenant_a",
			configuredSchema: "audit",
			expected:         "audit",
		},
		{
			name:             "non sqlserver connection schema does not qualify table",
			dialect:          platform.Postgres,
			connectionSchema: "public",
			expected:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := metadataTableSchemaName(tt.dialect, tt.connectionSchema, tt.configuredSchema)

			c.Assert(got, qt.Equals, tt.expected)
		})
	}
}

func TestMetadataInformationSchemaName_DialectDefaults(t *testing.T) {
	tests := []struct {
		name             string
		dialect          string
		connectionSchema string
		configuredSchema string
		expected         string
	}{
		{
			name:             "mysql uses current database",
			dialect:          platform.MySQL,
			connectionSchema: "ptah_test",
			expected:         "ptah_test",
		},
		{
			name:             "mariadb uses current database",
			dialect:          platform.MariaDB,
			connectionSchema: "ptah_test",
			expected:         "ptah_test",
		},
		{
			name:     "postgres default public schema",
			dialect:  platform.Postgres,
			expected: "public",
		},
		{
			name:             "sqlserver uses connection schema",
			dialect:          platform.SQLServer,
			connectionSchema: "tenant_a",
			expected:         "tenant_a",
		},
		{
			name:             "explicit schema wins",
			dialect:          platform.MySQL,
			connectionSchema: "ptah_test",
			configuredSchema: "tenant_a",
			expected:         "tenant_a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := metadataInformationSchemaName(tt.dialect, tt.connectionSchema, tt.configuredSchema)

			c.Assert(got, qt.Equals, tt.expected)
		})
	}
}
