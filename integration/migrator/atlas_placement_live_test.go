//go:build integration

package migrator_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// These tests are about where the Atlas revision table lands, so each one gets
// a database of its own and reaches it through the URL the case names. The
// rest of this package pins search_path=public (see pinPublicSchema) and
// cannot see the placement at all.
//
// The rule is Atlas's, measured against the pinned community binary v1.3.0 on
// PostgreSQL 18: through a URL that pins no search_path it keeps
// atlas_schema_revisions in a schema of the same name, and through one that
// pins a schema it keeps the table there (stokaro/ptah#3534).

// placedTable is one revision table as the catalog reports it.
type placedTable struct {
	Schema string
	Table  string
}

func placementDirectory() fstest.MapFS {
	return fstest.MapFS{
		"20260101000000_a.sql": {Data: []byte("CREATE TABLE placement_a (id integer PRIMARY KEY);\n")},
		"20260101000001_b.sql": {Data: []byte("CREATE TABLE placement_b (id integer PRIMARY KEY);\n")},
	}
}

// newPlacementDatabase creates a throwaway database and returns the URL that
// reaches it with query appended. The database is dropped when the test ends.
func newPlacementDatabase(c *qt.C, query string) string {
	c.Helper()
	ctx := context.Background()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_placement_%d", time.Now().UnixNano())
	ident := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+ident)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	values := parsed.Query()
	extra, err := url.ParseQuery(query)
	c.Assert(err, qt.IsNil)
	maps.Copy(values, extra)
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

func connectPlacement(c *qt.C, dbURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func newPlacementMigrator(c *qt.C, conn *dbschema.DatabaseConnection) *migrator.Migrator {
	c.Helper()
	m, err := migrator.NewFSMigrator(conn, placementDirectory(), migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return m
}

// revisionTables reads back every table either layout could have written,
// wherever it landed, so a copy in the wrong schema shows up beside the right
// one instead of hiding behind it.
func revisionTables(c *qt.C, conn *dbschema.DatabaseConnection) []placedTable {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND c.relname IN ('atlas_schema_revisions', 'schema_migrations', 'placement_revisions')
		ORDER BY n.nspname, c.relname`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var tables []placedTable
	for rows.Next() {
		var table placedTable
		c.Assert(rows.Scan(&table.Schema, &table.Table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

func placementSchemaExists(c *qt.C, conn *dbschema.DatabaseConnection, schema string) bool {
	c.Helper()
	var exists bool
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema,
	).Scan(&exists), qt.IsNil)
	return exists
}

func TestAtlasPlacement_HappyPath(t *testing.T) {
	atlas := migrator.RevisionTableFormatAtlas
	ptah := migrator.RevisionTableFormatPtah
	tests := []struct {
		name string
		// query is appended to the throwaway database's URL.
		query string
		// schema and table are the WithMigrationsTable arguments.
		schema string
		table  string
		// before and after are the formats applied before and after
		// WithMigrationsTable, in order, because the builders chain either way.
		before []migrator.RevisionTableFormat
		after  []migrator.RevisionTableFormat
		want   []placedTable
	}{
		{
			name:  "Atlas's table through a URL that pins no schema",
			after: []migrator.RevisionTableFormat{atlas},
			want:  []placedTable{{Schema: "atlas_schema_revisions", Table: "atlas_schema_revisions"}},
		},
		{
			name:   "the format named before the table",
			before: []migrator.RevisionTableFormat{atlas},
			want:   []placedTable{{Schema: "atlas_schema_revisions", Table: "atlas_schema_revisions"}},
		},
		{
			name:  "a URL that pins search_path keeps the table there",
			query: "search_path=public",
			after: []migrator.RevisionTableFormat{atlas},
			want:  []placedTable{{Schema: "public", Table: "atlas_schema_revisions"}},
		},
		{
			name:   "a schema the caller names",
			schema: "revs",
			after:  []migrator.RevisionTableFormat{atlas},
			want:   []placedTable{{Schema: "revs", Table: "atlas_schema_revisions"}},
		},
		{
			// Atlas reads only its own table name, so another name gains no
			// reader by moving and stays where the caller would look.
			name:  "a table under another name",
			table: "placement_revisions",
			after: []migrator.RevisionTableFormat{atlas},
			want:  []placedTable{{Schema: "public", Table: "placement_revisions"}},
		},
		{
			// The schema is what this row is about. The table keeps the name
			// the Atlas layout gave it, because a format change renames only
			// the native default.
			name:  "the placement goes with the format",
			after: []migrator.RevisionTableFormat{atlas, ptah},
			want:  []placedTable{{Schema: "public", Table: "atlas_schema_revisions"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectPlacement(c, newPlacementDatabase(c, test.query))
			m := newPlacementMigrator(c, conn)
			for _, format := range test.before {
				m = m.WithRevisionTableFormat(format)
			}
			m = m.WithMigrationsTable(test.schema, test.table)
			for _, format := range test.after {
				m = m.WithRevisionTableFormat(format)
			}

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

			c.Assert(revisionTables(c, conn), qt.DeepEquals, test.want)
			status, err := m.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.PendingMigrations, qt.HasLen, 0)
		})
	}
}

// TestAtlasPlacement_FailurePath covers a history in the connection's schema
// that the placed run would read past. Read as an empty history it makes `up`
// apply the whole directory again, so the run refuses, before it creates
// anything, and names both ways forward.
func TestAtlasPlacement_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := connectPlacement(c, newPlacementDatabase(c, ""))
	c.Assert(
		newPlacementMigrator(c, conn).
			WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
			WithMigrationsTable("public", "").
			MigrateUp(c.Context()),
		qt.IsNil,
	)
	placed := newPlacementMigrator(c, conn).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	const wantErr = `.*found revision table "atlas_schema_revisions" in schema "public", and none in schema ` +
		`"atlas_schema_revisions", where Atlas keeps it for a URL that pins no search_path and where this run ` +
		`reads it: pass --migrations-schema public to keep using the table where it is, or move it into schema ` +
		`"atlas_schema_revisions"`

	c.Assert(placed.MigrateUp(c.Context()), qt.ErrorMatches, wantErr)
	_, err := placed.GetMigrationStatus(c.Context())
	c.Assert(err, qt.ErrorMatches, wantErr)

	c.Assert(placementSchemaExists(c, conn, "atlas_schema_revisions"), qt.IsFalse)
	c.Assert(revisionTables(c, conn), qt.DeepEquals, []placedTable{{Schema: "public", Table: "atlas_schema_revisions"}})
}

// TestAtlasPlacement_NamedSchemaReadsTheHistory is the control for the
// refusal: the remedy it names reads the same history as applied.
func TestAtlasPlacement_NamedSchemaReadsTheHistory(t *testing.T) {
	c := qt.New(t)
	conn := connectPlacement(c, newPlacementDatabase(c, ""))
	writer := newPlacementMigrator(c, conn).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable("public", "")
	c.Assert(writer.MigrateUp(c.Context()), qt.IsNil)

	status, err := newPlacementMigrator(c, conn).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable("public", "").
		GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{20260101000000, 20260101000001})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

// TestAtlasPlacement_PlacedTableWinsWhenBothExist is the other control: the
// refusal is about a history the run would miss, and a run that finds its own
// table reads it, whatever else stands in the connection's schema.
func TestAtlasPlacement_PlacedTableWinsWhenBothExist(t *testing.T) {
	c := qt.New(t)
	conn := connectPlacement(c, newPlacementDatabase(c, ""))
	placed := newPlacementMigrator(c, conn).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(placed.MigrateUp(c.Context()), qt.IsNil)
	_, err := conn.ExecContext(c.Context(), `CREATE TABLE public.atlas_schema_revisions (version varchar PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	status, err := newPlacementMigrator(c, conn).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{20260101000000, 20260101000001})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}
