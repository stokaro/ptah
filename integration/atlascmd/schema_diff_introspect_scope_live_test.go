//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// uniqueScopeSuffix names throwaway databases and schemas so parallel runs
// against one PostgreSQL server never collide.
func uniqueScopeSuffix() string {
	return fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
}

// createDisposableDatabase creates a database on the server behind dbURL,
// registers its removal, and returns a URL pointing at it. `schema diff`
// introspects whole databases, so the fixtures below need databases of their
// own rather than schemas inside the shared test database.
func createDisposableDatabase(c *qt.C, dbURL, name string) string {
	c.Helper()

	admin, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// seedDatabase runs setup DDL against one disposable database.
func seedDatabase(c *qt.C, dbURL string, statements ...string) {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(context.Background(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// runCompatSchemaDiff executes `schema diff` on the ptah-compat surface and
// returns the bytes it wrote, so assertions can pin output rather than exit
// status: a diff that reports "synced" and a diff that found nothing share an
// exit code.
func runCompatSchemaDiff(c *qt.C, args ...string) string {
	c.Helper()

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"schema", "diff"}, args...))

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	return out.String()
}

// TestSchemaDiffIntrospectsRequestedSchemaLivePostgres pins that a
// database-backed diff side is introspected at the schemas --schema names.
//
// Before this was fixed, both sides were read at the connection's default
// schema and --schema was applied afterwards as a filter over a universe that
// never contained the requested schema. The selection then matched nothing,
// and nothing was reported as an empty database: a diff against a file emitted
// a from-scratch CREATE TABLE for a table that already existed, and piping
// that plan into the very database it was computed from failed with
// `relation "users" already exists`.
func TestSchemaDiffIntrospectsRequestedSchemaLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	suffix := uniqueScopeSuffix()
	scoped := "ptah_diff_scope_" + suffix
	defaultTable := "ptah_diff_default_" + suffix
	targetURL := createDisposableDatabase(c, dbURL, "ptah_diff_from_"+suffix)
	seedDatabase(c, targetURL,
		"CREATE SCHEMA "+scoped,
		"CREATE TABLE "+scoped+".users (id SERIAL PRIMARY KEY, email TEXT)",
		"CREATE TABLE "+defaultTable+" (id SERIAL PRIMARY KEY, email TEXT)",
	)
	desiredPath := filepath.Join(t.TempDir(), "schema.sql")
	desired := "CREATE TABLE " + scoped + ".users (id SERIAL PRIMARY KEY, email TEXT, extra TEXT);\n" +
		"CREATE TABLE " + defaultTable + " (id SERIAL PRIMARY KEY, email TEXT, extra TEXT);\n"
	c.Assert(os.WriteFile(desiredPath, []byte(desired), 0o600), qt.IsNil)

	tests := []struct {
		name string
		// scope names the schema the row asks for, resolved per run because
		// the fixture names carry a unique suffix.
		scope func() string
		// wantContains are byte sequences the emitted plan must hold.
		wantContains func() []string
		// wantAbsent are byte sequences the emitted plan must not hold.
		wantAbsent func() []string
	}{
		{
			// The requested schema is not the connection default, so this row
			// only passes when introspection honored --schema.
			name:  "non-default schema is introspected",
			scope: func() string { return scoped },
			wantContains: func() []string {
				return []string{`ALTER TABLE "` + scoped + `"."users" ADD COLUMN "extra" TEXT;`}
			},
			wantAbsent: func() []string {
				return []string{"CREATE TABLE", "Schemas are synced"}
			},
		},
		{
			// Discriminating control on the same binary and the same fixture:
			// the connection default schema was always read, so this row is
			// correct with or without the introspection scope. It separates
			// "--schema is honored" from "diff works at all".
			name:  "default schema control",
			scope: func() string { return "public" },
			wantContains: func() []string {
				return []string{`ALTER TABLE "` + defaultTable + `" ADD COLUMN "extra" TEXT;`}
			},
			wantAbsent: func() []string {
				return []string{"CREATE TABLE", "Schemas are synced", scoped}
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			out := runCompatSchemaDiff(c,
				"--from", targetURL,
				"--to", "file://"+desiredPath,
				"--dev-url", targetURL,
				"--schema", test.scope(),
			)

			for _, want := range test.wantContains() {
				c.Assert(out, qt.Contains, want)
			}
			for _, absent := range test.wantAbsent() {
				c.Assert(out, qt.Not(qt.Contains), absent)
			}
		})
	}
}

// TestSchemaDiffDatabaseToDatabaseIntrospectsRequestedSchemaLivePostgres pins
// the same fix for a diff whose two sides are both live databases. No HCL or
// SQL file is involved, so a wrong answer here cannot be blamed on desired
// state parsing: with the introspection scope ignored, both sides read the
// empty default schema and the command answered "Schemas are synced" for two
// databases that plainly differ.
func TestSchemaDiffDatabaseToDatabaseIntrospectsRequestedSchemaLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	suffix := uniqueScopeSuffix()
	scoped := "ptah_dbdiff_scope_" + suffix
	fromURL := createDisposableDatabase(c, dbURL, "ptah_dbdiff_from_"+suffix)
	toURL := createDisposableDatabase(c, dbURL, "ptah_dbdiff_to_"+suffix)
	seedDatabase(c, fromURL,
		"CREATE SCHEMA "+scoped,
		"CREATE TABLE "+scoped+".users (id SERIAL PRIMARY KEY, email TEXT)",
	)
	seedDatabase(c, toURL,
		"CREATE SCHEMA "+scoped,
		"CREATE TABLE "+scoped+".users (id SERIAL PRIMARY KEY, email TEXT, extra TEXT)",
		"CREATE TABLE "+scoped+".blatant (id SERIAL PRIMARY KEY)",
	)

	out := runCompatSchemaDiff(c,
		"--from", fromURL,
		"--to", toURL,
		"--dev-url", fromURL,
		"--schema", scoped,
	)

	// Both sides are introspected, so the added column carries the type
	// spelling PostgreSQL reports rather than the one a schema file declared.
	c.Assert(out, qt.Contains, `ALTER TABLE "`+scoped+`"."users" ADD COLUMN "extra" text;`)
	c.Assert(out, qt.Contains, `CREATE TABLE "`+scoped+`"."blatant"`)
	c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
}

// TestSchemaDiffMigrationDirReplayIntrospectsRequestedSchemaLivePostgres
// covers the second introspection site: a migration directory replayed on the
// dev database. The replayed state was read at the dev connection's default
// schema too, so a migration directory that provisions its own schema was
// compared as if it were empty.
//
// The second row additionally names the dev database's default schema, where
// the replay bookkeeping lives, to pin that widening the read never leaks the
// Atlas revision table into the comparison.
func TestSchemaDiffMigrationDirReplayIntrospectsRequestedSchemaLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	suffix := uniqueScopeSuffix()
	scoped := "ptah_replay_scope_" + suffix
	fromURL := createDisposableDatabase(c, dbURL, "ptah_replay_from_"+suffix)
	devURL := createDisposableDatabase(c, dbURL, "ptah_replay_dev_"+suffix)
	seedDatabase(c, fromURL,
		"CREATE SCHEMA "+scoped,
		"CREATE TABLE "+scoped+".users (id SERIAL PRIMARY KEY, email TEXT)",
	)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "20260801120000_init.sql"),
		[]byte("CREATE SCHEMA "+scoped+";\n"+
			"CREATE TABLE "+scoped+".users (id SERIAL PRIMARY KEY, email TEXT, extra TEXT);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name string
		// scope names the --schema value, resolved per run because the fixture
		// names carry a unique suffix.
		scope func() string
	}{
		{
			name:  "replayed schema only",
			scope: func() string { return scoped },
		},
		{
			// The revision table the replay writes lives in the dev database's
			// default schema, which this row pulls into the read.
			name:  "replayed schema alongside the dev default",
			scope: func() string { return scoped + ",public" },
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			out := runCompatSchemaDiff(c,
				"--from", fromURL,
				"--to", "file://"+migrationsDir,
				"--dev-url", devURL,
				"--schema", test.scope(),
			)

			c.Assert(out, qt.Contains, `ALTER TABLE "`+scoped+`"."users" ADD COLUMN "extra" text;`)
			c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
			c.Assert(out, qt.Not(qt.Contains), "atlas_schema_revisions")
		})
	}
}
