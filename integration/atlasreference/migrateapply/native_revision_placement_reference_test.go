//go:build integration

package migrateapply_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// Native `ptah migrations up --revision-format atlas` and the pinned binary
// have to find each other's history on PostgreSQL through the plain URL an
// operator hands both. The pinned binary keeps its table in the
// atlas_schema_revisions schema there, and a native run that kept it in the
// connection's schema reported the other's database as never migrated, in
// both directions (stokaro/ptah#3534).

const (
	placementFirst  = "20260101000000_a.sql"
	placementSecond = "20260101000001_b.sql"
)

func placementFiles() map[string]string {
	return map[string]string{
		placementFirst:  "CREATE TABLE placement_a (id integer PRIMARY KEY);\n",
		placementSecond: "CREATE TABLE placement_b (id integer PRIMARY KEY);\n",
	}
}

// newPlacementPostgres creates a throwaway database and returns its plain URL,
// the spelling that pins no schema.
func newPlacementPostgres(c *qt.C) string {
	c.Helper()
	ctx := context.Background()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_reference_placement_%d", time.Now().UnixNano())
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
	return parsed.String()
}

func nativeAtlasArgs(verb, dir, dbURL string) []string {
	return []string{
		"migrations", verb,
		"--migrations-dir", dir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--db-url", dbURL,
	}
}

// TestReferenceReadsANativeAtlasHistoryOnPostgres is the first direction: the
// pinned binary reads what native Ptah wrote as applied, and has nothing left
// to run.
func TestReferenceReadsANativeAtlasHistoryOnPostgres(t *testing.T) {
	reference := requireAtlasReference(t)
	c := qt.New(t)
	native := clirun.Build(c, clirun.Ptah)
	dir := writeReferenceMigrationDir(c, reference, placementFiles())
	dbURL := newPlacementPostgres(c)

	up := runCommand(c, native, nativeAtlasArgs("up", dir, dbURL)...)
	c.Assert(up.code, qt.Equals, 0, qt.Commentf("native up output: %s", up.output))

	status := runCommand(c, reference, "migrate", "status", "--dir", "file://"+dir, "--url", dbURL)
	c.Assert(status.code, qt.Equals, 0, qt.Commentf("reference status output: %s", status.output))
	c.Assert(status.output, qt.Contains, "Migration Status: OK")
	c.Assert(status.output, qt.Contains, "Current Version: 20260101000001")
	c.Assert(status.output, qt.Contains, "Executed Files:  2")

	apply := runCommand(c, reference, "migrate", "apply", "--dir", "file://"+dir, "--url", dbURL)
	c.Assert(apply.code, qt.Equals, 0, qt.Commentf("reference apply output: %s", apply.output))
	c.Assert(apply.output, qt.Contains, "No migration files to execute")
}

// TestNativeReadsAReferenceAtlasHistoryOnPostgres is the other direction. A
// native run that missed the history would apply the directory again, and the
// first CREATE TABLE would fail on the table the pinned binary created.
func TestNativeReadsAReferenceAtlasHistoryOnPostgres(t *testing.T) {
	reference := requireAtlasReference(t)
	c := qt.New(t)
	native := clirun.Build(c, clirun.Ptah)
	dir := writeReferenceMigrationDir(c, reference, placementFiles())
	dbURL := newPlacementPostgres(c)

	apply := runCommand(c, reference, "migrate", "apply", "--dir", "file://"+dir, "--url", dbURL)
	c.Assert(apply.code, qt.Equals, 0, qt.Commentf("reference apply output: %s", apply.output))

	status := runCommand(c, native, append(nativeAtlasArgs("status", dir, dbURL), "--json")...)
	c.Assert(status.code, qt.Equals, 0, qt.Commentf("native status output: %s", status.output))
	c.Assert(status.output, qt.Contains, `"current_version": 20260101000001`)
	c.Assert(status.output, qt.Contains, `"pending_migrations": []`)

	up := runCommand(c, native, nativeAtlasArgs("up", dir, dbURL)...)
	c.Assert(up.code, qt.Equals, 0, qt.Commentf("native up output: %s", up.output))
}
