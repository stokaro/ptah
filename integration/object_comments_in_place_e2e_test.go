//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The PostgreSQL family keeps an index's, a schema's and a role's comment as
// an object comment. A changed one is planned as one COMMENT ON and the
// second diff is in sync. Each engine is asked only for the comments it
// stores: CockroachDB has no COMMENT ON ROLE, and the Spanner PostgreSQL
// interface stores no index comment, so its render names the comment as
// skipped and the apply succeeds.

// commentedObjectsDocument declares the schema s with the table u and its
// index u_n_ix, each commented with comment, and the role named role with it
// too when role is not empty. An empty comment declares none.
func commentedObjectsDocument(role, comment string) string {
	var b strings.Builder
	b.WriteString("CREATE SCHEMA s;\n")
	b.WriteString("CREATE TABLE s.u (id bigint PRIMARY KEY, n bigint);\n")
	b.WriteString("CREATE INDEX u_n_ix ON s.u (n);\n")
	if role != "" {
		fmt.Fprintf(&b, "CREATE ROLE %s NOLOGIN;\n", role)
	}
	if comment == "" {
		return b.String()
	}
	fmt.Fprintf(&b, "COMMENT ON SCHEMA s IS '%s';\n", comment)
	fmt.Fprintf(&b, "COMMENT ON INDEX s.u_n_ix IS '%s';\n", comment)
	if role != "" {
		fmt.Fprintf(&b, "COMMENT ON ROLE %s IS '%s';\n", role, comment)
	}
	return b.String()
}

// commentEngine is one PostgreSQL-family engine. role names the role the
// test comments, or nothing on an engine that stores no role comment, and
// commented is how many objects then carry a comment.
type commentEngine struct {
	name      string
	server    func(c *qt.C) devDialectDatabase
	role      func(c *qt.C) string
	commented int
}

// postgresCommentsDatabase is a scratch PostgreSQL database.
func postgresCommentsDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	return devDialectDatabase{url: devURL, conn: connectDevDialect(c, devURL)}
}

var commentEngines = []commentEngine{
	{name: "PostgreSQL", server: postgresCommentsDatabase, role: scratchRole(dbtarget.PostgreSQL), commented: 3},
	{name: "YugabyteDB", server: yugabyteDevDatabase, role: scratchRole(dbtarget.YugabyteDB), commented: 3},
	{name: "CockroachDB", server: cockroachDevDatabase, role: noRole, commented: 2},
}

// noRole is the role an engine without COMMENT ON ROLE is asked about: none.
func noRole(*qt.C) string { return "" }

// scratchRole names a role unique to one test on engine, which drops it when
// the test ends: a role belongs to the cluster, so dropping the database
// leaves it behind. It is asked for before the scratch database, so the role
// goes after the database does: on YugabyteDB a DROP ROLE between them makes
// the DROP DATABASE answer that its catalog snapshot was invalidated.
func scratchRole(engine dbtarget.Engine) func(c *qt.C) string {
	return func(c *qt.C) string {
		c.Helper()
		admin := connectDevDialect(c, dbtarget.URL(c, engine))
		role := fmt.Sprintf("ptah_cm_%d", time.Now().UnixNano()%1_000_000_000)
		c.Cleanup(func() {
			_, err := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
			c.Check(err, qt.IsNil)
		})
		return role
	}
}

// heldComments reads back the comments of s, s.u_n_ix and role, as the
// catalog stores them.
func heldComments(c *qt.C, dev devDialectDatabase, role string) []string {
	c.Helper()
	queries := []string{
		"SELECT COALESCE(obj_description(oid, 'pg_namespace'), '') FROM pg_namespace WHERE nspname = 's'",
		"SELECT COALESCE(obj_description('s.u_n_ix'::regclass, 'pg_class'), '')",
	}
	if role != "" {
		queries = append(queries,
			"SELECT COALESCE(shobj_description(oid, 'pg_authid'), '') FROM pg_roles WHERE rolname = '"+role+"'")
	}
	comments := make([]string, 0, len(queries))
	for _, query := range queries {
		var comment string
		c.Assert(dev.conn.QueryRowContext(c.Context(), query).Scan(&comment), qt.IsNil, qt.Commentf("%s", query))
		comments = append(comments, comment)
	}
	return comments
}

// applyCommentedObjects writes the document for comment, applies it, and
// diffs the database against it again.
func applyCommentedObjects(c *qt.C, dev devDialectDatabase, role, comment string) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(commentedObjectsDocument(role, comment)), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
}

// TestSchemaApplyChangesObjectCommentsInPlaceE2E applies the objects with one
// comment, then with another, then with none, and reads each back. A schema's
// comment is compared only where the declaration states one, so the last
// apply leaves the schema's in place and removes the others.
func TestSchemaApplyChangesObjectCommentsInPlaceE2E(t *testing.T) {
	for _, engine := range commentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			role := engine.role(c)
			dev := engine.server(c)

			applyCommentedObjects(c, dev, role, "first")
			c.Assert(heldComments(c, dev, role), qt.DeepEquals, slices.Repeat([]string{"first"}, engine.commented))

			applyCommentedObjects(c, dev, role, "second")
			c.Assert(heldComments(c, dev, role), qt.DeepEquals, slices.Repeat([]string{"second"}, engine.commented))

			applyCommentedObjects(c, dev, role, "")
			c.Assert(heldComments(c, dev, role), qt.DeepEquals,
				append([]string{"second"}, slices.Repeat([]string{""}, engine.commented-1)...))
		})
	}
}

// TestSchemaApplySkipsAnIndexCommentOnSpannerE2E applies an index with a
// comment on Spanner, which stores none: the render names the comment as
// skipped, the index is created, and a second diff is in sync.
func TestSchemaApplySkipsAnIndexCommentOnSpannerE2E(t *testing.T) {
	c := qt.New(t)
	dev := spannerDevDatabase(c)
	c.Cleanup(func() {
		_, err := dev.conn.ExecContext(context.Background(), "DROP INDEX IF EXISTS u_n_ix")
		c.Check(err, qt.IsNil)
		_, err = dev.conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS u")
		c.Check(err, qt.IsNil)
	})
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE u (id bigint PRIMARY KEY, n bigint);\n"+
		"CREATE INDEX u_n_ix ON u (n);\nCOMMENT ON INDEX u_n_ix IS 'first';\n"), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "-- SPANNER: index comment u_n_ix is not supported by this target; skipped.")

	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced")
}
