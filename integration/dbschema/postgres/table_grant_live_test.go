//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// These tests read back a table that carries a GRANT (stokaro/ptah#3589).
// CockroachDB reports the grantor of every table grant as NULL in
// information_schema.role_table_grants, and scanning it into a string failed
// the whole schema read, so nothing that reads a database -- db read, schema
// compare, a plan -- got past a table with a grant on it. PostgreSQL reports a
// grantor and is the control.

var tableGrantEngines = []struct {
	name   string
	engine dbtarget.Engine
	// grantorNamed says whether the engine names the grantor of a table
	// grant. PostgreSQL does; CockroachDB leaves it NULL, which reads as empty.
	grantorNamed bool
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL, grantorNamed: true},
	{name: "CockroachDB", engine: dbtarget.CockroachDB, grantorNamed: false},
}

// tableGrantFixture is one throwaway schema holding a table granted to one
// role.
type tableGrantFixture struct {
	conn   *dbschema.DatabaseConnection
	schema string
	role   string
}

func newTableGrantFixture(c *qt.C, engine dbtarget.Engine) tableGrantFixture {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	f := tableGrantFixture{
		conn:   conn,
		schema: fmt.Sprintf("ptah_table_grant_%d", suffix),
		role:   fmt.Sprintf("ptah_table_grant_%d", suffix),
	}
	c.Cleanup(func() {
		cleanup := context.Background()
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + pgx.Identifier{f.schema}.Sanitize() + " CASCADE",
			"DROP ROLE IF EXISTS " + pgx.Identifier{f.role}.Sanitize(),
		} {
			_, dropErr := conn.ExecContext(cleanup, statement)
			c.Check(dropErr, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
		c.Check(conn.Close(), qt.IsNil)
	})
	table := pgx.Identifier{f.schema, "docs"}.Sanitize()
	for _, statement := range []string{
		"CREATE SCHEMA " + pgx.Identifier{f.schema}.Sanitize(),
		"CREATE ROLE " + pgx.Identifier{f.role}.Sanitize(),
		"CREATE TABLE " + table + " (id integer PRIMARY KEY)",
		"GRANT SELECT ON " + table + " TO " + pgx.Identifier{f.role}.Sanitize(),
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return f
}

func (f tableGrantFixture) read(c *qt.C) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	return live
}

// tableGrantRead is the part of a read grant these tests are about. The
// grantor is the connecting user, which depends on the URL, so only whether one
// was named is kept.
type tableGrantRead struct {
	Privilege, Schema, ObjectName string
	GrantorNamed                  bool
}

// grantsTo keeps the table grants to role. The connecting user also holds the
// owner's privileges on the table, and which user that is depends on the URL.
func grantsTo(grants []catalog.Grant, role string) []tableGrantRead {
	var kept []tableGrantRead
	for _, grant := range grants {
		if grant.Role != role || grant.ObjectType != "TABLE" {
			continue
		}
		kept = append(kept, tableGrantRead{
			Privilege:    grant.Privilege,
			Schema:       grant.Schema,
			ObjectName:   grant.ObjectName,
			GrantorNamed: grant.GrantedBy != "",
		})
	}
	return kept
}

// TestTableGrant_LiveReadReportsTheGrant reads the table back and finds the
// grant it carries.
func TestTableGrant_LiveReadReportsTheGrant(t *testing.T) {
	for _, engine := range tableGrantEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTableGrantFixture(c, engine.engine)

			live := f.read(c)

			c.Assert(grantsTo(live.Grants, f.role), qt.DeepEquals, []tableGrantRead{
				{Privilege: "SELECT", Schema: f.schema, ObjectName: "docs", GrantorNamed: engine.grantorNamed},
			})
		})
	}
}

// TestTableGrant_LiveSchemaFileComparesEqual compares a schema file that
// declares the same table and grant with the database. Nothing is planned for
// the grant.
func TestTableGrant_LiveSchemaFileComparesEqual(t *testing.T) {
	for _, engine := range tableGrantEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTableGrantFixture(c, engine.engine)
			body := fmt.Sprintf("CREATE TABLE %[1]s.docs (id integer PRIMARY KEY);\nGRANT SELECT ON %[1]s.docs TO %[2]s;\n", f.schema, f.role)
			path := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
			dialect := f.conn.Info().Dialect
			desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: dialect})
			c.Assert(err, qt.IsNil)

			diff := schemadiff.CompareWithDialect(desired, f.read(c), dialect)

			c.Assert(diff.GrantsAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.GrantsAdded))
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.GrantsRemoved))
		})
	}
}
