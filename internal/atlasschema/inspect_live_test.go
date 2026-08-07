//go:build integration

package atlasschema_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// These cases pin the half of `schema inspect` a single-namespace fixture
// cannot express: which schemas the connection's scope puts in the document.
//
// The two scopes are reached by two URL spellings and both are exercised on
// the SAME database below, because a suite that always pins `search_path`
// cannot fail on realm scope — which is exactly how stokaro/ptah#1257 stayed
// invisible, and how the realm half of stokaro/ptah#1264 reached master.
//
// Every want string was produced by the pinned Atlas community binary v1.3.0
// against PostgreSQL 17 on 2026-08-07, through `--format '{{ json . }}'`, and
// is reproduced byte for byte. The database each row was measured on is named
// on the row.

// inspectLiveMultiSchema is the state that separates the two scopes: one table
// in the connection's own schema and one in a schema the URL never names.
var inspectLiveMultiSchema = []string{
	"CREATE SCHEMA extra",
	"CREATE TABLE public.a (id integer)",
	"CREATE TABLE extra.b (id integer)",
}

func TestInspectLive_ScopeSelectsSchemas(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		query   string
		setup   []string
		schemas []string
		want    string
	}{
		{
			// The issue's headline row. `extra` and its table were absent
			// entirely: the tool described less of the database than it found
			// and said nothing about it.
			name:  "plain URL describes every schema",
			setup: inspectLiveMultiSchema,
			want: `{"schemas":[{"name":"extra","tables":[{"name":"b","columns":[{"name":"id","type":"integer",` +
				`"null":true}]}]},{"name":"public","tables":[{"name":"a","columns":[{"name":"id","type":"integer",` +
				`"null":true}]}],"comment":"standard public schema"}]}`,
		},
		{
			// The control for the row above, on the same state. A URL that
			// pins a schema keeps describing exactly that schema, so the realm
			// fix cannot be a fix that stopped filtering.
			name:  "search_path keeps the document at one schema",
			query: "search_path=public",
			setup: inspectLiveMultiSchema,
			want: `{"schemas":[{"name":"public","tables":[{"name":"a","columns":[{"name":"id","type":"integer",` +
				`"null":true}]}],"comment":"standard public schema"}]}`,
		},
		{
			// An empty database reports the schema it has, at both scopes. It
			// used to report `{}`, so anything walking `.schemas` broke on a
			// database that was merely empty.
			name: "empty database reports its schema at realm scope",
			want: `{"schemas":[{"name":"public","comment":"standard public schema"}]}`,
		},
		{
			name:  "empty database reports its schema at schema scope",
			query: "search_path=public",
			want:  `{"schemas":[{"name":"public","comment":"standard public schema"}]}`,
		},
		{
			// --schema outranks the URL's realm scope.
			name:    "an explicit schema narrows realm scope",
			setup:   inspectLiveMultiSchema,
			schemas: []string{"extra"},
			want: `{"schemas":[{"name":"extra","tables":[{"name":"b","columns":[{"name":"id","type":"integer",` +
				`"null":true}]}]}]}`,
		},
		{
			// The control that stops "an empty database reports its schema"
			// from becoming "every database reports a schema": a selection that
			// matched nothing is an empty document on both implementations.
			name:    "a schema that does not exist stays an empty document",
			setup:   inspectLiveMultiSchema,
			schemas: []string{"nope"},
			want:    `{}`,
		},
		{
			// A schema comment is carried wherever the catalog has one, not
			// only on `public`.
			name: "schema comments are carried",
			setup: []string{
				"CREATE SCHEMA app",
				"COMMENT ON SCHEMA app IS 'app schema comment'",
			},
			schemas: []string{"app"},
			want:    `{"schemas":[{"name":"app","comment":"app schema comment"}]}`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			conn := newInspectLiveConnection(c, ctx, test.query, test.setup)

			rendered, err := atlasschema.Inspect(ctx, conn, atlasschema.InspectOptions{
				Format:  "json",
				Schemas: test.schemas,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

// TestInspectLive_HCLRealmScope records what the HCL surface does with the same
// two URL forms.
//
// It is the same scope question in the other rendering, and the fix reaches it
// through the same reader: a plain URL now renders a `schema` block for every
// schema, and the tables of a schema the URL never named. Only the blocks this
// issue is about are asserted — the compatibility surface also renders roles
// and permissions the pinned binary does not model, which is Ptah describing
// more of the database and is not what this test is for.
func TestInspectLive_HCLRealmScope(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		query   string
		want    []string
		notWant []string
	}{
		{
			name: "plain URL renders both schemas and both tables",
			want: []string{
				`schema "extra" {`,
				`schema "public" {`,
				`table "a" {`,
				`table "b" {`,
			},
		},
		{
			name:  "search_path renders only the schema it pins",
			query: "search_path=public",
			want: []string{
				`schema "public" {`,
				`table "a" {`,
			},
			notWant: []string{
				`schema "extra" {`,
				`table "b" {`,
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			conn := newInspectLiveConnection(c, ctx, test.query, inspectLiveMultiSchema)

			rendered, err := atlasschema.Inspect(ctx, conn, atlasschema.InspectOptions{Format: "hcl"})

			c.Assert(err, qt.IsNil)
			for _, block := range test.want {
				c.Assert(rendered, qt.Contains, block)
			}
			for _, block := range test.notWant {
				c.Assert(rendered, qt.Not(qt.Contains), block)
			}
		})
	}
}

// newInspectLiveConnection provisions a throwaway database in the state the
// case is about, so a case that creates schemas and tables cannot disturb any
// other live test sharing the server.
//
// query is appended to the URL and is what selects the scope. Passing "" is the
// plain URL the compatibility documentation uses, and it is a fixture in its
// own right.
//
// setup runs through a separate connection that is closed again before the
// connection under test is opened, which is the real order: the database is
// already in that state when a run reaches it.
func newInspectLiveConnection(
	c *qt.C,
	ctx context.Context,
	query string,
	setup []string,
) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := requireInspectLiveURL(c)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_inspect_%d", time.Now().UnixNano())
	nameIdent := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+nameIdent)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(ctx),
			"DROP DATABASE IF EXISTS "+nameIdent+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	applyInspectLiveFixture(c, ctx, parsed.String(), setup)

	parsed.RawQuery = inspectLiveQuery(parsed.RawQuery, query)
	conn, err := dbschema.ConnectToDatabase(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// applyInspectLiveFixture puts the throwaway database into the state under test
// through a connection of its own.
func applyInspectLiveFixture(c *qt.C, ctx context.Context, dbURL string, statements []string) {
	c.Helper()
	if len(statements) == 0 {
		return
	}
	seed, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(seed.Close(), qt.IsNil) }()
	for _, statement := range statements {
		_, execErr := seed.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// inspectLiveQuery joins the admin URL's own parameters, such as sslmode, with
// the fixture's.
func inspectLiveQuery(base, extra string) string {
	switch {
	case base == "":
		return extra
	case extra == "":
		return base
	default:
		return base + "&" + extra
	}
}

func requireInspectLiveURL(c *qt.C) string {
	c.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL"} {
		raw := os.Getenv(name)
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		c.Assert(err, qt.IsNil)
		parsed.Scheme = "postgres"
		return parsed.String()
	}
	c.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	return ""
}
