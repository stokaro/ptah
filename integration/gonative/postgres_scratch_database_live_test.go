//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
)

// scratchPostgresDatabase creates a database of the test's own on the server
// dsn names, drops it when the test ends, and returns its DSN.
//
// A test that wants an empty "public" gets one here instead of dropping the
// shared database's. `DROP SCHEMA public CASCADE; CREATE SCHEMA public` on the
// shared database removed every other test's objects in it, and the schema it
// recreated was owned by the connecting role with no grant for PUBLIC: a role
// created afterwards had no usable schema in its default search path
// (stokaro/ptah#3657).
func scratchPostgresDatabase(c *qt.C, dsn string) string {
	c.Helper()
	admin, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_gonative_%d", time.Now().UnixNano())
	ident := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+ident)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		// WITH (FORCE), because a connection the test opened may outlive the
		// assertion that failed.
		_, dropErr := admin.ExecContext(context.WithoutCancel(c.Context()), "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	parsed, err := url.Parse(dsn)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	return parsed.String()
}
