//go:build integration

package dblock_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// Oracle's lock table is named without a schema, so it lives in the lock
// session's current schema, and the migrator qualifies its revision table with
// the current schema it read on the same pool. Two accounts that migrate one
// schema therefore lock one row only because no Oracle URL can name a schema
// other than the session's own: go-ora refuses the option before the
// connection opens. A connector that let ?schema= through would put the
// revision table in the named schema and leave each account's lock in its own,
// so the second account would take the lock the first holds. This test fails
// first when that changes, and the lock table then has to be qualified with
// the named schema.
func TestOracleURLNamingASchemaLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("schema", "SYSTEM")
	parsed.RawQuery = query.Encode()

	conn, err := dbschema.ConnectToDatabase(c.Context(), parsed.String())

	c.Assert(err, qt.ErrorMatches, `(?s).*unknown URL option: schema.*`)
	c.Assert(conn, qt.IsNil)
}
