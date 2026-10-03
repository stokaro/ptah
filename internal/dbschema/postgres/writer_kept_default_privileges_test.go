package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aclitem"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/pgdefaultacl"
)

// keptDefaultRowsMarker is what the read of the rows a cleanup keeps selects
// and no other statement the writer sends does: the schema-scoped read's
// empty built-in list.
const keptDefaultRowsMarker = "'[]' AS builtin"

// keptDefaultRowsStayGoneQuery plays a server whose schema holds nothing and
// whose pg_default_acl never shows the kept row, whatever the cleanup runs:
// the statement that sets it back is accepted and does not take effect.
func keptDefaultRowsStayGoneQuery(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(query, keptDefaultRowsMarker) {
		return dbtest.QueryResult{Columns: []string{"nspname", "grantor", "object_type", "acl", "builtin"}}, nil
	}
	return postgresCleanupCatalogQuery(query, "PostgreSQL 18.0", nil)
}

// TestDropAllTablesKeeping_FailurePath runs a cleanup that keeps a default
// privilege the server does not show again after the cleanup set it back.
// The check that follows the statements refuses the cleanup, naming the
// default, and the transaction is rolled back rather than committed with
// the dev database's environment missing.
func TestDropAllTablesKeeping_FailurePath(t *testing.T) {
	t.Run("a kept default privilege the cleanup did not set back", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, keptDefaultRowsStayGoneQuery)
		kept := dbreset.DefaultPrivileges{
			Scope: dbreset.DefaultPrivilegeScope{Schemas: []string{"public"}},
			Rows: []pgdefaultacl.Row{{
				Schema: "public", Grantor: "o", ObjectType: "TABLES",
				ACL: []aclitem.Item{{Grantee: "app", Grantor: "o", Privileges: []aclitem.Privilege{{Name: "SELECT"}}}},
			}},
		}

		err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropAllTablesKeeping(c.Context(), dbreset.Kept{DefaultPrivileges: kept})

		c.Assert(err, qt.ErrorMatches, `PostgreSQL cleanup left default privilege "o/r/app" in schema "public" `+
			`different from what the dev database held when it was claimed`)
		c.Assert(db.CommitCount(), qt.Equals, 0)
	})
}
