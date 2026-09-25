//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// TestPostgresRoutineGrant_HCLDocumentConverges applies an HCL document that
// declares a function in a schema of its own, revokes PUBLIC's EXECUTE on it
// and grants EXECUTE to a role, then compares again. The function is named by
// its block and its argument types by `args`; the grant is right only if the
// reference resolves to the function's schema and the argument types match
// the catalog's, which only a server read can confirm.
func TestPostgresRoutineGrant_HCLDocumentConverges(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)
	document := fmt.Sprintf(`schema %[1]q {}

table "signatures" {
  schema = schema.%[1]s
  column "id" {
    type = bigint
  }
  primary_key {
    columns = [column.id]
  }
}

function "purge" {
  schema = schema.%[1]s
  lang = "SQL"
  return = "integer"
  arg "p_id" {
    type = uuid
  }
  security = "DEFINER"
  as = "SELECT 1"
}

revoke {
  from = "PUBLIC"
  for = function.purge
  args = ["uuid"]
  privileges = ["EXECUTE"]
}

permission {
  to = %[2]q
  for = function.purge
  args = ["uuid"]
  privileges = ["EXECUTE"]
}
`, fixture.schema, fixture.role)
	desired, err := atlashcl.Parse([]byte(document), "schema.hcl")
	c.Assert(err, qt.IsNil)

	fixture.apply(c, ctx, desired)
	settled := fixture.compare(c, ctx, desired)

	privileges := fixture.privileges(c, ctx)
	c.Assert(privileges.PublicExecutes, qt.IsFalse)
	c.Assert(privileges.RoleExecutes, qt.IsTrue)
	c.Assert(settled.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", settled.GrantsAdded))
	c.Assert(settled.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", settled.GrantsRemoved))
}
