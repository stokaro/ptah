//go:build integration

package atlas_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// TestSchemaApplyRoleMembershipBlockPostgresLive applies an HCL document whose
// role states the options of its membership in a `membership` block, and reads
// the options back from the server. The community binary accepts such a
// document and plans no role at all, so what this holds is the capability the
// compatibility surface keeps: the block reaches a statement, and the server
// holds what the block stated.
//
// The two options differ from the defaults on purpose. A read-back that showed
// the defaults would mean the clauses never ran.
func TestSchemaApplyRoleMembershipBlockPostgresLive(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL16)
	targetURL, devURL := createRLSSpellingDatabases(t, adminURL)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	group, member := "ptah_blk_grp_"+suffix, "ptah_blk_mem_"+suffix

	schemaPath := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, fmt.Appendf(nil, `schema "public" {}

role %[1]q {
  login = false
}

role %[2]q {
  login = true
  membership {
    role    = role.%[1]s
    admin   = true
    inherit = false
  }
}
`, group, member), 0o600), qt.IsNil)

	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+member+`"`)
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+group+`"`)
		dbschema.CloseAndWarn(admin)
	})

	output, err := runCompatSchemaApply(targetURL, devURL, schemaPath)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))

	var adminOption, inheritOption, setOption bool
	row := admin.QueryRowContext(c.Context(),
		`SELECT m.admin_option, m.inherit_option, m.set_option
		   FROM pg_auth_members m
		  WHERE m.roleid = $1::regrole AND m.member = $2::regrole`, group, member)
	c.Assert(row.Scan(&adminOption, &inheritOption, &setOption), qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(adminOption, qt.IsTrue)
	c.Assert(inheritOption, qt.IsFalse)
	c.Assert(setOption, qt.IsTrue)

	again, err := runCompatSchemaApply(targetURL, devURL, schemaPath)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", again))
	c.Assert(again, qt.Contains, "Schema is synced, no changes to be made")
}
