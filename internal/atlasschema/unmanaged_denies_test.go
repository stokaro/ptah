package atlasschema_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/internal/atlasschema"
)

// TestReportUnmanagedDenies_HappyPath pins the line written for each DENY the
// plan keeps, on a whole object and on a column, and that a run with no
// diagnostics writer is quiet.
func TestReportUnmanagedDenies_HappyPath(t *testing.T) {
	denies := []difftypes.GrantRef{
		{Role: "r", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t"},
		{Role: "public", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "dbo.t", Column: "secret"},
	}
	t.Run("lines", func(t *testing.T) {
		c := qt.New(t)
		var out bytes.Buffer

		atlasschema.ReportUnmanagedDenies(&out, denies)

		c.Assert(out.String(), qt.Equals,
			`UNMANAGED: the database denies SELECT on TABLE dbo.t to "r", and the schema does not declare the DENY, `+
				"so the plan does not revoke it; declare it to manage it, or revoke it by hand\n"+
				`UNMANAGED: the database denies UPDATE on TABLE dbo.t (secret) to "public", and the schema does not declare the DENY, `+
				"so the plan does not revoke it; declare it to manage it, or revoke it by hand\n")
	})
	t.Run("no writer", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(func() { atlasschema.ReportUnmanagedDenies(nil, denies) }, qt.Not(qt.PanicMatches), ".*")
	})
}

// TestReportRollbackUnmanagedDenies_HappyPath pins the line a derived rollback
// writes for each DENY it leaves in place: it names the DENY, says the
// rollback cannot give back the grant it replaced, and says the list holds
// every such DENY rather than only the ones the rolled-back migrations added.
func TestReportRollbackUnmanagedDenies_HappyPath(t *testing.T) {
	denies := []difftypes.GrantRef{
		{Role: "r", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t"},
		{Role: "public", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "dbo.t", Column: "secret"},
	}
	t.Run("lines", func(t *testing.T) {
		c := qt.New(t)
		var out bytes.Buffer

		atlasschema.ReportRollbackUnmanagedDenies(&out, denies)

		c.Assert(out.String(), qt.Equals,
			`UNMANAGED: the database denies SELECT on TABLE dbo.t to "r", and the target version does not declare the DENY, `+
				"so the derived rollback leaves it and cannot give back a grant it replaced; this lists every such DENY, "+
				"not only the ones the rolled-back migrations added, so revoke it by hand if one of them added it\n"+
				`UNMANAGED: the database denies UPDATE on TABLE dbo.t (secret) to "public", and the target version does not declare the DENY, `+
				"so the derived rollback leaves it and cannot give back a grant it replaced; this lists every such DENY, "+
				"not only the ones the rolled-back migrations added, so revoke it by hand if one of them added it\n")
	})
	t.Run("no writer", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(func() { atlasschema.ReportRollbackUnmanagedDenies(nil, denies) }, qt.Not(qt.PanicMatches), ".*")
	})
}
