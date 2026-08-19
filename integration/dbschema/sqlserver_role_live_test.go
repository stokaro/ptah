//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestSQLServerLiveRoleAndGrantRoundTrip is the test the RoleManagement
// capability could not have been flipped without.
//
// The key promises three things at once -- Ptah renders the object, reads it
// back, and plans it again -- and the failure mode when one is missing is not a
// compile error. It is a plan that reports the same pending change forever,
// because the reader never sees what the renderer made. No offline test can
// catch that: the fixture on both sides is written by the same hand
// (stokaro/ptah#1698).
func TestSQLServerLiveRoleAndGrantRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	role := fmt.Sprintf("ptah_role_%d", suffix)
	table := fmt.Sprintf("ptah_rt_%d", suffix)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS [dbo]."+quoteSQLServerIdentifier(table))
		_, _ = conn.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoteSQLServerIdentifier(role))
	}()

	description := sqlServerRoleSchema(role, table)

	// 1. The renderer's statements are the ones the server is given. Nothing is
	// hand-written, so a statement this engine refuses fails the test.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "CREATE ROLE")
	c.Assert(rendered, qt.Contains, "GRANT")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it holds.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)

	held, withOption := grantsHeldBy(live.Grants, role)
	c.Assert(held["SELECT"], qt.IsTrue)
	c.Assert(held["INSERT"], qt.IsTrue)
	c.Assert(withOption["INSERT"], qt.IsTrue)
	c.Assert(withOption["SELECT"], qt.IsFalse)

	names := map[string]bool{}
	for _, readRole := range live.Roles {
		names[readRole.Name] = true
	}
	c.Assert(names[role], qt.IsTrue)
	// public is a role in every database and cannot be dropped. A reader that
	// reported it would have the comparator plan a DROP ROLE the engine refuses.
	c.Assert(names["public"], qt.IsFalse)

	// 3. The convergence assertion, which is what the capability key is really
	// about. Comparing the same description against what the server now holds
	// must produce nothing to do.
	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(rolesNamed(settled.RolesAdded, role), qt.HasLen, 0)
	c.Assert(rolesNamed(settled.RolesRemoved, role), qt.HasLen, 0)
	c.Assert(settled.RolesModified, qt.HasLen, 0)
	c.Assert(grantsFor(settled.GrantsAdded, role), qt.HasLen, 0)
	c.Assert(grantsFor(settled.GrantsRemoved, role), qt.HasLen, 0)
	c.Assert(grantsFor(settled.GrantOptionsAdded, role), qt.HasLen, 0)
	c.Assert(grantsFor(settled.GrantOptionsRevoked, role), qt.HasLen, 0)
}

// TestSQLServerLiveRoleRefusesWhatTheRendererDeclines pins that the spellings
// the renderer reports or refuses are ones the engine really refuses, rather
// than rules this repository invented.
func TestSQLServerLiveRoleRefusesWhatTheRendererDeclines(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	role := fmt.Sprintf("ptah_refused_role_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(role)
	_, err = conn.ExecContext(ctx, "CREATE ROLE "+quoted)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoted)
		_, _ = conn.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoteSQLServerIdentifier(role+"_login"))
	}()

	tests := []struct {
		name      string
		statement string
		wantError string
	}{
		{
			name:      "a database role cannot log in",
			statement: "CREATE ROLE " + quoteSQLServerIdentifier(role+"_login") + " LOGIN",
			wantError: "Incorrect syntax near 'LOGIN'",
		},
		{
			name:      "USAGE is not a T-SQL privilege",
			statement: "GRANT USAGE ON SCHEMA::[dbo] TO " + quoted,
			wantError: "Incorrect syntax near 'USAGE'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, execErr := conn.ExecContext(ctx, test.statement)
			c.Assert(execErr, qt.IsNotNil)
			c.Assert(execErr.Error(), qt.Contains, test.wantError)
		})
	}

	// The hazard that has no error at all: a schema grant written without the
	// SCHEMA:: prefix resolves the name as an OBJECT. With no table of that
	// name the server says so; with one, the grant lands on the table instead,
	// which is why the renderer always writes the prefix.
	_, bare := conn.ExecContext(ctx, "GRANT SELECT ON [dbo] TO "+quoted)
	c.Assert(bare, qt.IsNotNil)
	c.Assert(bare.Error(), qt.Contains, "Cannot find the object")

	// The control: the spelling the renderer does emit is accepted.
	_, err = conn.ExecContext(ctx, "GRANT SELECT ON SCHEMA::[dbo] TO "+quoted)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "REVOKE SELECT ON SCHEMA::[dbo] FROM "+quoted)
	c.Assert(err, qt.IsNil)
}

// sqlServerRoleSchema declares one attribute-free role, a table, and two grants
// on it -- one plain, one carrying the grant option.
func sqlServerRoleSchema(role, table string) *goschema.Database {
	return &goschema.Database{
		Roles:  []goschema.Role{{StructName: "Access", Name: role, Inherit: true}},
		Tables: []goschema.Table{{StructName: "T", Name: table, Schema: "dbo"}},
		Fields: []goschema.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
		Grants: []goschema.Grant{
			{StructName: "Access", Role: role, Privileges: []string{"SELECT"}, OnTable: "dbo." + table},
			{
				StructName: "Access", Role: role, Privileges: []string{"INSERT"},
				OnTable: "dbo." + table, WithOption: true,
			},
		},
	}
}

func rolesNamed(names []string, role string) []string {
	kept := make([]string, 0)
	for _, name := range names {
		if name == role {
			kept = append(kept, name)
		}
	}
	return kept
}

func grantsFor(refs []difftypes.GrantRef, role string) []difftypes.GrantRef {
	kept := make([]difftypes.GrantRef, 0)
	for _, ref := range refs {
		if ref.Role == role {
			kept = append(kept, ref)
		}
	}
	return kept
}

// grantRow returns the row a role holds for one privilege, or nil.
func grantRow(grants []dbschematypes.DBGrant, role, privilege string) *dbschematypes.DBGrant {
	for i := range grants {
		if grants[i].Role == role && grants[i].Privilege == privilege {
			return &grants[i]
		}
	}
	return nil
}

// TestSQLServerLiveReaderClassifiesDenyAndSchemaGrants covers the two rows no
// declaration can produce, and which the round trip above therefore never
// reaches.
//
// A DENY subtracts a privilege from a broader grant, and Ptah plans no such
// shape. A schema grant needs the SCHEMA:: target the reader has to classify as
// something other than a table. Both are applied out of band here, because a
// mutation sweep showed nothing else reaches them: a reader that called every
// permission class a table grant, and one that read DENY as a grant, both
// survived until this existed (stokaro/ptah#1698).
func TestSQLServerLiveReaderClassifiesDenyAndSchemaGrants(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	role := fmt.Sprintf("ptah_deny_role_%d", suffix)
	table := fmt.Sprintf("ptah_dt_%d", suffix)
	quotedRole := quoteSQLServerIdentifier(role)
	quotedTable := "[dbo]." + quoteSQLServerIdentifier(table)
	defer func() {
		_, _ = conn.ExecContext(ctx, "REVOKE EXECUTE ON SCHEMA::[dbo] FROM "+quotedRole)
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quotedTable)
		_, _ = conn.ExecContext(ctx, "DROP ROLE IF EXISTS "+quotedRole)
	}()

	for _, statement := range []string{
		"CREATE ROLE " + quotedRole,
		"CREATE TABLE " + quotedTable + " ([id] INT PRIMARY KEY)",
		"GRANT SELECT ON " + quotedTable + " TO " + quotedRole,
		"DENY DELETE ON " + quotedTable + " TO " + quotedRole,
		"GRANT EXECUTE ON SCHEMA::[dbo] TO " + quotedRole,
	} {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)

	// The DENY is reported rather than dropped, and marked as the subtraction
	// it is, so a live validation can see that effective privileges are
	// narrower than the grant rows alone would say.
	denied := grantRow(live.Grants, role, "DELETE")
	c.Assert(denied, qt.IsNotNil)
	c.Assert(denied.IsPartialRevoke, qt.IsTrue)

	// A schema grant keeps its own object type. Calling it a table grant would
	// compare it against a table of that name.
	schemaGrant := grantRow(live.Grants, role, "EXECUTE")
	c.Assert(schemaGrant, qt.IsNotNil)
	c.Assert(schemaGrant.ObjectType, qt.Equals, "SCHEMA")
	c.Assert(schemaGrant.ObjectName, qt.Equals, "dbo")

	// The control on both: a plain table grant is neither.
	plain := grantRow(live.Grants, role, "SELECT")
	c.Assert(plain, qt.IsNotNil)
	c.Assert(plain.IsPartialRevoke, qt.IsFalse)
	c.Assert(plain.ObjectType, qt.Equals, "TABLE")

	// And the comparator does not plan a REVOKE of the DENY. It is not a grant
	// to revoke: the role already does not hold that privilege, and revoking it
	// would remove the exception instead.
	description := &goschema.Database{
		Roles:  []goschema.Role{{StructName: "A", Name: role, Inherit: true}},
		Grants: []goschema.Grant{{StructName: "A", Role: role, Privileges: []string{"SELECT"}, OnTable: "dbo." + table}},
	}
	diff := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	for _, ref := range grantsFor(diff.GrantsRemoved, role) {
		c.Assert(ref.Privilege, qt.Not(qt.Equals), "DELETE")
	}
}

// grantsHeldBy collects what one role holds, and which of those carry the grant
// option.
func grantsHeldBy(grants []dbschematypes.DBGrant, role string) (held, withOption map[string]bool) {
	held = map[string]bool{}
	withOption = map[string]bool{}
	for _, grant := range grants {
		if grant.Role != role {
			continue
		}
		held[grant.Privilege] = true
		withOption[grant.Privilege] = grant.WithOption
	}
	return held, withOption
}
