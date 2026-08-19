//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestMySQLLiveRoleRoundTrip is the test the RoleManagement capability could
// not have been flipped without.
//
// The key was false with the recorded reason that Ptah cannot read or compare a
// MySQL-family role, and that reason is what this measures. The failure it
// describes is not a compile error but an apply loop planning the same CREATE
// ROLE forever, so only a live read can show it is gone.
//
// The discriminator is the part no offline assertion reaches. A role is a row
// in the same table as every user account, and reading without separating them
// would report every account on the server as a role -- and the first plan
// would then offer to drop them (stokaro/ptah#1762).
func TestMySQLLiveRoleRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MySQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	role := fmt.Sprintf("ptah_role_%d", time.Now().UnixNano()%1_000_000_000)
	table := fmt.Sprintf("ptah_role_t_%d", time.Now().UnixNano()%1_000_000_000)
	defer func() {
		_ = conn.Writer().ExecuteSQL(ctx, "DROP ROLE IF EXISTS `"+role+"`")
		_ = conn.Writer().ExecuteSQL(ctx, "DROP TABLE IF EXISTS `"+table+"`")
	}()
	c.Assert(conn.Writer().ExecuteSQL(ctx,
		"CREATE TABLE `"+table+"` (id INT PRIMARY KEY)"), qt.IsNil)

	declared := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: table}},
		Fields: []goschema.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
		Roles:  []goschema.Role{{StructName: "R", Name: role}},
	}

	// 1. The role is seen as missing, planned, and the statement runs.
	live, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	creation := schemadiff.CompareWithDialect(declared, live, conn.Info().Dialect)
	c.Assert(creation.RolesAdded, qt.Contains, role)

	statements, err := planner.GenerateSchemaDiffSQLStatements(creation, declared, conn.Info().Dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE ROLE")
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The read reports the role, and reports it as a ROLE rather than as one
	// of the accounts sitting in the same table.
	created, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(roleNames(created.Roles), qt.Contains, role)
	// root is an account that can log in. A read that could not tell the two
	// apart would carry it here, and the next plan would offer to drop it.
	c.Assert(roleNames(created.Roles), qt.Not(qt.Contains), "root")

	// 3. The convergence assertion. Comparing the same declaration against a
	// freshly read database must leave nothing to do.
	settled := schemadiff.CompareWithDialect(declared, created, conn.Info().Dialect)
	c.Assert(settled.RolesAdded, qt.HasLen, 0)
	c.Assert(settled.RolesModified, qt.HasLen, 0)
	c.Assert(settled.RolesRemoved, qt.HasLen, 0)
}

// TestMySQLLiveRoleAttributeIsRefusedByTheServerToo pins that the declaration
// this renderer refuses is one the server refuses as well.
//
// Without it the refusal is only this renderer's opinion, and an opinion that
// turned out to be wrong would be a capability withheld for no reason.
func TestMySQLLiveRoleAttributeIsRefusedByTheServerToo(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MySQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	t.Run("LOGIN is a syntax error", func(t *testing.T) {
		c := qt.New(t)

		err := conn.Writer().ExecuteSQL(ctx, "CREATE ROLE ptah_login_probe LOGIN")
		c.Assert(err, qt.IsNotNil)
	})

	t.Run("PASSWORD is a syntax error", func(t *testing.T) {
		c := qt.New(t)

		err := conn.Writer().ExecuteSQL(ctx, "CREATE ROLE ptah_pw_probe PASSWORD 'x'")
		c.Assert(err, qt.IsNotNil)
	})

	t.Run("the rendered form is accepted, twice", func(t *testing.T) {
		c := qt.New(t)

		role := fmt.Sprintf("ptah_twice_%d", time.Now().UnixNano()%1_000_000_000)
		defer func() { _ = conn.Writer().ExecuteSQL(ctx, "DROP ROLE IF EXISTS `"+role+"`") }()

		create := "CREATE ROLE IF NOT EXISTS `" + role + "`;"
		c.Assert(conn.Writer().ExecuteSQL(ctx, create), qt.IsNil)
		// The guard is unconditional in the rendered form, so it has to be
		// idempotent.
		c.Assert(conn.Writer().ExecuteSQL(ctx, create), qt.IsNil)
	})
}

// roleNames lists the names a read reported.
func roleNames(roles []dbschematypes.DBRole) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}
