//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestPostgresLiveRoleReadBySchemaOwnerWithoutProtectedCatalogAccess proves
// that a least-privileged schema owner can read and plan its ordinary schema
// without being granted access to protected role-password metadata.
//
// PostgreSQL's public role view returns the same masked value for a role with
// a password and a role without one. Only the protected catalog can separate
// those states, so the least-privilege read must report both as unknown. The
// role comment remains public metadata and must not disappear with the
// protected join.
func TestPostgresLiveRoleReadBySchemaOwnerWithoutProtectedCatalogAccess(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })

	suffix := time.Now().UnixNano()
	ownerName := fmt.Sprintf("ptah_lpr_owner_%d", suffix)
	roleName := fmt.Sprintf("ptah_lpr_role_%d", suffix)
	schemaName := fmt.Sprintf("ptah_lpr_schema_%d", suffix)
	const password = "PtahLeastPrivilege_42"
	const roleComment = "least-privilege role comment"

	c.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		c.Check(cleanupPostgresLeastPrivilegeRoles(
			cleanupCtx, admin, schemaName, ownerName, roleName,
		), qt.IsNil)
	})

	_, err = admin.ExecContext(ctx, fmt.Sprintf(`
		CREATE ROLE %s WITH LOGIN PASSWORD '%s'
			NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
		CREATE ROLE %s WITH NOLOGIN
			NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
		CREATE SCHEMA %s AUTHORIZATION %s;
		COMMENT ON ROLE %s IS '%s'`,
		postgresIdentifier(ownerName), password,
		postgresIdentifier(roleName),
		postgresIdentifier(schemaName), postgresIdentifier(ownerName),
		postgresIdentifier(roleName), roleComment,
	))
	c.Assert(err, qt.IsNil)

	ownerURL := postgresURLWithCredentials(c, adminURL, ownerName, password)
	owner, err := dbschema.ConnectToDatabase(ctx, ownerURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(owner) })

	_, err = owner.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s.documents (id INTEGER PRIMARY KEY)",
		postgresIdentifier(schemaName),
	))
	c.Assert(err, qt.IsNil)

	var protectedCatalogReadable bool
	err = owner.QueryRowContext(ctx, `SELECT has_table_privilege(
		current_user, 'pg_catalog.pg_authid', 'SELECT'
	)`).Scan(&protectedCatalogReadable)
	c.Assert(err, qt.IsNil)
	c.Assert(protectedCatalogReadable, qt.IsFalse)

	maskedPresent := publicRolePassword(c, ctx, owner, ownerName)
	maskedAbsent := publicRolePassword(c, ctx, owner, roleName)
	c.Assert(maskedPresent, qt.Equals, "********")
	c.Assert(maskedAbsent, qt.Equals, maskedPresent)

	described, err := dbschema.ReadSchemaWithSchemasContext(ctx, owner, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(described.Tables, qt.HasLen, 1)

	ownerRole := catalogRoleByName(c, described.RolesOutOfScope, ownerName)
	passwordlessRole := catalogRoleByName(c, described.RolesOutOfScope, roleName)
	c.Assert(ownerRole.PasswordState, qt.Equals, catalog.RolePasswordUnknown)
	c.Assert(passwordlessRole.PasswordState, qt.Equals, catalog.RolePasswordUnknown)
	c.Assert(passwordlessRole.Comment, qt.Equals, roleComment)

	declared := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: schemaName}},
		Tables: []schemamodel.Table{{
			StructName: "Document",
			Name:       "documents",
			Schema:     schemaName,
		}},
		Fields: []schemamodel.Field{{
			StructName: "Document",
			Name:       "id",
			Type:       "INTEGER",
			Primary:    true,
		}},
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, described, owner.Info(), nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, owner.Info().Dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 0)
}

func postgresURLWithCredentials(c *qt.C, address, user, password string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}

func publicRolePassword(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	roleName string,
) string {
	c.Helper()
	var password string
	err := conn.QueryRowContext(ctx,
		"SELECT rolpassword FROM pg_roles WHERE rolname = $1",
		roleName,
	).Scan(&password)
	c.Assert(err, qt.IsNil)
	return password
}

func catalogRoleByName(c *qt.C, roles []catalog.Role, name string) catalog.Role {
	c.Helper()
	for _, role := range roles {
		if role.Name == name {
			return role
		}
	}
	c.Fatalf("role %q was not read", name)
	return catalog.Role{}
}

func cleanupPostgresLeastPrivilegeRoles(
	ctx context.Context,
	admin *dbschema.DatabaseConnection,
	schemaName, ownerName, roleName string,
) error {
	_, err := admin.ExecContext(ctx, fmt.Sprintf(`
		DROP SCHEMA IF EXISTS %s CASCADE;
		DO $ptah_lpr_cleanup$
		BEGIN
			IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%s') THEN
				DROP OWNED BY %s;
				DROP ROLE %s;
			END IF;
			IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%s') THEN
				DROP OWNED BY %s;
				DROP ROLE %s;
			END IF;
		END
		$ptah_lpr_cleanup$`,
		postgresIdentifier(schemaName),
		roleName, postgresIdentifier(roleName), postgresIdentifier(roleName),
		ownerName, postgresIdentifier(ownerName), postgresIdentifier(ownerName),
	))
	return err
}

func postgresIdentifier(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
