package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/catalog"
)

// ownerPrivilegeRelationKinds are the relation kinds whose owner has a default
// set of privileges a read reports: the kinds the table and sequence grant reads
// cover, as an SQL list for pg_class.relkind.
const ownerPrivilegeRelationKinds = `('r', 'v', 'f', 'p', 'm', 'S')`

// relationDefaultACL is the ACL PostgreSQL gives the relation or sequence the
// pg_class alias class holds when nobody has granted or revoked anything on it.
func relationDefaultACL(class string) string {
	return `acldefault(CASE ` + class + `.relkind WHEN 'S' THEN 's' ELSE 'r' END::"char", ` + class + `.relowner)`
}

// schemaDefaultACL is the ACL PostgreSQL gives the schema the pg_namespace alias
// namespace holds when nobody has granted or revoked anything on it.
func schemaDefaultACL(namespace string) string {
	return `acldefault('n', ` + namespace + `.nspowner)`
}

// ownerRevokedPrivilegeRoles is a branch of the role scoping: the owner of an
// object from objects, whose acl lacks some privilege the object's defaultACL
// gives the role owner. A description states that as a revoke naming the owner.
// Ownership alone is no reason, so the branch selects the role through the
// privilege that is gone.
func ownerRevokedPrivilegeRoles(objects, defaultACL, owner, acl string) string {
	return `SELECT d.grantee FROM ` + objects + `
			CROSS JOIN LATERAL aclexplode(` + defaultACL + `) d
			WHERE ` + acl + ` IS NOT NULL
			AND d.grantee = ` + owner + `
			AND NOT EXISTS (
				SELECT 1 FROM aclexplode(` + acl + `) held
				WHERE held.grantee = ` + owner + ` AND held.privilege_type = d.privilege_type
			)`
}

// readOwnerPrivilegesForSchema reads what the owner of each relation and sequence
// of one schema, and of the schema itself, holds of the privileges PostgreSQL
// gives an owner, one implicit row per privilege.
//
// An object's ACL is NULL until its first GRANT or REVOKE, and that statement
// writes the owner's entry out. The two forms mean the same thing, so a read that
// reports the entry as grants tells two objects with the same owner privileges
// apart: one comparison direction grants them all, and the other revokes them from
// the owner, which then cannot read its own table. So the grant reads leave the
// entry out and this read answers for both forms, from the object type's default
// ACL as this server computes it: MAINTAIN is in it from PostgreSQL 17, and a read
// must not take its absence on an older server for a revoke.
//
// A privilege the owner holds is marked [catalog.Grant.Implicit]: a comparison
// pairs a declared grant with it and plans a declared revoke of it, and a
// declaration that says nothing about it leaves it alone. A privilege the owner
// revoked from itself is also marked [catalog.Grant.IsPartialRevoke].
//
// The grant option comes from the ACL, where it says nothing about what the owner
// may do: PostgreSQL treats an owner as holding every grant option. It is reported
// so that a declared option converges instead of being planned on every run.
// Columns have no such default, and pg_attribute.attacl holds only what was
// granted on the column.
func (r *Reader) readOwnerPrivilegesForSchema(ctx context.Context, schemaName string) ([]catalog.Grant, error) {
	query := `
		SELECT
			o.object_type,
			o.object_name,
			owner_role.rolname,
			d.privilege_type,
			(
				SELECT bool_or(held.is_grantable)
				FROM aclexplode(COALESCE(o.acl, o.default_acl)) held
				WHERE held.grantee = o.owner_oid AND held.privilege_type = d.privilege_type
			)
		FROM (
			SELECT
				CASE c.relkind WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END AS object_type,
				c.relname AS object_name,
				c.relowner AS owner_oid,
				c.relacl AS acl,
				` + relationDefaultACL("c") + ` AS default_acl
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1
			AND c.relkind IN ` + ownerPrivilegeRelationKinds + `
			UNION ALL
			SELECT 'SCHEMA', n.nspname, n.nspowner, n.nspacl, ` + schemaDefaultACL("n") + `
			FROM pg_namespace n
			WHERE n.nspname = $1
		) o
		JOIN pg_roles owner_role ON owner_role.oid = o.owner_oid
		CROSS JOIN LATERAL aclexplode(o.default_acl) d
		WHERE d.grantee = o.owner_oid
		ORDER BY o.object_type, o.object_name, d.privilege_type`

	rows, err := r.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query owner privileges for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		grant := catalog.Grant{Implicit: true}
		var grantable sql.NullBool
		if err := rows.Scan(&grant.ObjectType, &grant.ObjectName, &grant.Role, &grant.Privilege, &grantable); err != nil {
			return nil, fmt.Errorf("failed to scan owner privilege for schema %s: %w", schemaName, err)
		}
		// A grant on a schema lives in no schema of its own.
		if grant.ObjectType != "SCHEMA" {
			grant.Schema = r.outputSchema(schemaName)
		}
		grant.WithOption = grantable.Bool
		grant.IsPartialRevoke = !grantable.Valid
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read owner privileges for schema %s: %w", schemaName, err)
	}
	return grants, nil
}
