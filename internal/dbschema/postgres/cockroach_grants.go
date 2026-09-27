package postgres

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/reservedrole"
)

// cockroachBuiltinRoles are the roles CockroachDB grants ALL on every object
// and refuses to revoke from.
var cockroachBuiltinRoles = []string{"admin", "root"}

// notCockroachBuiltin renders the predicate that leaves CockroachDB's built-in
// roles out of a grant read, for the grantee column named by column.
func notCockroachBuiltin(column string) string {
	return column + " NOT IN ('" + cockroachBuiltinRoles[0] + "', '" + cockroachBuiltinRoles[1] + "')"
}

// readCockroachGrants reads the table, sequence, schema and routine grants of
// the schemas under this read from information_schema, on every CockroachDB
// line, rather than from the ACL columns the PostgreSQL reads explode.
// standaloneSequences is what [standaloneSequenceSet] built for
// [Reader.readGrants].
//
// v25.4.16 and v26.2.7 leave pg_class.relacl, pg_namespace.nspacl and
// pg_proc.proacl NULL whatever was granted, so a read built on them finds no
// grant there: a description applied elsewhere drops every one, a comparison
// plans every declared grant again on every run, and the role scoping misses
// each role that holds one (stokaro/ptah#3815). v26.3.1 fills the columns, but
// information_schema.role_table_grants, schema_privileges and
// role_routine_grants answer the same rows on all three lines, so one read
// serves them all.
//
// Rows that describe nothing anyone wrote are left out:
//
//   - the built-in admin role and the root user, which hold ALL on every
//     object. Measured on v25.4.16 and v26.3.1, REVOKE from admin answers
//     `user admin does not have privileges over table`, and REVOKE from root
//     answers `user root must have exactly [ALL] privileges`;
//   - the owner of a table, a view, a sequence or a schema, which holds ALL on
//     what it owns: REVOKE ALL from the owner succeeds and the row stays. Ptah
//     describes no ownership, and owning an object is not a reason for the
//     role scoping either.
//
// PUBLIC's EXECUTE on a routine is kept but marked implicit, as the PostgreSQL
// read marks a routine's default ACL. A new routine has it, and
// information_schema cannot tell that default from an explicit grant; the
// state is the same either way. A routine's owner is kept as an implicit row
// too; [Reader.readCockroachRoutineGrants] says why. CockroachDB records no grantor there -- the
// column is NULL -- so GrantedBy is empty, as it is for a table grant on any
// line. It refuses column privileges as a syntax error, so there are none to
// read.
func (r *Reader) readCockroachGrants(ctx context.Context, standaloneSequences map[string]bool) ([]catalog.Grant, error) {
	var grants []catalog.Grant
	for _, schemaName := range r.schemasToRead() {
		relationGrants, err := r.readCockroachRelationGrants(ctx, schemaName, standaloneSequences)
		if err != nil {
			return nil, err
		}
		grants = append(grants, relationGrants...)

		schemaGrants, err := r.readCockroachSchemaGrants(ctx, schemaName)
		if err != nil {
			return nil, err
		}
		grants = append(grants, schemaGrants...)

		if r.caps.Has(capability.Functions) {
			routineGrants, err := r.readCockroachRoutineGrants(ctx, schemaName)
			if err != nil {
				return nil, err
			}
			grants = append(grants, routineGrants...)
		}
	}
	return grants, nil
}

// readCockroachRelationGrants reads the grants on the tables, views and
// sequences of one schema. role_table_grants lists a sequence as a table, so
// the relation kind decides which it is, and only a standalone sequence is
// kept, as [Reader.readSequenceGrantsForSchema] keeps it.
func (r *Reader) readCockroachRelationGrants(
	ctx context.Context,
	schemaName string,
	standaloneSequences map[string]bool,
) ([]catalog.Grant, error) {
	query := `
		SELECT
			CASE g.grantee WHEN 'public' THEN 'PUBLIC' ELSE g.grantee END,
			g.privilege_type,
			c.relname,
			c.relkind = 'S',
			g.is_grantable = 'YES'
		FROM information_schema.role_table_grants g
		JOIN pg_namespace n ON n.nspname = g.table_schema
		JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = g.table_name
		WHERE g.table_schema = $1
		AND ` + notCockroachBuiltin("g.grantee") + `
		AND g.grantee <> pg_get_userbyid(c.relowner)
		AND ` + reservedrole.ExcludeSQL("g.grantee") + `
		ORDER BY c.relname, 1, g.privilege_type`

	rows, err := r.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table grants for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		var grant catalog.Grant
		var sequence bool
		if err := rows.Scan(&grant.Role, &grant.Privilege, &grant.ObjectName, &sequence, &grant.WithOption); err != nil {
			return nil, fmt.Errorf("failed to scan table grant for schema %s: %w", schemaName, err)
		}
		grant.Schema = r.outputSchema(schemaName)
		grant.ObjectType = "TABLE"
		if sequence {
			if !standaloneSequences[catalog.QualifyTableName(grant.Schema, grant.ObjectName)] {
				continue
			}
			grant.ObjectType = "SEQUENCE"
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read table grants for schema %s: %w", schemaName, err)
	}
	return grants, nil
}

// readCockroachSchemaGrants reads the grants on one schema.
func (r *Reader) readCockroachSchemaGrants(ctx context.Context, schemaName string) ([]catalog.Grant, error) {
	query := `
		SELECT
			CASE p.grantee WHEN 'public' THEN 'PUBLIC' ELSE p.grantee END,
			p.privilege_type,
			p.is_grantable = 'YES'
		FROM information_schema.schema_privileges p
		JOIN pg_namespace n ON n.nspname = p.table_schema
		WHERE p.table_schema = $1
		AND ` + notCockroachBuiltin("p.grantee") + `
		AND p.grantee <> pg_get_userbyid(n.nspowner)
		AND ` + reservedrole.ExcludeSQL("p.grantee") + `
		ORDER BY 1, p.privilege_type`

	rows, err := r.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema grants for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		grant := catalog.Grant{ObjectType: "SCHEMA", ObjectName: schemaName}
		if err := rows.Scan(&grant.Role, &grant.Privilege, &grant.WithOption); err != nil {
			return nil, fmt.Errorf("failed to scan schema grant for schema %s: %w", schemaName, err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read schema grants for schema %s: %w", schemaName, err)
	}
	return grants, nil
}

// readCockroachRoutineGrants reads the grants on the routines of one schema
// that [describedRoutinePredicate] describes. role_routine_grants names a
// routine by its specific name, the name and the oid joined by an underscore,
// which is what tells two overloads apart.
//
// The owner's row is kept, whoever the owner is, and marked implicit like
// PUBLIC's EXECUTE. It is what tells a routine whose PUBLIC EXECUTE was revoked
// from a routine this read never saw: without a row the description cannot
// say the revoke, and a routine created from it gets the privilege back. The
// owner cannot lose EXECUTE -- REVOKE ALL from the owner succeeds and the row
// stays, measured on v25.4.16 and v26.3.1 -- so the row is implicit rather
// than a grant anyone wrote, and it is spelled EXECUTE, which is all that ALL
// names on a routine.
func (r *Reader) readCockroachRoutineGrants(ctx context.Context, schemaName string) ([]catalog.Grant, error) {
	query := `
		SELECT
			p.proname,
			pg_get_function_identity_arguments(p.oid),
			CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END,
			CASE g.grantee WHEN 'public' THEN 'PUBLIC' ELSE g.grantee END,
			g.privilege_type,
			g.is_grantable = 'YES',
			g.grantee = pg_get_userbyid(p.proowner)
		FROM information_schema.role_routine_grants g
		JOIN pg_proc p ON p.proname || '_' || p.oid::text = g.specific_name
		JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_language l ON l.oid = p.prolang
		WHERE g.routine_schema = $1
		AND n.nspname = $1` + describedRoutinePredicate + `
		AND (` + notCockroachBuiltin("g.grantee") + ` OR g.grantee = pg_get_userbyid(p.proowner))
		AND ` + reservedrole.ExcludeSQL("g.grantee") + `
		ORDER BY p.proname, 2, 4, g.privilege_type`

	rows, err := r.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query routine grants for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		grant := catalog.Grant{Schema: r.outputSchema(schemaName)}
		var owner bool
		if err := rows.Scan(
			&grant.ObjectName, &grant.Arguments, &grant.ObjectType,
			&grant.Role, &grant.Privilege, &grant.WithOption, &owner,
		); err != nil {
			return nil, fmt.Errorf("failed to scan routine grant for schema %s: %w", schemaName, err)
		}
		switch {
		case owner:
			grant.Privilege, grant.WithOption, grant.Implicit = "EXECUTE", false, true
		case grant.Role == "PUBLIC" && grant.Privilege == "EXECUTE" && !grant.WithOption:
			grant.Implicit = true
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read routine grants for schema %s: %w", schemaName, err)
	}
	return grants, nil
}

// cockroachRolesInScopeClauses are the grant branches of the role scoping on
// CockroachDB, read from information_schema for the reason the grant reads
// above are. They mirror the PostgreSQL branches: a role holding a privilege
// on a relation or a schema in scope, and, where the target has routines,
// [cockroachRoutineRolesInScopeClause]. The built-in roles and each object's
// owner are left out as the grant reads leave them out, so a role is scoped in
// exactly when a grant the description carries names it. CockroachDB names no
// grantor, so there is no grantor branch, and it refuses column privileges, so
// there is no column branch.
func cockroachRolesInScopeClauses() []string {
	return []string{
		`SELECT grantee.oid AS roleoid FROM information_schema.role_table_grants g
			JOIN pg_namespace n ON n.nspname = g.table_schema
			JOIN scope s ON s.oid = n.oid
			JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = g.table_name
			JOIN pg_roles grantee ON grantee.rolname = g.grantee
			WHERE ` + notCockroachBuiltin("g.grantee") + `
			AND g.grantee <> pg_get_userbyid(c.relowner)`,
		`SELECT grantee.oid FROM information_schema.schema_privileges p
			JOIN pg_namespace n ON n.nspname = p.table_schema
			JOIN scope s ON s.oid = n.oid
			JOIN pg_roles grantee ON grantee.rolname = p.grantee
			WHERE ` + notCockroachBuiltin("p.grantee") + `
			AND p.grantee <> pg_get_userbyid(n.nspowner)`,
	}
}

// cockroachRoutineRolesInScopeClause is the routine branch of the role scoping
// on CockroachDB: a role holding a privilege on a routine in scope that
// [Reader.readCockroachRoutineGrants] reports on. The owner's row is read too,
// but it is implicit, and a description leaves an implicit grant out, so the
// owner is left out here.
var cockroachRoutineRolesInScopeClause = `SELECT grantee.oid FROM information_schema.role_routine_grants g
			JOIN pg_proc p ON p.proname || '_' || p.oid::text = g.specific_name
			JOIN pg_namespace n ON n.oid = p.pronamespace AND n.nspname = g.routine_schema
			JOIN scope s ON s.oid = n.oid
			JOIN pg_language l ON l.oid = p.prolang
			JOIN pg_roles grantee ON grantee.rolname = g.grantee
			WHERE ` + notCockroachBuiltin("g.grantee") + `
			AND g.grantee <> pg_get_userbyid(p.proowner)` + describedRoutinePredicate
