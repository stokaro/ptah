package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgconn"
)

// dependentObjectsStillExist is the SQLSTATE of a DROP ROLE the server refuses
// because the role still holds a privilege or owns an object.
const dependentObjectsStillExist = "2BP01"

// ServerRoles lists the name of every role on the server, sorted. A role
// belongs to the server rather than to one database, so the claim of a dev
// database on a server the run owns reads them, and the reset drops the ones
// the run created; see [PostgreSQLWriter.DropRolesAddedSince].
func (w *PostgreSQLWriter) ServerRoles(ctx context.Context) ([]string, error) {
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}
	rows, err := w.db.QueryContext(ctx, "SELECT rolname FROM pg_catalog.pg_roles ORDER BY rolname")
	if err != nil {
		return nil, fmt.Errorf("read the server's roles: %w", err)
	}
	defer rows.Close()
	roles := make([]string, 0)
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, fmt.Errorf("read the server's roles: %w", err)
		}
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the server's roles: %w", err)
	}
	return roles, nil
}

// DropRolesAddedSince drops every role on the server that claimed does not
// name, in name order. claimed is what [PostgreSQLWriter.ServerRoles] read
// when a run claimed its dev database on a server it owns, so a role it names
// is never dropped, whoever created it, and a role it does not name is one the
// run created. A dry run drops nothing.
//
// A dev database reset leaves a role, which is the server's rather than the
// database's. Left behind on a server the operator declared disposable, which
// outlives the run, it is there for the next run's replay, whose CREATE ROLE
// the server then refuses: `role "app_reader" already exists` (SQLSTATE
// 42710).
//
// The reset runs first and drops the objects whose privileges name the role,
// so DROP ROLE alone succeeds as a rule, and it needs no more than the right
// to create the role. One the server refuses because the role still holds a
// privilege or owns an object, such as a grant on a schema the reset empties
// in place, is dropped after REASSIGN OWNED hands what it owns to the
// session's user and DROP OWNED revokes what it holds in this database. An
// object it owns in a schema the reset keeps therefore stays. Both statements
// need the role's privileges, which a superuser has.
func (w *PostgreSQLWriter) DropRolesAddedSince(ctx context.Context, claimed []string) error {
	if w.dryRun {
		return nil
	}
	current, err := w.ServerRoles(ctx)
	if err != nil {
		return err
	}
	for _, role := range current {
		if slices.Contains(claimed, role) {
			continue
		}
		if err := w.dropAddedRole(ctx, role); err != nil {
			return fmt.Errorf("drop role %q, which the run created: %w", role, err)
		}
	}
	return nil
}

// dropAddedRole drops role, a role the run created; see
// [PostgreSQLWriter.DropRolesAddedSince].
func (w *PostgreSQLWriter) dropAddedRole(ctx context.Context, role string) error {
	quoted := quoteIdent(role)
	// #nosec G202 -- The role name is quoted through sqlident.
	_, err := w.db.ExecContext(ctx, "DROP ROLE "+quoted)
	var serverErr *pgconn.PgError
	if err == nil || !errors.As(err, &serverErr) || serverErr.Code != dependentObjectsStillExist {
		return err
	}
	for _, statement := range []string{
		"REASSIGN OWNED BY " + quoted + " TO CURRENT_USER",
		"DROP OWNED BY " + quoted,
		"DROP ROLE " + quoted,
	} {
		if _, err := w.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
