package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/internal/objectowner"
)

// firstVersionCheckingSet is the server_version_num of PostgreSQL 16.0, the
// first release that checks SET rather than membership on the new owner.
const firstVersionCheckingSet = 160000

// OwnerTransfers asks the server which roles this connection may not give an
// object to with ALTER ... OWNER TO. The server checks a role privilege on the
// new owner: SET on PostgreSQL 16 and later, membership before 16. CockroachDB
// checks membership, and rejects SET as a privilege name although it reports
// server_version_num 180000, so the dialect decides there rather than the
// version. A superuser, and on CockroachDB a member of admin, holds both on
// every role. The server also requires the new owner to hold CREATE on the
// object's schema, which a PostgreSQL superuser is exempt from and a
// CockroachDB admin is not.
//
// A writer for YugabyteDB, or one built without a dialect, asks nothing and
// answers an empty privilege: what YugabyteDB requires is not measured, and a
// statement the server refuses there fails as it runs.
func (w *PostgreSQLWriter) OwnerTransfers(ctx context.Context) (objectowner.Transfers, error) {
	if w.db == nil {
		return objectowner.Transfers{}, fmt.Errorf("no database connection")
	}
	if w.dialect != platform.Postgres && w.dialect != platform.CockroachDB {
		return objectowner.Transfers{}, nil
	}
	var answer objectowner.Transfers
	var superuser bool
	var version int
	err := w.db.QueryRowContext(ctx, `SELECT rolname, rolsuper, current_setting('server_version_num')::integer
		FROM pg_catalog.pg_roles WHERE rolname = current_user`,
	).Scan(&answer.Connected, &superuser, &version)
	if err != nil {
		return objectowner.Transfers{}, fmt.Errorf("read the privilege an owner change requires: %w", err)
	}
	answer.SchemaExempt = superuser && w.dialect == platform.Postgres
	answer.Privilege = "MEMBER"
	if w.dialect == platform.Postgres && version >= firstVersionCheckingSet {
		answer.Privilege = "SET"
	}
	rows, err := w.db.QueryContext(ctx, `SELECT rolname FROM pg_catalog.pg_roles WHERE NOT pg_has_role(current_user, oid, $1::text)`,
		answer.Privilege)
	if err != nil {
		return objectowner.Transfers{}, fmt.Errorf("read the roles the connection may not give an object to: %w", err)
	}
	defer rows.Close()
	answer.Refused = make([]string, 0)
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return objectowner.Transfers{}, fmt.Errorf("read the roles the connection may not give an object to: %w", err)
		}
		answer.Refused = append(answer.Refused, role)
	}
	if err := rows.Err(); err != nil {
		return objectowner.Transfers{}, fmt.Errorf("read the roles the connection may not give an object to: %w", err)
	}
	slices.Sort(answer.Refused)
	return answer, nil
}

// Relation returns what the server holds of the table, view or materialized
// view a statement names as written: its owner, its schema, and whether role
// holds CREATE on that schema, which ALTER ... OWNER TO requires of the new
// owner on PostgreSQL and on CockroachDB. The server reads the name as it
// reads it in the statement, quotes, folding and search_path included. The
// owner is empty where no such relation exists.
func (w *PostgreSQLWriter) Relation(ctx context.Context, written, role string) (objectowner.Relation, error) {
	if w.db == nil {
		return objectowner.Relation{}, fmt.Errorf("no database connection")
	}
	var relation objectowner.Relation
	err := w.db.QueryRowContext(ctx, `SELECT pg_catalog.pg_get_userbyid(c.relowner), n.nspname,
			pg_catalog.has_schema_privilege($2::text, c.relnamespace, 'CREATE')
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = to_regclass($1::text)`, written, role,
	).Scan(&relation.Owner, &relation.Schema, &relation.Creates)
	if errors.Is(err, sql.ErrNoRows) {
		return objectowner.Relation{}, nil
	}
	if err != nil {
		return objectowner.Relation{}, fmt.Errorf("read the owner of %s: %w", written, err)
	}
	return relation, nil
}
