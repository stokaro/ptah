package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/envbool"
)

// AllowForeignMetadataTableEnvVar accepts a metadata table this connection
// does not own, for a deployment where ownership cannot be transferred to the
// role that runs migrations.
const AllowForeignMetadataTableEnvVar = "PTAH_ALLOW_FOREIGN_METADATA_TABLE"

// Gated: the community binary performs no ownership check at all, so refusing
// a foreign metadata table is behavior it does not have. A reference run keeps
// the refusal -- being stricter about whose table Ptah writes to costs a
// caller nothing it asked for -- and refuses the variable that would relax it.
var allowForeignMetadataTableVar = envbool.New(AllowForeignMetadataTableEnvVar, false, envbool.Gated)

// ErrForeignMetadataTable is returned when a metadata table this migrator
// would write to is owned by a role the connection is not.
//
// It is a sentinel so a caller can tell this refusal from a permission error
// the server raised: nothing was attempted against the table, and the
// remedy is a decision about ownership rather than about grants.
var ErrForeignMetadataTable = errors.New("metadata table is owned by another role")

// ForeignMetadataTableError names the table and the owner behind
// [ErrForeignMetadataTable].
type ForeignMetadataTableError struct {
	// Table is the metadata table, qualified the way the catalog reports it.
	Table string
	// Owner is the role the catalog says owns it.
	Owner string
	// Role is the role this connection would write as.
	Role string
}

func (e *ForeignMetadataTableError) Error() string {
	return fmt.Sprintf(
		"refusing to use metadata table %s: it is owned by %q and this connection runs as %q, "+
			"so it is not the table Ptah would have created. Transfer it with ALTER TABLE %s OWNER TO %q, "+
			"or set %s=1 to accept a table this connection does not own",
		e.Table, e.Owner, e.Role, e.Table, e.Role, allowForeignMetadataTableVar.Name(),
	)
}

func (e *ForeignMetadataTableError) Is(target error) bool {
	return target == ErrForeignMetadataTable
}

// refuseForeignMetadataTable refuses a metadata table the connection's role
// neither owns nor is a member of the owner of.
//
// Ptah creates its metadata tables with CREATE TABLE IF NOT EXISTS and then
// writes to them, so a table already standing under that name is adopted
// whatever put it there. Where a lower-privileged role can create objects in
// the metadata schema -- PostgreSQL 14 and earlier grant CREATE on public to
// PUBLIC -- that role can pre-create the table, attach an invoker-rights
// trigger, and have Ptah's own write run the trigger body with the migration
// role's privileges. Measured on PostgreSQL 14.24 against both the revision
// table and the operation log (stokaro/ptah#3474).
//
// The check is ownership rather than a list of what a table may carry: a
// trigger is one way in, and a rule, a default expression on a column Ptah
// does not write, and a row-level policy are others. Provenance survives the
// next one.
//
// A table that is absent, and a dialect whose catalog reports no owner, are
// both "nothing to refuse". The MySQL family is the second: it has no table
// owner, and a trigger there runs as its definer rather than as the connected
// account, so the same squat gains the squatter a hook and not the migration
// role's privileges. Measured on MySQL 8.4.11 and MariaDB 12.3.3.
func (m *Migrator) refuseForeignMetadataTable(ctx context.Context, table string) error {
	allowed, err := allowForeignMetadataTableVar.Resolve()
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	query, args, ok := metadataTableOwnerQuery(
		m.connectionDialect(),
		configuredOrConnectionSchema(m.metadataTableSchemaName(), m.connectionSchemaName()),
		table,
	)
	if !ok {
		return nil
	}
	var owner, role string
	var mine bool
	err = m.conn.QueryRowContext(ctx, sqlutil.Rebind(m.connectionDialect(), query), args...).
		Scan(&owner, &role, &mine)
	if errors.Is(err, sql.ErrNoRows) {
		// No row means no table. Ptah is about to create it, and what it
		// creates it owns.
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read the owner of metadata table %s: %w", table, err)
	}
	if mine {
		return nil
	}
	return &ForeignMetadataTableError{Table: table, Owner: owner, Role: role}
}

// metadataTableOwnerQuery builds the catalog query that answers who owns one
// metadata table, as (owner, current role, whether they are the same role or
// the current one is a member of the owner).
//
// The membership arm is what keeps a normal deployment working: a table an
// administrator created and handed to an application role belongs to a role
// the application is a member of, and refusing that would report an
// arrangement an operator set up deliberately as an attack.
//
// ok reports whether this dialect answers the question at all. Those that do
// not are listed at [Migrator.refuseForeignMetadataTable].
func metadataTableOwnerQuery(dialect, schema, table string) (string, []any, bool) {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return postgresTableOwnerQuery, []any{schema, table}, true
	case platform.Oracle:
		return oracleTableOwnerQuery, []any{schema, table}, true
	default:
		return "", nil, false
	}
}

// postgresTableOwnerQuery reads the owner from pg_class and asks the server
// itself whether the current role is that role or a member of it, because
// pg_has_role follows inheritance the way a grant does and a name comparison
// would not.
//
// No relkind filter: CREATE TABLE IF NOT EXISTS collides with any relation
// holding the name, so a partitioned table, a view or a foreign table under it
// is adopted the same way an ordinary one is. Narrowing to `r` would report a
// partitioned squat as an absent table and hand it the adoption this refusal
// exists to stop.
//
// An empty schema is the connection's own, which is where an unqualified
// CREATE TABLE lands.
const postgresTableOwnerQuery = `SELECT
  pg_get_userbyid(c.relowner),
  current_user,
  pg_has_role(current_user, c.relowner, 'MEMBER')
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = COALESCE(NULLIF(?, ''), current_schema())
  AND c.relname = ?`

// oracleTableOwnerQuery reads ALL_TABLES, where a schema is a user, so the
// owner column is the answer. There is no membership to follow: a table in
// another account's schema is that account's, and reaching it at all is a
// deliberate cross-schema arrangement.
const oracleTableOwnerQuery = `SELECT
  t.owner,
  SYS_CONTEXT('USERENV', 'CURRENT_USER'),
  CASE WHEN t.owner = SYS_CONTEXT('USERENV', 'CURRENT_USER') THEN 1 ELSE 0 END
FROM all_tables t
WHERE t.owner = ? AND t.table_name = ?`
