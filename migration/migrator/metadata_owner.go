package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/envbool"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
	"ptah.run/internal/sqlident"
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
			"so it is not the table Ptah would have created. Drop it and let Ptah create it, after moving "+
			"any rows worth keeping: ALTER TABLE ... OWNER TO would keep the triggers, defaults and "+
			"policies it carries, and this check would then accept it. To use it as it stands, set %s=1",
		e.Table, e.Owner, e.Role, AllowForeignMetadataTableEnvVar,
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
// role's privileges. Measured on MySQL 8.4.11 and MariaDB 12.3.3. YDB is
// asked nothing for a stronger reason: a table there can carry no code at all.
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
		m.metadataTableSchemaName(),
		m.connectionSchemaName(),
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
	return &ForeignMetadataTableError{
		Table: m.qualifiedMetadataTable(table),
		Owner: owner,
		Role:  role,
	}
}

// qualifiedMetadataTable is the name the refusal prints, which has to be the
// one an operator would type: an unqualified ALTER TABLE resolves through
// search_path and could rename a different table, or fail while leaving the
// refused one exactly as it was.
//
// Every metadata table is named through it. On YDB the schema is a directory,
// so the qualified name is one quoted path rather than two quoted parts, which
// YQL would read as a cluster and a table.
func (m *Migrator) qualifiedMetadataTable(table string) string {
	return sqlident.Qualified(m.connectionDialect(), m.metadataTableSchemaName(), table)
}

// ErrUnaddressableMetadataTable is returned when the log table's derived name
// is longer than the target accepts.
var ErrUnaddressableMetadataTable = errors.New("metadata table name exceeds the identifier limit")

// refuseUnaddressableLogTable refuses a log table whose derived name the
// server would truncate.
//
// The name is the revision table's plus a suffix, so a revision table close to
// the limit derives one past it. PostgreSQL truncates at 63 bytes without an
// error, and the truncated name is what the DDL creates and the INSERT writes
// while every catalog lookup here binds the full string: the ownership check
// reports the table absent, and a table somebody else placed under the
// truncated name is adopted. Two revision tables whose names differ only past
// the limit would also share one log.
//
// Refused rather than compensated for. Guessing the truncation per dialect
// would put a second spelling of every identifier into the write path.
//
// It closes the log rather than the run. The revision table is addressable --
// only the derived name is not -- so failing here would turn a working
// migration into a stopped one over a record beside the work, which is the
// trade-off the foreign-table refusal already makes on this path.
func (m *Migrator) refuseUnaddressableLogTable() error {
	if !m.migrationLogWritable() {
		return nil
	}
	return m.refuseTruncatedIdentifier(
		m.migrationsTableName()+migrationLogTableSuffix,
		"the operation log for migrations table "+strconv.Quote(m.migrationsTableName()),
		"shorten the migrations table name, or set migration.log to false",
	)
}

// refuseUnaddressableMetadata refuses the names this migrator was configured
// with, whatever it does with them afterwards.
//
// The log's derived name is the one that goes over a limit by accident, but it
// is not the only name that can: a configured revision table or schema over
// the limit is truncated by the DDL while every catalog lookup binds the full
// string, so the ownership check reports the relation absent and CREATE TABLE
// IF NOT EXISTS adopts whatever holds the truncated name.
//
// This one is terminal. Without an addressable revision table Ptah cannot
// record what it did, so there is no reduced mode to fall back to the way the
// log has one.
func (m *Migrator) refuseUnaddressableMetadata() error {
	if err := m.refuseTruncatedIdentifier(
		m.migrationsTableName(),
		"the migrations table",
		"shorten it",
	); err != nil {
		return err
	}
	if schema := m.metadataTableSchemaName(); schema != "" {
		return m.refuseTruncatedIdentifier(schema, "the migrations schema", "shorten it")
	}
	return nil
}

// refuseTruncatedIdentifier refuses one name the target would cut short.
func (m *Migrator) refuseTruncatedIdentifier(name, subject, remedy string) error {
	limit := capability.Identifiers(m.connectionDialect())
	if !limit.Exceeds(name) {
		return nil
	}
	return fmt.Errorf(
		"%w: %s is named %q, which is over the %d-%s limit this target enforces, "+
			"so the name Ptah writes and the name it looks up would differ; %s",
		ErrUnaddressableMetadataTable, subject, name, limit.Max, limit.Unit, remedy,
	)
}

// metadataTableOwnerQuery builds the catalog query that answers who owns one
// metadata table, as (owner, current role, whether they are the same role or
// the current one is a member of the owner), through the engine's owner
// service.
//
// The membership arm is what keeps a normal deployment working: a table an
// administrator created and handed to an application role belongs to a role
// the application is a member of, and refusing that would report an
// arrangement an operator set up deliberately as an attack.
//
// ok reports whether this dialect answers the question at all. Those that do
// not are listed at [Migrator.refuseForeignMetadataTable].
func metadataTableOwnerQuery(dialect, configuredSchema, connectionSchema, table string) (string, []any, bool) {
	owners := builtin.Lookup(dialect).Owners
	if owners == nil {
		return "", nil, false
	}
	query, args := owners.OwnerQuery(migratorext.Table{
		ConfiguredSchema: configuredSchema,
		ConnectionSchema: connectionSchema,
		Name:             table,
	})
	return query, args, true
}
