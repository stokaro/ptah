// Package oracle gives migration/migrator what Oracle does differently from
// what its descriptor states: the catalog queries about the migrator's own
// tables, which Oracle answers from its ALL_ views because it has no
// information_schema, the owner check, and what it adds to proving a check
// assertion read-only.
package oracle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/migratorext"
)

// Services returns Oracle's services. A check runs in a read-only session:
// dbschema.WithIsolatedQuerySession turns the request into SET TRANSACTION
// READ ONLY, which binds DML.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects:       []string{platform.Oracle},
		Catalog:        catalog{},
		Owners:         catalog{},
		Checks:         checks{},
		ReadOnlyChecks: true,
	}
}

type catalog struct{}

// PresenceQuery asks ALL_TABLES. A schema is a user, so the owner is the
// configured schema or the connected account. The name is compared exactly:
// Ptah quotes the revision table's name, so the catalog keeps the case it was
// given.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	return `SELECT COUNT(*)
FROM all_tables
WHERE owner = ? AND table_name = ?`, []any{table.ConfiguredOrConnectionSchema(), table.Name}, nil
}

// ColumnExists asks ALL_TAB_COLUMNS. The revision table's columns are created
// unquoted, so the catalog holds their upper-case fold; the table name is
// quoted and kept as given.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	query := sqlutil.Rebind(platform.Oracle, `SELECT COUNT(*)
FROM all_tab_columns
WHERE owner = ? AND table_name = ? AND column_name = ?`)
	var count int
	if err := q.QueryRowContext(
		ctx,
		query,
		table.ConfiguredOrConnectionSchema(),
		table.Name,
		strings.ToUpper(column),
	).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", column, err)
	}
	return count > 0, nil
}

// OwnerQuery reads ALL_OBJECTS, where a schema is a user, so the owner column
// is the answer. There is no membership to follow: an object in another
// account's schema is that account's, and reaching it at all is a deliberate
// cross-schema arrangement. Oracle needs a name: the connected account is the
// one an unqualified CREATE TABLE lands in.
//
// ALL_OBJECTS rather than ALL_TABLES: a view holding the name collides with
// CREATE TABLE the same way a table does, and the PL/SQL block that creates
// the revision table suppresses ORA-00955 for the collision, so a lookup that
// saw only tables would report the name free and hand a crafted view with an
// INSTEAD OF trigger the adoption this refuses.
//
// No object-type filter either. An object type in its own namespace -- an
// index, a trigger -- cannot collide, but one standing in this account's own
// schema answers that the account owns it, so filtering would remove nothing
// but the sentence explaining the filter.
func (catalog) OwnerQuery(table migratorext.Table) (string, []any) {
	return `SELECT
  o.owner,
  SYS_CONTEXT('USERENV', 'CURRENT_USER'),
  CASE WHEN o.owner = SYS_CONTEXT('USERENV', 'CURRENT_USER') THEN 1 ELSE 0 END
FROM all_objects o
WHERE o.owner = ? AND o.object_name = ?
FETCH FIRST 1 ROWS ONLY`, []any{table.ConfiguredOrConnectionSchema(), table.Name}
}

type checks struct{}

// Validate refuses an assertion that takes a NEXTVAL, which no transaction
// rolls back anywhere, so the text is the only place it can be refused.
func (checks) Validate(assertion string) error {
	if migratorext.ContainsIdentifierSequence(assertion, dialectlexer.Options(platform.Oracle), "NEXTVAL") {
		return errors.New("check assertion must not advance an Oracle sequence with NEXTVAL")
	}
	return nil
}
