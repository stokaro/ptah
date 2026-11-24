package dbreset

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// AutocommitBeforeDDL is what a PostgreSQL-family session answers for
// CockroachDB's session setting autocommit_before_ddl: "on", "off", or empty on
// a server that has no such setting, as PostgreSQL, YugabyteDB and CockroachDB
// v23.2 are.
type AutocommitBeforeDDL string

// The values a CockroachDB session reports for autocommit_before_ddl.
const (
	AutocommitBeforeDDLOn  AutocommitBeforeDDL = "on"
	AutocommitBeforeDDLOff AutocommitBeforeDDL = "off"
)

// autocommitBeforeDDLQuery reads the setting with missing_ok, so a server
// without it answers NULL instead of an error that would abort the
// transaction the question is asked in.
const autocommitBeforeDDLQuery = "SELECT COALESCE(current_setting('autocommit_before_ddl', true), '')"

// ReadAutocommitBeforeDDL reads autocommit_before_ddl from the session q runs
// on. It only reads, so it is safe inside the transaction it asks about. It is
// for a PostgreSQL-family session: another engine has no current_setting and
// answers an error.
func ReadAutocommitBeforeDDL(ctx context.Context, q Querier) (AutocommitBeforeDDL, error) {
	rows, err := q.QueryContext(ctx, autocommitBeforeDDLQuery)
	if err != nil {
		return "", fmt.Errorf("read the session's autocommit_before_ddl: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var setting sql.NullString
	if rows.Next() {
		if err := rows.Scan(&setting); err != nil {
			return "", fmt.Errorf("read the session's autocommit_before_ddl: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read the session's autocommit_before_ddl: %w", err)
	}
	return AutocommitBeforeDDL(strings.ToLower(strings.TrimSpace(setting.String))), nil
}

// CommitsEachSchemaStatement reports whether the session commits an open
// transaction ahead of each schema statement, as CockroachDB does while the
// setting is on, its default from v25. A transaction around a cleanup's drops
// then ends at the first drop and takes its savepoint with it, so each drop
// stands alone and stays committed when a later one fails. The schema writers
// and internal/schemaclean both decide by it, so the two cannot disagree about
// the same session.
func (s AutocommitBeforeDDL) CommitsEachSchemaStatement() bool {
	return s == AutocommitBeforeDDLOn
}
