package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// renameSetting remembers legacy_alter_table as a session held it before a
// statement changed it, so the session can be given back that value.
//
// The setting belongs to the connection and outlives the transaction that
// changed it. A rebuild plan turns it on around a rename that a view or a
// trigger names and off after, whatever the connection held: the plan cannot
// read the value. A migration file can change it too. Left as the statement
// left it, the next statement on the connection, or the next migration file,
// renames tables under other rules: with the setting on, a rename leaves the
// views and triggers that name the table as they are, and with it off it
// rewrites them.
//
// Only a statement that names the setting is watched, and the value is read
// right before the first such statement runs, so a session that never names it
// pays no extra statement.
type renameSetting struct {
	found   int
	known   bool
	touched bool
}

// renameSettingStatement reads and writes legacy_alter_table.
const renameSettingStatement = "PRAGMA legacy_alter_table"

// namesRenameSetting reports whether statement may change legacy_alter_table.
func namesRenameSetting(statement string) bool {
	return strings.Contains(strings.ToLower(statement), "legacy_alter_table")
}

// before records the value the session holds, ahead of a statement that names
// the setting.
func (r *renameSetting) before(ctx context.Context, session interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, statement string,
) error {
	if !namesRenameSetting(statement) {
		return nil
	}
	if !r.known {
		if err := session.QueryRowContext(ctx, renameSettingStatement).Scan(&r.found); err != nil {
			return fmt.Errorf("sqlite: read legacy_alter_table setting: %w", err)
		}
		r.known = true
	}
	r.touched = true
	return nil
}

// reset gives the session back the value recorded before the first statement
// that named the setting. It does nothing where no statement named it.
func (r *renameSetting) reset(ctx context.Context, session interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
},
) error {
	if !r.touched {
		return nil
	}
	if _, err := session.ExecContext(ctx, fmt.Sprintf("%s = %d", renameSettingStatement, r.found)); err != nil {
		return fmt.Errorf("sqlite: restore legacy_alter_table setting: %w", err)
	}
	r.touched = false
	return nil
}
