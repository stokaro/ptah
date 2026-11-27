package mysqlgrants

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/internal/dbreset"
)

// Read asks the server what the session on q holds, with the settings that decide
// how each row of the answer applies. mariaDB is [Options.MariaDB].
//
// It is the one way into [Parse] for a live session, so every question about what
// a session may see reads the same rows under the same settings. SHOW GRANTS and
// CURRENT_ROLE() about oneself need no privilege, so a failure here means the
// question could not be asked, and the caller decides what that costs.
func Read(ctx context.Context, q dbreset.Querier, mariaDB bool) (Grants, error) {
	lines, err := dbreset.QueryNames(ctx, q, "SHOW GRANTS")
	if err != nil {
		return Grants{}, fmt.Errorf("failed to read the connecting account's grants: %w", err)
	}
	options, err := ReadOptions(ctx, q)
	if err != nil {
		return Grants{}, fmt.Errorf("failed to read the settings grants depend on: %w", err)
	}
	options.MariaDB = mariaDB
	if options.ActiveRole, err = options.readActiveRole(ctx, q); err != nil {
		return Grants{}, fmt.Errorf("failed to read the session's active roles: %w", err)
	}
	return Parse(lines, options), nil
}

// readActiveRole reports whether a role is active in the session on q, as
// MySQL's CURRENT_ROLE() answers: NONE where none is. A pool hands out
// sessions that activated the account's default roles and nothing else, since
// Ptah runs no SET ROLE, so every session of one pool answers the same.
// MariaDB is asked nothing: it has one lookup, so [Options.ActiveRole]
// changes nothing there.
func (o Options) readActiveRole(ctx context.Context, q dbreset.Querier) (bool, error) {
	if o.MariaDB {
		return false, nil
	}
	names, err := dbreset.QueryNames(ctx, q, "SELECT CURRENT_ROLE()")
	if err != nil {
		return false, err
	}
	return len(names) == 1 && names[0] != "" && names[0] != "NONE", nil
}

// ReadOptions reads the server settings that decide how a database-level row
// applies: partial_revokes, which MariaDB does not have and which then reads
// as off, and lower_case_table_names. [Options.MariaDB] is the caller's to
// set.
func ReadOptions(ctx context.Context, q dbreset.Querier) (Options, error) {
	rows, err := q.QueryContext(ctx, `SHOW VARIABLES WHERE Variable_name IN ('partial_revokes', 'lower_case_table_names')`)
	if err != nil {
		return Options{}, err
	}
	defer func() { _ = rows.Close() }()
	var options Options
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return Options{}, err
		}
		switch strings.ToLower(name) {
		case "partial_revokes":
			options.LiteralDatabaseNames = strings.EqualFold(value, "ON")
		case "lower_case_table_names":
			options.FoldCase = value != "0"
		}
	}
	return options, rows.Err()
}
