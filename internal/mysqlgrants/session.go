package mysqlgrants

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/internal/dbreset"
)

// Read asks the server what the session on q holds, with the settings that decide
// how each row of the answer applies. mergedRoles is [Options.MergedRoles]: the server
// prints an active role's rows as the account's own, as MySQL does and MariaDB does
// not.
//
// It is the one way into [Parse] for a live session, so every question about what
// a session may see reads the same rows under the same settings. SHOW GRANTS about
// oneself needs no privilege, so a failure here means the question could not be
// asked, and the caller decides what that costs.
func Read(ctx context.Context, q dbreset.Querier, mergedRoles bool) (Grants, error) {
	lines, err := dbreset.QueryNames(ctx, q, "SHOW GRANTS")
	if err != nil {
		return Grants{}, fmt.Errorf("failed to read the connecting account's grants: %w", err)
	}
	options, err := readOptions(ctx, q)
	if err != nil {
		return Grants{}, fmt.Errorf("failed to read the settings grants depend on: %w", err)
	}
	options.MergedRoles = mergedRoles
	return Parse(lines, options), nil
}

func readOptions(ctx context.Context, q dbreset.Querier) (Options, error) {
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
