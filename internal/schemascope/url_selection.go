package schemascope

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/internal/atlasurl"
)

// RefuseOutsideURLDatabase validates an explicit schema selection before a
// native command opens its database. A MySQL-family URL naming one database
// reads only that database: accepting other names would silently omit part of
// the requested scope. A server URL and other engines keep their own rules.
//
// Names are compared with the database spelling in the URL, byte for byte.
// Server-dependent case folding cannot be established before connecting; a
// caller wanting another spelling can use a server URL, whose selection is
// resolved against the connected server by [ReadNames]. The existing MySQL
// URL parser owns TCP, driver-DSN and socket forms, including escaped names.
func RefuseOutsideURLDatabase(address string, requested []string) error {
	names := UniqueNames(requested)
	if len(names) == 0 {
		return nil
	}
	// Source classification trims surrounding URL whitespace before connecting.
	// Check the same address so a padded --from or --db-url cannot skip this
	// preflight. Escaped whitespace in a database name remains identifier data.
	target, err := atlasurl.ParseMySQLURL(strings.TrimSpace(address))
	if errors.Is(err, atlasurl.ErrNotMySQLURL) {
		return nil
	}
	if err != nil {
		return err
	}
	limit := target.Database()
	if limit == "" {
		return nil
	}
	for _, name := range names {
		if name != limit {
			return fmt.Errorf("schema selection names database %q, but the URL is limited to database %q; "+
				"remove the database from the URL for a whole-server command, or select only %q in the URL's exact spelling",
				name, limit, limit)
		}
	}
	return nil
}
