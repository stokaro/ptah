//go:build integration

package migrator_test

import "ptah.run/internal/revisiontable"

// dropRevisionTableStatements returns a DROP TABLE IF EXISTS statement for the
// revision table and for every table the migrator keeps beside it, the
// operation log included. The servers are shared, so a test that drops only
// the revision table leaves its log for a later test to read as part of the
// schema.
func dropRevisionTableStatements(table string) []string {
	names := revisiontable.Configured("", table)
	statements := make([]string, 0, len(names))
	for _, name := range names {
		statements = append(statements, "DROP TABLE IF EXISTS "+name)
	}
	return statements
}
