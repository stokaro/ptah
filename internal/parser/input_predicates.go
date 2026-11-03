package parser

import "ptah.run/internal/sqlbatch"

// isSQLServerGoBatchSeparatorAt reports whether a GO token is a SQL Server
// utility batch separator command on its own line. Identifiers such as
// "AS go" or variables such as "@go" are ordinary T-SQL tokens.
//
// The rule is [sqlbatch.SeparatorAt], which the statement splitter in
// core/sqlutil and the migration lint scanner ask too, so a schema file, a
// migration and a lint run end a batch at the same line.
func isSQLServerGoBatchSeparatorAt(input string, start, end int) bool {
	_, ok := sqlbatch.SeparatorAt(input, start, end)
	return ok
}
