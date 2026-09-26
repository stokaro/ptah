// Package dialectlexer provides shared SQL lexer configuration for database
// dialects used by internal parsing and analysis packages.
package dialectlexer

import (
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// Options returns the lexer behavior for dialect-sensitive SQL tokenization.
func Options(dialect string) lexer.Options {
	dialect = platform.NormalizeDialect(dialect)
	options := lexer.Options{
		StandardStrings:     true,
		BackslashEscapes:    usesBackslashEscapes(dialect),
		BracketIdentifiers:  dialect == platform.SQLServer,
		DisableHashComments: dialect == platform.SQLServer,
	}
	switch dialect {
	case platform.Postgres:
		options.PostgreSQLEscapeStrings = true
		options.PostgreSQLStringConstants = true
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		// PostgreSQLStringConstants is PostgreSQL's scanner, which CockroachDB
		// measurably is not, and which nobody has measured on YugabyteDB or on
		// Spanner. Left off, a continued or a U& string there is two tokens
		// and is refused, rather than read by a rule the server may not share.
		options.PostgreSQLEscapeStrings = true
	case platform.MySQL:
		options.RequireWhitespaceAfterDashDash = true
		options.ExecutableComments = lexer.ExecutableCommentsMySQL
	case platform.MariaDB:
		options.RequireWhitespaceAfterDashDash = true
		options.ExecutableComments = lexer.ExecutableCommentsMariaDB
	}
	return options
}

func usesBackslashEscapes(dialect string) bool {
	switch dialect {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		return true
	default:
		return false
	}
}
