// Package dialectlexer provides shared SQL lexer configuration for database
// dialects used by internal parsing and analysis packages.
package dialectlexer

import (
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/lexer"
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
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
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
