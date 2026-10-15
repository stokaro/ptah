package builtindialects_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_IdentifiersAgreeWithTheSwitchesTheyReplace pins Quoting and
// ColumnKeys against the switches internal/sqlident and internal/columnkey
// chose by before the descriptors carried them, written out here as they
// stood. sqlident.Quote quoted MySQL, MariaDB and ClickHouse in backticks, SQL
// Server in brackets, YDB in YQL's backticks and every other dialect in double
// quotes; Ident wrote Oracle's plain names bare; BareOrQuoted always quoted
// YDB; Qualified wrote a YDB name as one path; and columnkey named the key
// after the column on MySQL and MariaDB and as <table>_<column>_key on
// PostgreSQL alone.
func TestDescriptors_IdentifiersAgreeWithTheSwitchesTheyReplace(t *testing.T) {
	tests := []struct {
		dialect    string
		style      descriptor.QuoteStyle
		bare       descriptor.BareNames
		paths      bool
		columnKeys descriptor.ColumnKeyNaming
	}{
		{dialect: platform.Postgres, style: descriptor.QuoteDouble, columnKeys: descriptor.ColumnKeyTableColumnKey},
		{dialect: platform.MySQL, style: descriptor.QuoteBacktick, columnKeys: descriptor.ColumnKeyAfterColumn},
		{dialect: platform.MariaDB, style: descriptor.QuoteBacktick, columnKeys: descriptor.ColumnKeyAfterColumn},
		{dialect: platform.ClickHouse, style: descriptor.QuoteBacktick},
		{dialect: platform.SQLite, style: descriptor.QuoteDouble},
		{dialect: platform.SQLServer, style: descriptor.QuoteBracket},
		{dialect: platform.CockroachDB, style: descriptor.QuoteDouble},
		{dialect: platform.YugabyteDB, style: descriptor.QuoteDouble},
		{dialect: platform.Spanner, style: descriptor.QuoteDouble},
		{dialect: platform.Oracle, style: descriptor.QuoteDouble, bare: descriptor.BareEverywhere},
		{dialect: platform.YDB, style: descriptor.QuoteYQL, bare: descriptor.BareNever, paths: true},
	}
	c := qt.New(t)
	// Every built-in dialect has a row, so a new one cannot go unpinned.
	c.Assert(builtindialects.Descriptors(), qt.HasLen, len(tests))
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			got, found := platform.Lookup(test.dialect)

			c.Assert(found, qt.IsTrue)
			c.Assert(got.Quoting.Style, qt.Equals, test.style)
			c.Assert(got.Quoting.Bare, qt.Equals, test.bare)
			c.Assert(got.Quoting.Paths, qt.Equals, test.paths)
			c.Assert(got.ColumnKeys, qt.Equals, test.columnKeys)
		})
	}
}

// TestDescriptors_KeywordsAgreeWithTheListsTheyReplace pins each dialect's
// keywords against the lists sqlident's isSQLiteReservedKeyword and
// isOracleReservedKeyword switched on, written out here as they stood; every
// other dialect had none.
func TestDescriptors_KeywordsAgreeWithTheListsTheyReplace(t *testing.T) {
	sqlite := []string{
		"ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH",
		"AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
		"COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH",
		"ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST",
		"FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE",
		"IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS",
		"ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT",
		"NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER",
		"PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES",
		"REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS",
		"SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER",
		"UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE",
		"WINDOW", "WITH", "WITHOUT",
	}
	oracle := []string{
		"ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN", "BY",
		"CHAR", "CHECK", "CLUSTER", "COLUMN", "COLUMN_VALUE", "COMMENT", "COMPRESS", "CONNECT",
		"CREATE", "CURRENT", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP",
		"ELSE", "EXCEPT", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP",
		"HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL", "INSERT",
		"INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS",
		"MLSLABEL", "MODE", "MODIFY", "NESTED_TABLE_ID", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT",
		"NULL", "NUMBER", "OF", "OFFLINE", "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE",
		"PRIOR", "PUBLIC", "RAW", "RENAME", "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM", "ROWS",
		"SELECT", "SESSION", "SET", "SHARE", "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM",
		"SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER",
		"VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", "WHERE", "WITH",
	}
	tests := []struct {
		dialect  string
		keywords []string
	}{
		{dialect: platform.Postgres},
		{dialect: platform.MySQL},
		{dialect: platform.MariaDB},
		{dialect: platform.ClickHouse},
		{dialect: platform.SQLite, keywords: sqlite},
		{dialect: platform.SQLServer},
		{dialect: platform.CockroachDB},
		{dialect: platform.YugabyteDB},
		{dialect: platform.Spanner},
		{dialect: platform.Oracle, keywords: oracle},
		{dialect: platform.YDB},
	}
	c := qt.New(t)
	c.Assert(builtindialects.Descriptors(), qt.HasLen, len(tests))
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			got, found := platform.Lookup(test.dialect)
			c.Assert(found, qt.IsTrue)

			upper := make([]string, 0, len(got.Quoting.Keywords))
			for _, keyword := range got.Quoting.Keywords {
				upper = append(upper, strings.ToUpper(keyword))
			}

			c.Assert(slices.Sorted(slices.Values(upper)), qt.DeepEquals, slices.Sorted(slices.Values(test.keywords)))
		})
	}
}
