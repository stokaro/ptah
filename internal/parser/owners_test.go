package parser_test

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/sqlext"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/internal/parser"
	"ptah.run/internal/pgtablesource"
)

// clickHouseOwners selects the ClickHouse owner, whose syntax a ClickHouse
// document carries beside the shared grammar.
func clickHouseOwners() sqlext.Set {
	return must.Must(sqlext.NewSet(chsource.SQL()))
}

// withClickHouseOwners is the parse option that selects [clickHouseOwners].
func withClickHouseOwners() parser.Option {
	return parser.WithSQL(clickHouseOwners())
}

// withMySQLOwners is the parse option that selects the MySQL owner, whose
// column options a MySQL or MariaDB document carries, and a document that
// names no target too.
func withMySQLOwners() parser.Option {
	return parser.WithSQL(must.Must(sqlext.NewSet(mysqlsource.SQL())))
}

func withPostgresTableOwners() parser.Option {
	return parser.WithSQL(must.Must(sqlext.NewSet(pgtablesource.SQL()...)))
}
