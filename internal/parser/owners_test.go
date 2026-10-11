package parser_test

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/sqlext"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/internal/parser"
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
