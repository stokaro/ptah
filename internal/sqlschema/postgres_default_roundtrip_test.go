package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/internal/sqlschema"
)

// A PostgreSQL keyword default must survive the complete SQL source path.
// Adding parentheses to CURRENT_TIMESTAMP produces invalid SQL; quoting NULL
// changes a missing value into the text 'NULL'.
func TestRead_PostgresKeywordDefaultsReachRenderedSQL(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		declared   string
		want       string
	}{
		{name: "transaction timestamp", columnType: "timestamptz", declared: "CURRENT_TIMESTAMP", want: "CURRENT_TIMESTAMP"},
		{name: "transaction date", columnType: "date", declared: "CURRENT_DATE", want: "CURRENT_DATE"},
		{name: "transaction time", columnType: "timetz", declared: "CURRENT_TIME", want: "CURRENT_TIME"},
		{name: "local timestamp", columnType: "timestamp", declared: "LOCALTIMESTAMP", want: "LOCALTIMESTAMP"},
		{name: "local time", columnType: "time", declared: "LOCALTIME", want: "LOCALTIME"},
		{name: "timestamp precision", columnType: "timestamptz", declared: "CURRENT_TIMESTAMP(3)", want: "CURRENT_TIMESTAMP(3)"},
		{name: "SQL NULL", columnType: "varchar(64)", declared: "NULL", want: "NULL"},
		{name: "typed SQL NULL", columnType: "varchar(64)", declared: "NULL::varchar", want: "NULL::varchar"},
		{name: "text NULL stays text", columnType: "varchar(64)", declared: "'NULL'", want: "'NULL'"},
		{name: "a function stays a call", columnType: "timestamptz", declared: "now()", want: "now()"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read(
				[]byte("CREATE TABLE defaults (id integer PRIMARY KEY, value "+test.columnType+" DEFAULT "+test.declared+");"),
				platform.Postgres,
			)
			c.Assert(err, qt.IsNil)

			statements, err := renderer.GetOrderedCreateStatements(&database, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "DEFAULT "+test.want+"\n")
		})
	}
}
