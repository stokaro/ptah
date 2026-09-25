//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// columnSpellingDocument is a table holding one declared column.
func columnSpellingDocument(column string) map[string]string {
	return map[string]string{"schema.sql": `CREATE TABLE "%[1]s".spelled (id integer PRIMARY KEY, ` + column + `);`}
}

// Each column below is spelled differently from what PostgreSQL stores and
// reads back, so a comparison of the texts planned a change of type or default
// on every run after the column was created (stokaro/ptah#3617). The server is
// asked to spell the declaration, and the second plan is empty.
func TestPostgresLiveColumnSpellingsConverge(t *testing.T) {
	for _, column := range []string{
		"v varchar(10)[]",
		"v varchar(10)[] DEFAULT '{}'::varchar(10)[]",
		"t timestamptz DEFAULT '2020-01-01'::timestamp with time zone",
		"t timestamp DEFAULT '2020-01-01'",
		"t timestamptz DEFAULT '2020-01-01 10:00'",
		"d date DEFAULT '2020-1-1'",
		"s text DEFAULT 'x'::text::character varying",
		"b boolean DEFAULT 't'",
		`j jsonb DEFAULT '{"a":1}'`,
		"n numeric(5,2) DEFAULT 1.5",
	} {
		t.Run(column, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)

			settleFormsDocument(c, conn, schemaName, columnSpellingDocument(column))
		})
	}
}

// The control: the server's spelling is compared, not trusted. A default or a
// type the document changes still plans.
func TestPostgresLiveColumnSpellingChangePlans(t *testing.T) {
	tests := []struct {
		name       string
		from, to   string
		wantInPlan string
	}{
		{
			name:       "a timestamp default moves a day",
			from:       "t timestamptz DEFAULT '2020-01-01'::timestamp with time zone",
			to:         "t timestamptz DEFAULT '2020-01-02'::timestamp with time zone",
			wantInPlan: "2020-01-02",
		},
		{
			name:       "an array element widens",
			from:       "v varchar(10)[]",
			to:         "v varchar(20)[]",
			wantInPlan: "varchar(20)[]",
		},
		{
			name:       "a jsonb default gains a key",
			from:       `j jsonb DEFAULT '{"a":1}'`,
			to:         `j jsonb DEFAULT '{"a":1,"b":2}'`,
			wantInPlan: `"b":2`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			settleFormsDocument(c, conn, schemaName, columnSpellingDocument(test.from))

			statements := planFormsDocument(c, conn, schemaName, columnSpellingDocument(test.to))

			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.wantInPlan,
				qt.Commentf("%s", fmt.Sprint(statements)))
		})
	}
}

// A column the server refuses to create does not leave the rest of its table
// unresolved: the probe falls back to one column at a time. Here the declared
// default names a function that does not exist, so the table's probe is
// refused, and the timestamp column beside it still settles.
func TestPostgresLiveColumnSpellingOneRefusedColumn(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(
		`CREATE TABLE "%s".spelled (id integer PRIMARY KEY, t timestamptz DEFAULT '2020-01-01', n integer DEFAULT 1)`,
		schemaName))
	c.Assert(err, qt.IsNil)

	statements := planFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `CREATE TABLE "%[1]s".spelled (
    id integer PRIMARY KEY,
    t  timestamptz DEFAULT '2020-01-01'::timestamp with time zone,
    n  integer DEFAULT ptah_no_such_function()
);`})

	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "ptah_no_such_function()")
	c.Assert(joined, qt.Not(qt.Contains), `ALTER COLUMN "t"`)
}
