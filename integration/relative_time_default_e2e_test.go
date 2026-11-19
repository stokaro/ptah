//go:build integration

package integration_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyRefusesARelativeTimeDefaultE2E applies, on PostgreSQL 18,
// schema files whose default the server reads once, when it creates the table
// or the domain: measured there, each stores that moment, so every row gets
// it and every plan changes the default again. The apply refuses each by
// name before it sends a statement, and the database gains no table, no
// domain and no schema.
func TestSchemaApplyRefusesARelativeTimeDefaultE2E(t *testing.T) {
	rows := []struct {
		name, source, want string
	}{
		{
			name:   "a quoted qualified type",
			source: `CREATE TABLE notes (id int PRIMARY KEY, at "pg_catalog"."timestamptz" DEFAULT 'now');`,
			want:   `DEFAULT 'now' on a "pg_catalog"."timestamptz" column is read once, when the table is created`,
		},
		{
			name:   "a quoted type whose default ALTER COLUMN sets",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at \"timestamp\");\nALTER TABLE notes ALTER COLUMN at SET DEFAULT 'now';",
			want:   `DEFAULT 'now' on a "timestamp" column is read once, when the table is created`,
		},
		{
			name:   "an array literal",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at timestamp[] DEFAULT '{epoch,now}');",
			want:   `DEFAULT '{epoch,now}' on a timestamp[] column is read once, when the table is created`,
		},
		{
			name:   "an ARRAY constructor whose default ALTER COLUMN sets",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at date[]);\nALTER TABLE notes ALTER COLUMN at SET DEFAULT ARRAY['today']::date[];",
			want:   `DEFAULT ARRAY['today']::date[] on a date[] column is read once, when the table is created`,
		},
		{
			name:   "a timestamp cast to a date",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at date DEFAULT ('now'::timestamp)::date);",
			want:   `DEFAULT ('now'::timestamp)::date on a date column is read once, when the table is created`,
		},
		{
			name:   "a domain's own default",
			source: "CREATE DOMAIN moment AS timestamp DEFAULT 'now';\nCREATE TABLE notes (id int PRIMARY KEY, at moment);",
			want:   `moment": DEFAULT 'now' on a timestamp domain is read once, when the domain is created`,
		},
		{
			name: "a name two domains share",
			source: "CREATE SCHEMA a;\nCREATE SCHEMA b;\nCREATE DOMAIN a.moment AS timestamp;\nCREATE DOMAIN b.moment AS text;\n" +
				"CREATE TABLE notes (id int PRIMARY KEY, at moment DEFAULT 'now');",
			want: `DEFAULT 'now' on a moment column may be read once, when the table is created: more than one domain answers to moment`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := postgresScratchDevURL(c, "relative_time")

			applied := applyModel(c, database, declaredModel{source: row.source, file: "schema.sql", flag: "--schema-file"})

			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.want, historyComment(applied))
			c.Assert(relativeTimeObjectsIn(c, database), qt.Equals, "0,0,0")
		})
	}
}

// TestSchemaApplyKeepsAStringDefaultTheServerReadsAsWrittenE2E applies, on
// PostgreSQL 18, the defaults beside the refused ones that the server keeps
// as written or calls for each row: a shared domain name qualified with the
// schema of the domain over text, a text array, a domain over a timestamp
// whose default is a function, and 'now' cast to text before a date, which
// the server keeps as an expression and converts for each row. Each applies,
// the second apply plans nothing, and a new row takes the default.
func TestSchemaApplyKeepsAStringDefaultTheServerReadsAsWrittenE2E(t *testing.T) {
	rows := []struct{ name, source, read, want string }{
		{
			name: "a shared name qualified with the schema of the domain over text",
			source: "CREATE SCHEMA a;\nCREATE SCHEMA b;\nCREATE DOMAIN a.moment AS timestamp;\nCREATE DOMAIN b.moment AS text;\n" +
				"CREATE TABLE notes (id int PRIMARY KEY, at b.moment DEFAULT 'now');\n",
			read: "SELECT at FROM notes",
			want: "now",
		},
		{
			name:   "a text array",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at text[] DEFAULT '{now}');\n",
			read:   "SELECT array_to_string(at, ',') FROM notes",
			want:   "now",
		},
		{
			name:   "a string cast to text before a date",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at date DEFAULT ('now'::text)::date);\n",
			read: "SELECT pg_get_expr(d.adbin, d.adrelid) || ',' || (n.at = CURRENT_DATE)::text " +
				"FROM notes n, pg_attrdef d WHERE d.adrelid = 'notes'::regclass",
			want: "('now'::text)::date,true",
		},
		{
			name:   "a timestamp array whose element is cast to text",
			source: "CREATE TABLE notes (id int PRIMARY KEY, at timestamp[] DEFAULT ARRAY['now'::text]::timestamp[]);\n",
			read: "SELECT pg_get_expr(d.adbin, d.adrelid) || ',' || (n.at[1] > now()::timestamp - interval '1 hour')::text " +
				"FROM notes n, pg_attrdef d WHERE d.adrelid = 'notes'::regclass",
			want: "ARRAY[('now'::text)::timestamp without time zone],true",
		},
		{
			name:   "a domain whose default is a function",
			source: "CREATE DOMAIN moment AS timestamptz DEFAULT now();\nCREATE TABLE notes (id int PRIMARY KEY, at moment);\n",
			read:   "SELECT (at > now() - interval '1 hour')::text FROM notes",
			want:   "true",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := postgresScratchDevURL(c, "relative_time")

			got := applyEscapedLiteralModel(c, database, declaredModel{source: row.source, file: "schema.sql", flag: "--schema-file"}, row.read)

			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// relativeTimeObjectsIn counts what database holds outside the system
// schemas, as "tables,domains,schemas a and b".
func relativeTimeObjectsIn(c *qt.C, database string) string {
	c.Helper()
	conn, err := sql.Open("pgx", database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	var counts string
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT concat_ws(',',
		(SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')),
		(SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE t.typtype = 'd' AND n.nspname NOT IN ('pg_catalog', 'information_schema')),
		(SELECT count(*) FROM pg_namespace WHERE nspname IN ('a', 'b')))`).Scan(&counts), qt.IsNil)
	return counts
}
