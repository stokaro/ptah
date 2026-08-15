//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestPostgresArrayColumnSurvivesAReadE2E is the round trip stokaro/ptah#1138
// cluster B is about, measured against the only judge that matters here: the
// server.
//
// PostgreSQL's information_schema describes an array column as the bare
// category "ARRAY", with a null character_maximum_length, so neither data_type
// nor udt_name can reconstruct `character varying(100)[]`. Reading a schema and
// rendering it back therefore produced `records ARRAY NOT NULL`, which reports
// success and then fails on the user's database with
// `syntax error at or near "ARRAY"` -- the same shape of defect #1106 named,
// moved to the read side.
//
// There is deliberately no comparison against the pinned community binary in
// this test: `schema inspect --format '{{ hcl . }}'` is not available in that
// binary at all, so PostgreSQL executing the DDL is the whole assertion.
//
// Reverted, the type reads back as "ARRAY" and the last step fails to create
// the table.
func TestPostgresArrayColumnSurvivesAReadE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{
			name:     "a sized element type keeps its length",
			declared: "varchar(100)[]",
			want:     "character varying(100)[]",
		},
		{
			name:     "an unsized element type",
			declared: "text[]",
			want:     "text[]",
		},
		{
			// PostgreSQL stores no dimensions, so the round trip is expected to
			// come back as a plain array rather than to preserve `[10][]`. The
			// row is here so that expectation is written down rather than
			// rediscovered.
			name:     "declared dimensions are the server's to drop",
			declared: "varchar(100)[10][]",
			want:     "character varying(100)[]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", dbURL)
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()

			testDBName := fmt.Sprintf("ptah_array_roundtrip_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c.TB, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c.TB, context.Background(), adminDB, testDBName)

			scopedURL := replaceDatabaseName(c.TB, dbURL, testDBName)
			setupDB, err := sql.Open("pgx", scopedURL)
			c.Assert(err, qt.IsNil)
			defer setupDB.Close()
			_, err = setupDB.ExecContext(ctx,
				"CREATE TABLE logs (records "+test.declared+" NOT NULL)")
			c.Assert(err, qt.IsNil)

			conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			read, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)

			c.Assert(postgresColumnType(read, "logs", "records"), qt.Equals, test.want)

			// The type as the converter hands it onward is the value every
			// renderer downstream writes, so recreating the column from it is
			// what proves the read is usable and not merely non-empty.
			converted := dbschematogo.ConvertDBSchemaToGoSchema(read)
			c.Assert(converted.Fields, qt.Not(qt.HasLen), 0)
			_, err = setupDB.ExecContext(ctx,
				"CREATE TABLE logs_again (records "+converted.Fields[0].Type+" NOT NULL)")
			c.Assert(err, qt.IsNil)

			c.Assert(livePostgresColumnFormat(c.TB, ctx, setupDB, "logs_again", "records"),
				qt.Equals, test.want)
		})
	}
}

// postgresColumnType returns the type an introspected schema carries for one
// column, or "" when the column is not there.
func postgresColumnType(schema *dbschematypes.DBSchema, table, column string) string {
	for _, dbTable := range schema.Tables {
		for _, dbColumn := range dbTable.Columns {
			if dbTable.Name == table && dbColumn.Name == column {
				return dbColumn.FormattedType
			}
		}
	}
	return ""
}

// livePostgresColumnFormat asks the server how it spells a column's type.
func livePostgresColumnFormat(tb testing.TB, ctx context.Context, db *sql.DB, table, column string) string {
	c := qt.New(tb)
	c.Helper()
	var formatted string
	err := db.QueryRowContext(ctx, `
		SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = $1::regclass AND a.attname = $2`, table, column).Scan(&formatted)
	c.Assert(err, qt.IsNil)
	return formatted
}
