//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// postgresSpellings are the column types whose catalog name is not the name a
// document carries, one row each.
//
// Every one of them planned an `ALTER COLUMN ... TYPE` on every run before
// stokaro/ptah#2027, #2034 and #2035 -- on PostgreSQL a full table rewrite
// under an ACCESS EXCLUSIVE lock, for a column nobody had touched. The bit rows
// were worse than a rewrite: the width was dropped, so replaying the document
// into a fresh database produced `bit(1)` from `bit(4)`.
//
// They are separate rows rather than one wide table so a failure names the
// type rather than the fixture.
var postgresSpellings = []struct {
	name     string
	declared string
}{
	{name: "double precision, which the catalog calls float8", declared: "double precision"},
	{name: "real, which it calls float4", declared: "real"},
	{name: "char, which it calls bpchar", declared: "char(8)"},
	{name: "time, which it calls time", declared: "time without time zone"},
	{name: "time with a zone, which it calls timetz", declared: "time with time zone"},
	{name: "bit, whose width lives in another column", declared: "bit(4)"},
	{name: "bit varying, the same", declared: "bit varying(8)"},
	// The controls. These already agreed, and a fold that swallowed the width
	// would break them rather than these.
	{name: "varchar, the family the width rule came from", declared: "varchar(80)"},
	{name: "numeric, whose precision lives in two more columns", declared: "numeric(12,4)"},
	{name: "an array, which is not its element type", declared: "bit varying(8)[]"},
}

// TestPostgresTypeSpellingsConvergeE2E pins that the description of a
// PostgreSQL database matches the database it describes, for every type whose
// catalog spelling differs from the document's.
//
// It is live because the two spellings only exist once a server has answered:
// the catalog reports `float8`, `bpchar`, `timetz` and `varbit`, and the
// renderer writes `double precision`, `char(8)`, `time with time zone` and
// `bit varying`. A unit test would have to hard-code the catalog's half, which
// is the half that was wrong.
//
// The same matrix was measured on MySQL 8.4.11, MariaDB 11.4.12, SQL Server
// 2025 and ClickHouse 24.10 and converged on all four before any of these
// fixes. SQLite does NOT converge and has no row here: it stores the declared
// type text verbatim while Ptah's renderer canonicalizes to affinity types, so
// a hand-made `VARCHAR(80)` column is described faithfully and rendered as
// `TEXT`. That is a design question rather than a defect (stokaro/ptah#2040).
func TestPostgresTypeSpellingsConvergeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_type_spelling_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	scopedURL := replaceDatabaseName(c, dbURL, testDBName)
	setupDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()

	_, err = setupDB.ExecContext(ctx, createSpellingTable())
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// Non-vacuity: the table has to be in the read, or a comparison of nothing
	// against nothing would report no changes and pass.
	c.Assert(readColumnCount(read, "spellings"), qt.Equals, len(postgresSpellings)+1)

	described := dbschematogo.ConvertDBSchemaToGoSchema(read)
	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, described, read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(modifiedColumnSummaries(diff), qt.HasLen, 0)
}

// createSpellingTable builds one table with a column per row above.
func createSpellingTable() string {
	columns := make([]string, 0, len(postgresSpellings)+1)
	columns = append(columns, "id integer PRIMARY KEY")
	for i, spelling := range postgresSpellings {
		columns = append(columns, fmt.Sprintf("c_%d %s", i, spelling.declared))
	}
	return "CREATE TABLE spellings (" + strings.Join(columns, ", ") + ")"
}

// modifiedColumnSummaries names every column change a comparison reported, in
// a form a failure message can be read from.
func modifiedColumnSummaries(diff *difftypes.SchemaDiff) []string {
	var summaries []string
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			for attribute, change := range column.Changes {
				summaries = append(summaries,
					fmt.Sprintf("%s.%s %s: %s", table.TableName, column.ColumnName, attribute, change))
			}
		}
	}
	return summaries
}

// readColumnCount counts the columns a read carries for one table, so a
// vacuous comparison cannot pass for a healthy one.
func readColumnCount(schema *dbschematypes.DBSchema, table string) int {
	for _, dbTable := range schema.Tables {
		if dbTable.Name == table {
			return len(dbTable.Columns)
		}
	}
	return 0
}
