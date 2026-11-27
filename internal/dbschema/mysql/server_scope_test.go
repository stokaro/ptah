package mysql_test

import (
	"database/sql/driver"
	"errors"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/mysqlname"
)

// serverTables are the tables of a fake MySQL server, by database, and
// serverDatabaseNames its databases in the order the server lists them.
var (
	serverTables        = map[string]string{"r1": "t", "r2": "u"}
	serverDatabaseNames = []string{"r1", "r2"}
)

// serverCatalog answers a reader as a MySQL server holding serverTables. The
// session selects the database selected names, or none when it is nil, as a
// connection to a URL naming no database does. Every question it does not
// model answers no rows.
func serverCatalog(selected any) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		answers := []func([]driver.NamedValue) dbtest.QueryResult{
			func([]driver.NamedValue) dbtest.QueryResult {
				return dbtest.QueryResult{Columns: []string{"DATABASE()"}, Rows: [][]driver.Value{{selected}}}
			},
			serverDatabases,
			serverTablesIn,
			func([]driver.NamedValue) dbtest.QueryResult { return dbtest.QueryResult{} },
		}
		markers := []string{"SELECT DATABASE()", "FROM information_schema.SCHEMATA", "TABLE_TYPE = 'BASE TABLE'"}
		asked := slices.IndexFunc(markers, func(marker string) bool { return strings.Contains(query, marker) })
		return answers[(asked+len(answers))%len(answers)](args), nil
	}
}

// serverDatabases answers the SCHEMATA listing with every database the
// server holds. The reader picks the ones [mysql.Reader.SetSchemas] named.
func serverDatabases([]driver.NamedValue) dbtest.QueryResult {
	result := dbtest.QueryResult{Columns: []string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}}
	for _, database := range serverDatabaseNames {
		result.Rows = append(result.Rows, []driver.Value{database, "utf8mb4", "utf8mb4_0900_ai_ci"})
	}
	return result
}

// serverTablesIn answers the table listing of the database its argument names.
func serverTablesIn(args []driver.NamedValue) dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT", "TABLE_COLLATION", "ROW_FORMAT"},
		Rows:    [][]driver.Value{{serverTables[args[0].Value.(string)], "BASE TABLE", "", "utf8mb4_0900_ai_ci", "Dynamic"}},
	}
}

// tableNames lists the tables of a read as `schema.table`.
func tableNames(schema *catalog.Database) []string {
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, catalog.QualifyTableName(table.Schema, table.Name))
	}
	return names
}

// TestReader_ReadsAWholeServer_HappyPath reads a session that selected no
// database as the whole server: every database, each table under its own
// database, and the databases [mysql.Reader.SetSchemas] names when it names
// some (stokaro/ptah#3789). A session that selected a database reads that
// database with no schema on its tables, whatever the list says, as the pinned
// community binary reads a URL naming one.
func TestReader_ReadsAWholeServer_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		selected    any
		databases   []string
		wantTables  []string
		wantSchemas []catalog.Schema
	}{
		{
			name:       "every database",
			wantTables: []string{"r1.t", "r2.u"},
			wantSchemas: []catalog.Schema{
				{Name: "r1", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
				{Name: "r2", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
			},
		},
		{
			name:        "the databases the list names",
			databases:   []string{"r2"},
			wantTables:  []string{"r2.u"},
			wantSchemas: []catalog.Schema{{Name: "r2", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"}},
		},
		{
			name:       "a session that selected a database",
			selected:   "r1",
			databases:  []string{"r2"},
			wantTables: []string{"t"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, serverCatalog(test.selected)))
			reader := mysql.NewMySQLReader(db.SQL, "")
			reader.SetSchemas(test.databases)

			got, err := reader.ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(tableNames(got), qt.DeepEquals, test.wantTables)
			c.Assert(got.Schemas, qt.DeepEquals, test.wantSchemas)
		})
	}
}

// TestReader_ReadsTheNamedDatabasesOfAWholeServer_HappyPath matches the
// databases [mysql.Reader.SetSchemas] names as the server compares names: a
// server that folds names reads r2 for R2, and one that keeps them apart reads
// nothing for it. The listing answers every database, so the match is the
// reader's own.
func TestReader_ReadsTheNamedDatabasesOfAWholeServer_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		folds       int64
		wantSchemas []catalog.Schema
	}{
		{
			name:        "a server that folds names",
			folds:       1,
			wantSchemas: []catalog.Schema{{Name: "r2", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"}},
		},
		{
			name: "a server that keeps names apart by case",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, serverCatalog(nil))
			db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				folding := dbtest.QueryResult{Columns: []string{"folds"}, Rows: [][]driver.Value{{test.folds}}}
				answer, _ := session(query, args)
				return map[bool]dbtest.QueryResult{true: folding, false: answer}[strings.TrimSpace(query) == mysqlname.FoldsNamesQuery], nil
			})
			reader := mysql.NewMySQLReader(db.SQL, "")
			reader.SetSchemas([]string{"R2"})

			got, err := reader.ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.Schemas, qt.DeepEquals, test.wantSchemas)
		})
	}
}

// TestReader_ReadsTheNamedDatabasesOfAWholeServer_FailurePath reports a server
// that would not say how it compares database names. Without the answer the
// read cannot tell which databases the names [mysql.Reader.SetSchemas] gave
// select, so it refuses rather than read all of them or none.
func TestReader_ReadsTheNamedDatabasesOfAWholeServer_FailurePath(t *testing.T) {
	c := qt.New(t)
	session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, serverCatalog(nil))
	db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		refused := map[bool]error{true: errors.New("SELECT command denied"), false: nil}[strings.TrimSpace(query) == mysqlname.FoldsNamesQuery]
		result, _ := session(query, args)
		return result, refused
	})
	reader := mysql.NewMySQLReader(db.SQL, "")
	reader.SetSchemas([]string{"r2"})

	got, err := reader.ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `failed to read how the server compares database names: SELECT command denied`)
	c.Assert(got, qt.IsNil)
}

// TestReader_ReadsAWholeServer_FailurePath reports a database listing the
// server refused, rather than describing a server with no databases.
func TestReader_ReadsAWholeServer_FailurePath(t *testing.T) {
	c := qt.New(t)
	answer := serverCatalog(nil)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		refused := map[bool]error{true: errors.New("SELECT command denied"), false: nil}[strings.Contains(query, "SCHEMATA")]
		result, _ := answer(query, args)
		return result, refused
	}))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `failed to list databases: .*SELECT command denied`)
	c.Assert(got, qt.IsNil)
}
