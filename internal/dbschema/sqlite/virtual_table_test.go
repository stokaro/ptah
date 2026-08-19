package sqlite_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dbschema/sqlite"
)

// TestReadSchemaRoundTripsVirtualTables is the acceptance for
// stokaro/ptah#1028: a database containing a virtual table, described by
// `ptah db read` and replayed into an empty database, has to produce the same
// database.
//
// Before the fix the read emitted `CREATE TABLE "docs" ("title" BLOB, "body"
// BLOB)` for an FTS5 index plus a CREATE TABLE for each of the five shadow
// tables FTS5 maintains. Replayed, that made seven ordinary tables where the
// source had one virtual table: `MATCH` against the replayed `docs` fails, and
// the five shadow tables collide the moment a real FTS5 table is created.
//
// The proof is the catalog on both sides, not the SQL text. Each case reads
// `PRAGMA table_list` -- which reports `virtual`, `shadow` and `table` as three
// different kinds -- and the module declaration the object's own statement
// carries, from both databases, and requires them to be equal.
//
// The rows cover every module the SQLite build Ptah links registers that can
// own a persistent table. That set was enumerated with `PRAGMA module_list`,
// which reports exactly: dbstat, fts5, fts5vocab, geopoly, rtree, rtree_i32,
// sqlite_dbpage. It is deliberately not a list of modules Ptah knows about:
// the reader never names a module, and TestReadSchemaDescribesAVirtualTableOfAnUnavailableModule
// covers a module this build cannot even load.
func TestReadSchemaRoundTripsVirtualTables(t *testing.T) {
	tests := []struct {
		name       string
		setup      []string
		wantRender []string
	}{
		{
			name: "fts5 beside an ordinary table",
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "docs" USING fts5(title, body);`,
				"CREATE TABLE \"users\" (\n  \"id\" INTEGER PRIMARY KEY,\n  \"name\" TEXT NOT NULL\n);",
			},
		},
		{
			// Quoted whitespace is part of the SQLite identifier. Keeping the
			// near-twin ordinary table in the same catalog proves the read cannot
			// normalize either spelling without renaming or colliding the objects.
			name: "quoted whitespace beside an ordinary near-twin",
			setup: []string{
				`CREATE TABLE docs (id INTEGER PRIMARY KEY)`,
				`CREATE VIRTUAL TABLE " docs " USING fts5(body)`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE " docs " USING fts5(body);`,
				"CREATE TABLE \"docs\" (\n  \"id\" INTEGER PRIMARY KEY\n);",
			},
		},
		{
			name: "fts5 tokenizer and prefix options",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(a, b, tokenize = 'porter unicode61 remove_diacritics 2', prefix='2 3')`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "docs" USING fts5(a, b, tokenize = 'porter unicode61 remove_diacritics 2', prefix='2 3');`,
			},
		},
		{
			name: "module owned whitespace and comments",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5( body /* exact */ )`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "docs" USING fts5( body /* exact */ );`,
			},
		},
		{
			// The comma inside "col,two" is the point: a scanner that split the
			// module arguments on commas would cut this declaration in half.
			name: "quoted module arguments carrying a comma",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5([col one], "col,two")`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "docs" USING fts5([col one], "col,two");`,
			},
		},
		{
			name: "rtree",
			setup: []string{
				`CREATE VIRTUAL TABLE geo USING rtree(id, minx, maxx, miny, maxy)`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "geo" USING rtree(id, minx, maxx, miny, maxy);`,
			},
		},
		{
			name: "rtree_i32",
			setup: []string{
				`CREATE VIRTUAL TABLE geo USING rtree_i32(id, x0, x1)`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "geo" USING rtree_i32(id, x0, x1);`,
			},
		},
		{
			name: "geopoly, whose arguments are not a column list",
			setup: []string{
				`CREATE VIRTUAL TABLE shapes USING geopoly(label, area)`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "shapes" USING geopoly(label, area);`,
			},
		},
		{
			// fts5vocab reads another virtual table, so its arguments are
			// string literals rather than column declarations.
			name: "fts5vocab over an fts5 index",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(body)`,
				`CREATE VIRTUAL TABLE terms USING fts5vocab('docs', 'row')`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "docs" USING fts5(body);`,
				`CREATE VIRTUAL TABLE "terms" USING fts5vocab('docs', 'row');`,
			},
		},
		{
			name: "a module declared with no arguments at all",
			setup: []string{
				`CREATE VIRTUAL TABLE pages USING dbstat`,
			},
			wantRender: []string{
				`CREATE VIRTUAL TABLE "pages" USING dbstat;`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source := openMemoryDB(t)
			for _, statement := range tt.setup {
				execSQL(t, source, statement)
			}

			rendered := readAndRenderSQLite(t, source)
			c.Assert(rendered, qt.DeepEquals, tt.wantRender)

			replayed := openMemoryDB(t)
			for _, statement := range rendered {
				execSQL(t, replayed, statement)
			}
			c.Assert(tableCensus(t, replayed), qt.DeepEquals, tableCensus(t, source))
		})
	}
}

// TestReadSchemaSeparatesShadowTablesFromUserTablesOfTheSameShape is the case a
// name rule cannot answer.
//
// FTS5 maintains a shadow table literally called `docs_data`. An operator is
// also free to create a table called `docs_data` -- or `docs_backup`, or
// `notes_data` -- and it is theirs. The reader therefore asks SQLite, through
// `PRAGMA table_list`, which reports a table as `shadow` only when a module
// claims that exact suffix for that exact virtual table.
//
// The fixture holds a real `docs_data` shadow and, next to it, `docs_backup`:
// same prefix, same separator, a user table. Suppressing by suffix would keep
// the shadow out and take the user's table with it.
func TestReadSchemaSeparatesShadowTablesFromUserTablesOfTheSameShape(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE VIRTUAL TABLE docs USING fts5(title, body)`)
	execSQL(t, db, `CREATE TABLE docs_backup (id INTEGER PRIMARY KEY, payload TEXT)`)
	execSQL(t, db, `CREATE TABLE notes_data (id INTEGER PRIMARY KEY, block TEXT)`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	c.Assert(tableNames(schema.Tables), qt.DeepEquals, []string{"docs", "docs_backup", "notes_data"})

	// SQLite really does maintain a docs_data for this index, so the fixture
	// is the collision it claims to be and not a name that merely looks like
	// one.
	c.Assert(catalogNamesOfKind(t, db, "shadow"), qt.Contains, "docs_data")

	docs := findTable(schema.Tables, "docs")
	c.Assert(docs, qt.IsNotNil)
	c.Assert(docs.VirtualModule, qt.Equals, "fts5")
	c.Assert(docs.VirtualArguments, qt.Equals, "title, body")
	c.Assert(docs.Columns, qt.HasLen, 0)

	backup := findTable(schema.Tables, "docs_backup")
	c.Assert(backup, qt.IsNotNil)
	c.Assert(backup.VirtualModule, qt.Equals, "")
	c.Assert(columnNames(backup.Columns), qt.DeepEquals, []string{"id", "payload"})
}

// TestReadSchemaDescribesAVirtualTableOfAnUnavailableModule covers the module
// this build cannot load.
//
// The fix must not be a list of modules Ptah recognizes, so the case that
// proves it is a module the SQLite build Ptah links does not register at all.
// `PRAGMA module_list` on that build reports dbstat, fts5, fts5vocab, geopoly,
// rtree, rtree_i32 and sqlite_dbpage; fts3 and fts4 are absent, and a database
// written by a build that had them is an ordinary thing to be handed.
//
// On master this case did not merely emit a wrong statement, it failed the
// whole read: `pragma_table_xinfo` has to load the module to answer, so one
// such table aborted the batch with `no such module: fts4` and took every other
// table's columns with it.
//
// The catalog row is written directly because no CREATE statement this build
// accepts can produce it. `PRAGMA writable_schema` is SQLite's own mechanism
// for that, and the resulting classification is the one measured on a real
// fts4 database created by a build that has the module: `legacy` is reported
// `virtual`, and `legacy_segdir` -- a genuine fts4 shadow table -- is reported
// `table`, because only fts4 itself could say the suffix is its own.
func TestReadSchemaDescribesAVirtualTableOfAnUnavailableModule(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	forgeCatalogRow(t, db, "legacy", `CREATE VIRTUAL TABLE legacy USING fts4(a, b)`)
	forgeCatalogRow(t, db, "legacy_segdir", `CREATE TABLE 'legacy_segdir'(level INTEGER, idx INTEGER)`)

	c.Assert(catalogNamesOfKind(t, db, "virtual"), qt.DeepEquals, []string{"legacy"})

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	legacy := findTable(schema.Tables, "legacy")
	c.Assert(legacy, qt.IsNotNil)
	c.Assert(legacy.VirtualModule, qt.Equals, "fts4")
	c.Assert(legacy.VirtualArguments, qt.Equals, "a, b")

	// The rest of the schema still reads. This is the half of the defect that
	// was an outright failure rather than a wrong statement.
	users := findTable(schema.Tables, "users")
	c.Assert(users, qt.IsNotNil)
	c.Assert(columnNames(users.Columns), qt.DeepEquals, []string{"id"})

	// The documented residue, pinned so it cannot change in silence: without
	// the module, SQLite classifies its shadow table as an ordinary table and
	// Ptah reports what SQLite reports. See docs/sqlite.md.
	c.Assert(tableNames(schema.Tables), qt.DeepEquals, []string{"legacy", "legacy_segdir", "users"})
}

// TestReadSchemaRefusesAVirtualTableItCannotRead is the diagnostic half.
//
// The two signals the reader uses can disagree. `PRAGMA table_list` answers
// from the schema SQLite has loaded, and `sqlite_schema.sql` answers from the
// bytes in the file; a catalog edited under `PRAGMA writable_schema` separates
// them, and so does a damaged file. When SQLite says a table is virtual and
// its recorded statement is not a CREATE VIRTUAL TABLE, Ptah has no module
// declaration to emit -- and emitting the recorded CREATE TABLE instead is
// precisely the defect this change removes. So the read stops and names the
// table and the statement it could not read.
func TestReadSchemaRefusesAVirtualTableItCannotRead(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE VIRTUAL TABLE docs USING fts5(a)`)
	// writable_schema = OFF, not RESET: RESET reloads the schema from the
	// bytes just written, which would make the two signals agree again on the
	// ordinary-table answer and defeat the point of the fixture.
	execSQL(t, db, `PRAGMA writable_schema = ON`)
	execSQL(t, db, `UPDATE sqlite_master SET sql = 'CREATE TABLE docs (a)' WHERE type = 'table' AND name = 'docs'`)
	execSQL(t, db, `PRAGMA writable_schema = OFF`)
	c.Assert(catalogNamesOfKind(t, db, "virtual"), qt.DeepEquals, []string{"docs"})

	_, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"docs" is a virtual table`)
	c.Assert(err.Error(), qt.Contains, "CREATE TABLE docs (a)")
}

// readAndRenderSQLite runs the whole read path an operator runs: read the
// catalog, convert it to the desired-state model, and render it for SQLite.
// Asserting on the reader's struct alone would not catch a module declaration
// that is read correctly and then dropped on the way to the renderer, which is
// where the original defect actually lived.
func readAndRenderSQLite(t *testing.T, db *sql.DB) []string {
	t.Helper()

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	statements, err := renderer.GetOrderedCreateStatements(dbschematogo.ConvertDBSchemaToGoSchema(schema), "sqlite")
	if err != nil {
		t.Fatalf("render schema: %v", err)
	}
	// The renderer terminates every statement with a newline. It is trimmed so
	// the table above reads as the SQL an operator sees; the claim each row
	// makes is about the statement, and the catalog census is what proves it.
	trimmed := make([]string, 0, len(statements))
	for _, statement := range statements {
		trimmed = append(trimmed, strings.TrimRight(statement, "\n"))
	}
	return trimmed
}

// tableCensus reports what each table in the database actually is, as SQLite
// classifies it, together with the module declaration a virtual table's own
// statement carries. Two SQL texts can differ and describe the same database;
// two censuses cannot.
func tableCensus(t *testing.T, db *sql.DB) []string {
	t.Helper()

	const query = `
		SELECT l.name || '|' || l.type || '|' || COALESCE(
			(SELECT CASE
				WHEN m.sql LIKE 'CREATE VIRTUAL TABLE%'
				THEN substr(m.sql, instr(upper(m.sql), ' USING ') + 7)
				ELSE '' END
			   FROM sqlite_master AS m
			  WHERE m.name = l.name AND m.type = 'table'), '')
		  FROM pragma_table_list AS l
		 WHERE l.schema = 'main' AND l.name <> 'sqlite_schema'
		 ORDER BY l.name`
	return queryStrings(t, db, query)
}

func catalogNamesOfKind(t *testing.T, db *sql.DB, kind string) []string {
	t.Helper()

	return queryStrings(t, db,
		`SELECT name FROM pragma_table_list WHERE schema = 'main' AND type = ? ORDER BY name`, kind)
}

func queryStrings(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()

	values := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %q: %v", query, err)
	}
	return values
}

// forgeCatalogRow writes a sqlite_master row directly, so a test can present
// the reader with an object this SQLite build could never be asked to create.
func forgeCatalogRow(t *testing.T, db *sql.DB, name, ddl string) {
	t.Helper()

	execSQL(t, db, `PRAGMA writable_schema = ON`)
	execSQL(t, db,
		`INSERT INTO sqlite_master (type, name, tbl_name, rootpage, sql) VALUES ('table', ?, ?, 0, ?)`,
		name, name, ddl)
	execSQL(t, db, `PRAGMA writable_schema = RESET`)
}

func tableNames(tables []types.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

func columnNames(columns []types.DBColumn) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}
