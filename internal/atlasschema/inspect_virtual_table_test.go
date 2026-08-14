package atlasschema_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// TestInspectReportsAVirtualTableTheRenderingCannotCarry covers the loss the
// HCL and JSON renderings inflict on a SQLite virtual table.
//
// Only the SQL rendering has a construct for one. Measured on the command, the
// default format turns an FTS5 index into `table "docs" { schema = schema.main }`
// -- an empty block naming an ordinary table that, replayed, is not a full-text
// index -- and wrote ZERO bytes to standard error while doing it. The pinned
// community binary emits the same lossy block, so the document is not changed;
// what changes is that the loss is now said out loud, because a pipeline
// capturing inspection output cannot read documentation.
//
// The rows that expect silence are the ones that keep this from becoming a note
// on every run: the SQL format carries the declaration, and a database with no
// virtual table has nothing to lose.
func TestInspectReportsAVirtualTableTheRenderingCannotCarry(t *testing.T) {
	tests := []struct {
		name           string
		setup          []string
		format         string
		exclude        []string
		wantDiagnostic []string
		wantAbsent     []string
		wantSilent     bool
	}{
		{
			name: "the default format cannot carry it",
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format: "hcl",
			wantDiagnostic: []string{
				`"docs" (module fts5)`,
				"cannot carry a SQLite virtual table's module declaration",
				"--format '{{ sql . }}'",
			},
		},
		{
			name: "json cannot carry it either",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:         "json",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			name: "every dropped table is named",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
				`CREATE VIRTUAL TABLE geo USING rtree(id, x0, x1)`,
			},
			format:         "hcl",
			wantDiagnostic: []string{`"docs" (module fts5)`, `"geo" (module rtree)`, "were rendered"},
		},
		{
			name: "the SQL format carries it, so nothing is reported",
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// A custom template that reaches the SQL rendering carries the
			// declaration whatever else it does, which is why the check reads
			// the rendered text rather than classifying the template.
			name: "a custom template that renders SQL carries it",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:     `{{ sql . }}`,
			wantSilent: true,
		},
		{
			// SQLite compares the statement syntax, module, and ASCII parts of
			// identifiers without regard to ASCII case.
			name: "ASCII case differences still carry the declaration",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(body)`,
			},
			format:     "{{ hcl . }}\ncreate virtual table \"DOCS\" using FTS5(body);",
			wantSilent: true,
		},
		{
			// Even a comment that repeats the complete SQL is not a
			// declaration. The rendered HCL still cannot recreate the table.
			name: "a comment containing the complete declaration is still a loss",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:         "{{ hcl . }}\n# CREATE VIRTUAL TABLE \"docs\" USING fts5(title, body);",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// A complete declaration at a line boundary is still not executable
			// when it lives inside a SQL block comment.
			name: "a complete declaration in a block comment is still a loss",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format: "{{ hcl . }}\n/*\nCREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(title, body);\n*/",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// A block comment can close immediately before an executable
			// declaration. The opening delimiter lives on another line, so a
			// raw check of only the CREATE line cannot classify the leading
			// trivia correctly.
			name:  "a declaration after a multiline block comment is carried",
			setup: []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format: "{{ hcl . }}\n/* retained\n*/ CREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(title, body);",
			wantSilent: true,
		},
		{
			// A trailing line comment does not turn the complete executable
			// declaration before it into documentation.
			name:  "a declaration with a trailing line comment is carried",
			setup: []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format: "{{ hcl . }}\nCREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(title, body); -- retained comment",
			wantSilent: true,
		},
		{
			// The same text inside a multiline SQL string is data. A raw
			// line-boundary search cannot distinguish it from a statement, while
			// the SQLite lexer emits the whole string as one token.
			name: "a complete declaration in a string is still a loss",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format: "{{ hcl . }}\nSELECT 'note\nCREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(title, body);\n';",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// Comment markers inside a quoted module argument are data, not SQL
			// comments. The lexer-backed comment pass must preserve them.
			name: "comment markers in module argument strings are carried by SQL",
			setup: []string{
				forgedVirtualTable("docs", "CREATE VIRTUAL TABLE docs USING "+
					`fts5(body, tokenize = "porter /* exact */")`),
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// SQLite also preserves unquoted comments inside the module argument
			// span. They belong to the catalog declaration even though a SQL
			// lexer classifies them as comments.
			name: "an unquoted comment in module arguments is carried by SQL",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(body /* exact */)`,
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// The module arguments define the virtual table. Naming the right
			// table and module without them does not recreate the inspected
			// object.
			name: "a declaration without the inspected arguments is still a loss",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:         "{{ hcl . }}\nCREATE VIRTUAL TABLE \"docs\" USING fts5;",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// Different arguments can define different columns and module
			// behavior, so a declaration for another shape is not survival.
			name: "a declaration with different arguments is still a loss",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:         "{{ hcl . }}\nCREATE VIRTUAL TABLE \"docs\" USING fts5(other);",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// Module arguments are interpreted by the module rather than as SQL
			// identifiers. Their bytes remain significant even where the SQL
			// statement prefix is ASCII-case-insensitive.
			name: "ASCII case in module arguments remains significant",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(body, tokenize = 'porter')`,
			},
			format: "{{ hcl . }}\nCREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(body, tokenize = 'PORTER');",
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// SQLite preserves line breaks inside the module arguments, and the
			// SQL renderer carries them exactly. Complete-statement matching must
			// therefore span lines rather than regress a lossless SQL inspection.
			name: "multiline inspected arguments are carried by SQL",
			setup: []string{
				"CREATE VIRTUAL TABLE docs USING fts5(title,\nbody)",
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// SQLite treats a missing argument list and an explicitly empty one
			// as the same module declaration, and the catalog intentionally does
			// not distinguish them.
			name: "an explicitly empty argument list carries a no argument module",
			setup: []string{
				forgedVirtualTable("pages", `CREATE VIRTUAL TABLE pages USING dbstat`),
			},
			format:     "{{ hcl . }}\nCREATE VIRTUAL TABLE \"pages\" USING dbstat();",
			wantSilent: true,
		},
		{
			// A split/write template deliberately leaves the printed text empty
			// and puts the whole document in the planned files. Reading the text
			// alone reported a loss beside an out/main.sql that contained the
			// correct CREATE VIRTUAL TABLE.
			name: "a split/write SQL export carries it in the planned files",
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:     `{{ sql . | split | write "out" }}`,
			wantSilent: true,
		},
		{
			// The control for the row above: scanning the planned files must not
			// silence everything. An HCL export has no CREATE VIRTUAL TABLE in
			// its files either, and is still a loss.
			name: "a split/write HCL export is still a loss",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:         `{{ hcl . | split | write "out" }}`,
			wantDiagnostic: []string{`"docs" (module fts5)`},
		},
		{
			// The adversarial name. A check for the bare keyword anywhere in the
			// output matched the HCL block `table "CREATE VIRTUAL TABLE"` and
			// suppressed a real loss.
			name: "a table named after the keyword is still a loss in HCL",
			setup: []string{
				`CREATE VIRTUAL TABLE "CREATE VIRTUAL TABLE" USING fts5(t)`,
			},
			format:         "hcl",
			wantDiagnostic: []string{`"CREATE VIRTUAL TABLE" (module fts5)`},
		},
		{
			// The other half, and the one a keyword-splitting check got wrong:
			// the SQL rendering does carry this table's declaration.
			name: "a table named after the keyword is carried by SQL",
			setup: []string{
				`CREATE VIRTUAL TABLE "CREATE VIRTUAL TABLE" USING fts5(t)`,
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// Per table, not all-or-nothing. Both are virtual, one is scoped
			// out of the rendered schema, and only the one that is actually in
			// the document and actually lost gets named.
			name: "only the table the rendering dropped is named",
			setup: []string{
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
				`CREATE VIRTUAL TABLE geo USING rtree(id, x0, x1)`,
			},
			format:         "hcl",
			exclude:        []string{"docs"},
			wantDiagnostic: []string{`"geo" (module rtree)`},
			wantAbsent:     []string{`"docs"`},
		},
		{
			// A module name that needs identifier quoting. The renderer emits
			// `USING "fts-5"`, so a check spelling the module bare looked for
			// text the document never contains and called a lossless SQL
			// rendering lossy -- which strict compatibility then refused.
			name: "a module name needing quotes is carried by SQL",
			setup: []string{
				forgedVirtualTable("docs", `CREATE VIRTUAL TABLE docs USING "fts-5"(a, b)`),
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// The control: the same table in a format that really cannot carry
			// it is still a loss.
			name: "a module name needing quotes is still a loss in HCL",
			setup: []string{
				forgedVirtualTable("docs", `CREATE VIRTUAL TABLE docs USING "fts-5"(a, b)`),
			},
			format:         "hcl",
			wantDiagnostic: []string{`"docs" (module fts-5)`},
		},
		{
			// A keyword module is quoted by the renderer. The inspection check
			// must search for that same spelling or it falsely reports loss.
			name: "a module name that is a SQLite keyword is carried by SQL",
			setup: []string{
				forgedVirtualTable("docs", `CREATE VIRTUAL TABLE docs USING "select"(body)`),
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// Quoted whitespace belongs to the table identity. The SQL renderer
			// and the loss detector must both retain it or a lossless inspection
			// is reported as missing the original declaration.
			name: "quoted whitespace in a table name is carried by SQL",
			setup: []string{
				`CREATE VIRTUAL TABLE " docs " USING fts5(body)`,
			},
			format:     "sql",
			wantSilent: true,
		},
		{
			// SQLite folds only ASCII letters in identifiers. These two names
			// are therefore distinct, and a rendered statement for one must not
			// pretend the other declaration survived the custom format.
			name: "non ASCII case distinct table is checked independently",
			setup: []string{
				`CREATE VIRTUAL TABLE "Ä" USING fts5(body)`,
				`CREATE VIRTUAL TABLE "ä" USING fts5(body)`,
			},
			format:         "{{ hcl . }}\nCREATE VIRTUAL TABLE \"Ä\" USING fts5(body);",
			wantDiagnostic: []string{`"ä" (module fts5)`},
			wantAbsent:     []string{`"Ä" (module fts5)`},
		},
		{
			name: "a database with no virtual table reports nothing",
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
			},
			format:     "hcl",
			wantSilent: true,
		},
		{
			// An excluded table is not in the rendered schema, so it is not a
			// loss the rendering inflicted.
			name: "an excluded virtual table reports nothing",
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			},
			format:     "hcl",
			exclude:    []string{"docs"},
			wantSilent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, virtualTableFixture(c, t.TempDir(), tt.setup))
			defer dbschema.CloseAndWarn(conn)
			// The split/write rows plan real files against the working
			// directory, so give every row a throwaway one.
			t.Chdir(t.TempDir())

			var diagnostics bytes.Buffer
			opts := atlasschema.InspectOptions{
				Format:      tt.format,
				Diagnostics: &diagnostics,
				Exclude:     tt.exclude,
			}

			_, err := atlasschema.Inspect(context.Background(), conn, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.String() == "", qt.Equals, tt.wantSilent)
			for _, fragment := range tt.wantDiagnostic {
				c.Assert(diagnostics.String(), qt.Contains, fragment)
			}
			for _, fragment := range tt.wantAbsent {
				c.Assert(diagnostics.String(), qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestInspectRefusesAVirtualTableTheRenderingCannotCarry is the strict half.
//
// Strict compatibility owns the process output contract, so it refuses rather
// than handing a pipeline a document that looks complete and is not. The
// refusal is format-specific by construction -- it is only asked when the
// declaration is actually missing from the rendered text -- so the SQL format
// stays available, which is the row that keeps this from being "strict mode
// cannot inspect SQLite".
func TestInspectRefusesAVirtualTableTheRenderingCannotCarry(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		format   string
		validate func([]string) error
		wantErr  bool
	}{
		{
			name:     "the default format is refused",
			setup:    []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format:   "hcl",
			validate: refuseAnyVirtualTable,
			wantErr:  true,
		},
		{
			name:     "the SQL format is not",
			setup:    []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format:   "sql",
			validate: refuseAnyVirtualTable,
			wantErr:  false,
		},
		{
			name:     "a comment containing the complete declaration is refused",
			setup:    []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format:   "{{ hcl . }}\n# CREATE VIRTUAL TABLE \"docs\" USING fts5(title, body);",
			validate: refuseAnyVirtualTable,
			wantErr:  true,
		},
		{
			name:  "a declaration after a multiline block comment is not refused",
			setup: []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format: "{{ hcl . }}\n/* retained\n*/ CREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(title, body);",
			validate: refuseAnyVirtualTable,
			wantErr:  false,
		},
		{
			name:  "a declaration with a trailing line comment is not refused",
			setup: []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format: "{{ hcl . }}\nCREATE VIRTUAL TABLE \"docs\" " +
				"USING fts5(title, body); -- retained comment",
			validate: refuseAnyVirtualTable,
			wantErr:  false,
		},
		{
			name:     "a declaration with different arguments is refused",
			setup:    []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format:   "{{ hcl . }}\nCREATE VIRTUAL TABLE \"docs\" USING fts5(other);",
			validate: refuseAnyVirtualTable,
			wantErr:  true,
		},
		{
			name:     "quoted whitespace carried by SQL is not refused",
			setup:    []string{`CREATE VIRTUAL TABLE " docs " USING fts5(body)`},
			format:   "sql",
			validate: refuseAnyVirtualTable,
			wantErr:  false,
		},
		{
			name:     "a database with no virtual table is not",
			setup:    []string{`CREATE TABLE users (id INTEGER PRIMARY KEY)`},
			format:   "hcl",
			validate: refuseAnyVirtualTable,
			wantErr:  false,
		},
		{
			name:     "no policy means no refusal",
			setup:    []string{`CREATE VIRTUAL TABLE docs USING fts5(title, body)`},
			format:   "hcl",
			validate: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, virtualTableFixture(c, t.TempDir(), tt.setup))
			defer dbschema.CloseAndWarn(conn)

			_, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
				Format:                        tt.format,
				Diagnostics:                   &bytes.Buffer{},
				ValidateRenderedVirtualTables: tt.validate,
			})

			c.Assert(err != nil, qt.Equals, tt.wantErr)
		})
	}
}

// refuseAnyVirtualTable stands in for the strict policy, and refuses
// unconditionally on purpose. The production code asks only when the rendering
// actually dropped a declaration, so a stub that returned nil for an empty list
// would be covering for that guard rather than testing it: the rows expecting
// no error are the ones that prove the hook is not asked.
func refuseAnyVirtualTable(names []string) error {
	return fmt.Errorf("strict refusal: %v", names)
}

// forgedVirtualTable writes a sqlite_master row directly, so a fixture can
// present a module this build could never be asked to create -- which is the
// only way a module name that needs quoting arises, since a name SQLite can
// resolve bare does not need it. PRAGMA writable_schema is SQLite's own
// mechanism for this.
func forgedVirtualTable(name, ddl string) string {
	return `PRAGMA writable_schema = ON;` +
		`INSERT INTO sqlite_master (type, name, tbl_name, rootpage, sql) VALUES ` +
		`('table', '` + name + `', '` + name + `', 0, '` + ddl + `');` +
		`PRAGMA writable_schema = RESET;`
}

func virtualTableFixture(c *qt.C, dir string, statements []string) string {
	c.Helper()

	path := filepath.Join(dir, "virtual.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { _ = db.Close() }()

	for _, statement := range statements {
		// A forged catalog row is three statements in one entry, so the whole
		// script is executed rather than a single statement.
		_, err := db.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
	return path
}
