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
		wantDiagnostic []string
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
			opts := atlasschema.InspectOptions{Format: tt.format, Diagnostics: &diagnostics}
			opts.Exclude = excludeForVirtualTableCase(tt.name)

			_, err := atlasschema.Inspect(context.Background(), conn, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.String() == "", qt.Equals, tt.wantSilent)
			for _, fragment := range tt.wantDiagnostic {
				c.Assert(diagnostics.String(), qt.Contains, fragment)
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

// excludeForVirtualTableCase supplies the one case that needs a selector,
// keeping the table free of a column every other row would leave empty.
func excludeForVirtualTableCase(name string) []string {
	if name == "an excluded virtual table reports nothing" {
		return []string{"docs"}
	}
	return nil
}

func virtualTableFixture(c *qt.C, dir string, statements []string) string {
	c.Helper()

	path := filepath.Join(dir, "virtual.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { _ = db.Close() }()

	for _, statement := range statements {
		_, err := db.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
	return path
}
