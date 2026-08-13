package sqlite

// White-box testing required: parseVirtualTableDDL has to survive statements that
// no CREATE this SQLite build accepts can put in a catalog: a module this build
// does not register, a comment inside the module arguments, an unterminated
// quote in a damaged file, a statement that is not a CREATE VIRTUAL TABLE at
// all. The exported surface can only be handed a catalog SQLite agrees to open,
// so the boundaries below are unreachable through it. The shapes SQLite CAN
// produce are covered black-box, against a real database, in
// TestReadSchemaRoundTripsVirtualTables.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestParseVirtualTableDDL(t *testing.T) {
	tests := []struct {
		name          string
		ddl           string
		wantOK        bool
		wantModule    string
		wantArguments string
	}{
		{
			name:          "the statement SQLite records for an FTS5 index",
			ddl:           `CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			wantOK:        true,
			wantModule:    "fts5",
			wantArguments: "title, body",
		},
		{
			// The comma lives inside a quoted identifier and the brackets are
			// SQLite's fourth quoting. Splitting the arguments on commas, or
			// stopping at the first `]`, both cut this in the wrong place.
			name:          "arguments quoted four ways, one carrying a comma",
			ddl:           "CREATE VIRTUAL TABLE d USING fts5([col one], \"col,two\", `col three`, 'col four')",
			wantOK:        true,
			wantModule:    "fts5",
			wantArguments: "[col one], \"col,two\", `col three`, 'col four'",
		},
		{
			name:          "an option value holding a doubled quote",
			ddl:           `CREATE VIRTUAL TABLE d USING fts5(a, tokenize = 'unicode61 separators ''-''')`,
			wantOK:        true,
			wantModule:    "fts5",
			wantArguments: `a, tokenize = 'unicode61 separators ''-'''`,
		},
		{
			name:          "nested parentheses inside the arguments",
			ddl:           `CREATE VIRTUAL TABLE d USING mymod(a, check (b > 0), c)`,
			wantOK:        true,
			wantModule:    "mymod",
			wantArguments: "a, check (b > 0), c",
		},
		{
			// A module this build does not register. Nothing in the parse
			// consults the module, which is what keeps the fix from being a
			// list of modules Ptah knows.
			name:          "a module the build does not register",
			ddl:           `CREATE VIRTUAL TABLE legacy USING fts4(a, b)`,
			wantOK:        true,
			wantModule:    "fts4",
			wantArguments: "a, b",
		},
		{
			name:          "a module name nobody has ever heard of",
			ddl:           `CREATE VIRTUAL TABLE t USING VirtualShape("shapefile", "UTF-8", 4326)`,
			wantOK:        true,
			wantModule:    "VirtualShape",
			wantArguments: `"shapefile", "UTF-8", 4326`,
		},
		{
			// SQLite creates the same object from `USING dbstat` and
			// `USING dbstat()`, and records whichever it was given. Both parse
			// to no arguments, and both render as the bare form.
			name:       "no argument list at all",
			ddl:        `CREATE VIRTUAL TABLE pages USING dbstat`,
			wantOK:     true,
			wantModule: "dbstat",
		},
		{
			name:       "an empty argument list",
			ddl:        `CREATE VIRTUAL TABLE pages USING dbstat()`,
			wantOK:     true,
			wantModule: "dbstat",
		},
		{
			name:          "a quoted module name",
			ddl:           `CREATE VIRTUAL TABLE q USING "fts5"(a)`,
			wantOK:        true,
			wantModule:    "fts5",
			wantArguments: "a",
		},
		{
			name:          "a quoted and schema-qualified table name",
			ddl:           `CREATE VIRTUAL TABLE IF NOT EXISTS main."my docs" USING fts5(a)`,
			wantOK:        true,
			wantModule:    "fts5",
			wantArguments: "a",
		},
		{
			name:          "keywords in mixed case with a comment between them",
			ddl:           "create /* it is still a create */ virtual\n\ttable d using Fts5(a)",
			wantOK:        true,
			wantModule:    "Fts5",
			wantArguments: "a",
		},
		{
			name:          "a trailing semicolon after a bare module",
			ddl:           `CREATE VIRTUAL TABLE pages USING dbstat;`,
			wantOK:        true,
			wantModule:    "dbstat",
			wantArguments: "",
		},
		{
			name:   "an ordinary CREATE TABLE",
			ddl:    `CREATE TABLE docs (title TEXT, body TEXT)`,
			wantOK: false,
		},
		{
			// The shadow tables FTS5 maintains are recorded with their names in
			// single quotes. They are ordinary tables, and must not be mistaken
			// for the virtual table that owns them.
			name:   "the statement recorded for an FTS5 shadow table",
			ddl:    `CREATE TABLE 'docs_data'(id INTEGER PRIMARY KEY, block BLOB)`,
			wantOK: false,
		},
		{
			name:   "a view",
			ddl:    `CREATE VIEW v AS SELECT 1`,
			wantOK: false,
		},
		{
			name:   "an empty statement",
			ddl:    ``,
			wantOK: false,
		},
		{
			name:   "no USING clause",
			ddl:    `CREATE VIRTUAL TABLE docs`,
			wantOK: false,
		},
		{
			name:   "USING with no module",
			ddl:    `CREATE VIRTUAL TABLE docs USING`,
			wantOK: false,
		},
		{
			name:   "an unterminated argument list",
			ddl:    `CREATE VIRTUAL TABLE docs USING fts5(a, b`,
			wantOK: false,
		},
		{
			name:   "an unterminated quote inside the arguments",
			ddl:    `CREATE VIRTUAL TABLE docs USING fts5(a, "b)`,
			wantOK: false,
		},
		{
			name:   "text after the argument list that is not a statement end",
			ddl:    `CREATE VIRTUAL TABLE docs USING fts5 nonsense`,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			spec, ok := parseVirtualTableDDL(tt.ddl)

			c.Assert(ok, qt.Equals, tt.wantOK)
			c.Assert(spec.Module, qt.Equals, tt.wantModule)
			c.Assert(spec.Arguments, qt.Equals, tt.wantArguments)
		})
	}
}

func TestTableKindFromCatalog(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  tableKind
	}{
		{name: "an ordinary table", value: "table", want: tableKindOrdinary},
		{name: "a virtual table", value: "virtual", want: tableKindVirtual},
		{name: "a module's shadow table", value: "shadow", want: tableKindShadow},
		{name: "a view, which reaches this reader by another path", value: "view", want: tableKindOrdinary},
		{name: "an unknown kind a later SQLite may add", value: "something-new", want: tableKindOrdinary},
		{name: "an empty kind", value: "", want: tableKindOrdinary},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(tableKindFromCatalog(tt.value), qt.Equals, tt.want)
		})
	}
}

func TestExcludeTablesFilter(t *testing.T) {
	tests := []struct {
		name          string
		names         []string
		wantFragment  string
		wantArguments []any
	}{
		{
			name:          "nothing to exclude leaves the query untouched",
			names:         nil,
			wantFragment:  "",
			wantArguments: nil,
		},
		{
			name:          "one name binds one placeholder",
			names:         []string{"docs"},
			wantFragment:  "\n\t\t  AND m.name NOT IN (?)",
			wantArguments: []any{"docs"},
		},
		{
			// A table name is allowed to contain a quote, so the names are
			// bound rather than interpolated.
			name:          "three names bind three placeholders in order",
			names:         []string{"docs", `we"ird`, "docs_data"},
			wantFragment:  "\n\t\t  AND m.name NOT IN (?, ?, ?)",
			wantArguments: []any{"docs", `we"ird`, "docs_data"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			fragment, arguments := excludeTablesFilter(tt.names)

			c.Assert(fragment, qt.Equals, tt.wantFragment)
			c.Assert(arguments, qt.DeepEquals, tt.wantArguments)
		})
	}
}
