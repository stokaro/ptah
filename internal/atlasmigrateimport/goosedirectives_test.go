package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// TestLoadFSDirectiveSectionParsing pins the goose and dbmate directive parsers
// against behavior measured on the pinned community binary (v1.3.0), file by
// file and byte for byte.
//
// The byte assertions are not decoration. `migrate import` writes these bytes and
// then hashes them into atlas.sum, so a body that differs by one blank line makes
// the two tools disagree about a directory both call clean — which is worse than
// the refusal it replaces. Every expected body below was read back from the
// community binary's own converted output.
//
// Rows marked "refuses" split into two kinds, and the distinction is the whole
// point of the change:
//
//   - out-of-order directives, where the community binary ALSO refuses. Ptah used
//     to accept and execute these, which is the never-looser half of the parity
//     rule being violated on the same function #981 asked to change.
//   - near-miss spellings and a dbmate file with no up directive, where the
//     community binary exits 0. Those are deliberate divergences; see the doc
//     comments on gooseNearMissPragma and dbmateUpSQL for what it does instead.
func TestLoadFSDirectiveSectionParsing(t *testing.T) {
	const widgets = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"

	body := func(want string) func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error) {
		return func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(loaded.Entries, qt.HasLen, 1)
			c.Assert(string(loaded.Entries[0].Data), qt.Equals, want)
		}
	}
	refuses := func(pattern string) func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error) {
		return func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error) {
			c.Assert(err, qt.ErrorMatches, pattern)
		}
	}

	tests := []struct {
		name   string
		format atlasmigrateimport.Format
		file   string
		assert func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error)
	}{
		{
			// #981 proper. The community binary executes such a file's bytes
			// verbatim and records the revision honestly; a file with no
			// directives has no rollback section that could leak onto the apply
			// path, so refusing it protected nothing.
			name:   "goose file with no directives is the migration",
			format: atlasmigrateimport.FormatGoose,
			file:   widgets,
			assert: body(widgets),
		},
		{
			// Pins trimSQL rather than normalizeSQL for the verbatim path: the
			// community binary converts these 83 bytes to 83 bytes, keeping the
			// blank line. normalizeSQL would drop it and produce a different
			// atlas.sum for a directory the other tool calls clean.
			name:   "goose file with no directives keeps interior blank lines",
			format: atlasmigrateimport.FormatGoose,
			file:   "CREATE TABLE a (id INTEGER PRIMARY KEY);\n\nCREATE TABLE b (id INTEGER PRIMARY KEY);\n",
			assert: body("CREATE TABLE a (id INTEGER PRIMARY KEY);\n\nCREATE TABLE b (id INTEGER PRIMARY KEY);\n"),
		},
		{
			// The up body starts at the FILE START, not at the Up directive.
			// Ptah used to drop this CREATE silently while still exiting 0.
			name:   "goose keeps SQL written above the Up directive",
			format: atlasmigrateimport.FormatGoose,
			file:   "CREATE TABLE pre (id INTEGER PRIMARY KEY);\n-- +goose Up\n" + widgets,
			assert: body("CREATE TABLE pre (id INTEGER PRIMARY KEY);\n" + widgets),
		},
		{
			// NO TRANSACTION cannot open or close a section, so the community
			// binary leaves it in the executed SQL. Recognizing it would change
			// nothing, so the parser deliberately does not.
			name:   "goose keeps a NO TRANSACTION directive in the body",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose NO TRANSACTION\n-- +goose Up\n" + widgets,
			assert: body("-- +goose NO TRANSACTION\n" + widgets),
		},
		{
			name:   "goose keeps an unrecognized +goose line in the body",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Frobnicate\n" + widgets,
			assert: body("-- +goose Frobnicate\n" + widgets),
		},
		{
			// "Up, Down, Up" is a file the community binary accepts, and it
			// executes only the first up section.
			name:   "goose body stops at the first Down even when another Up follows",
			format: atlasmigrateimport.FormatGoose,
			file: "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n" +
				"-- +goose Down\nDROP TABLE a;\n-- +goose Up\nCREATE TABLE b (id INTEGER PRIMARY KEY);\n",
			assert: body("CREATE TABLE a (id INTEGER PRIMARY KEY);\n"),
		},
		{
			// A directive name runs to the first space, so trailing text is
			// tolerated and this IS the Up directive.
			name:   "goose treats trailing text after the directive name as part of the directive",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Up extra\n" + widgets,
			assert: body(widgets),
		},
		{
			// Exactly one space follows "+goose"; a second one breaks the
			// directive, so the community binary runs the whole file. Ptah
			// refuses instead — the near-miss guard, see below.
			name:   "goose refuses a second space before the directive name",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose  Up\n" + widgets,
			assert: refuses(`(?s)migration file 1_init\.sql line 1: .* is not a goose directive .*Write "-- \+goose Up" instead`),
		},
		{
			// An intentionally empty migration is legitimate. The community
			// binary records it as an applied revision with 0 statements; Ptah
			// used to drop it from the converted directory and from
			// atlas_schema_revisions while still exiting 0.
			name:   "goose empty up section is an entry, not a skip",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Up\n",
			assert: func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(loaded.Entries, qt.HasLen, 1)
				c.Assert(loaded.Entries[0].Data, qt.HasLen, 0)
			},
		},

		// --- out-of-order directives: the community binary refuses these too ---
		{
			name:   "goose refuses Down before any Up",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Down\nDROP TABLE widgets;\n-- +goose Up\n" + widgets,
			assert: refuses(`migration file 1_init\.sql line 1: unexpected "-- \+goose Down" directive because no up section has been opened yet`),
		},
		{
			name:   "goose refuses a second Up",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose Up\nCREATE TABLE b (id INTEGER PRIMARY KEY);\n",
			assert: refuses(`migration file 1_init\.sql line 3: unexpected "-- \+goose Up" directive because an up section is already open`),
		},
		{
			name:   "goose refuses StatementEnd with no StatementBegin",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose StatementEnd\n",
			assert: refuses(`migration file 1_init\.sql line 3: unexpected "-- \+goose StatementEnd" directive because no "-- \+goose StatementBegin" block is open`),
		},
		{
			name:   "goose refuses StatementBegin outside a section",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose StatementBegin\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose StatementEnd\n",
			assert: refuses(`migration file 1_init\.sql line 1: unexpected "-- \+goose StatementBegin" directive because no up section has been opened yet`),
		},
		{
			// Directives are parsed inside StatementBegin blocks, not passed
			// through: the community binary refuses this too.
			name:   "goose refuses a section directive inside a StatementBegin block",
			format: atlasmigrateimport.FormatGoose,
			file: "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n" +
				"-- +goose StatementBegin\n-- +goose Up\nSELECT 1;\n-- +goose StatementEnd\n",
			assert: refuses(`migration file 1_init\.sql line 4: unexpected "-- \+goose Up" directive because it appears inside a "-- \+goose StatementBegin" block`),
		},
		{
			// Well-formed control: must keep working, and the down section must
			// not reach the up SQL.
			name:   "goose well-formed Up and Down keeps only the up section",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Up\n" + widgets + "-- +goose Down\nDROP TABLE widgets;\n",
			assert: body(widgets),
		},
		{
			name:   "goose statement block body survives with its internal semicolons",
			format: atlasmigrateimport.FormatGoose,
			file: "-- +goose Up\n-- +goose StatementBegin\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n" +
				"-- +goose StatementEnd\n-- +goose Down\nDROP TABLE a;\n",
			assert: body("CREATE TABLE a (id INTEGER PRIMARY KEY);\n"),
		},

		// --- deliberate divergences: the community binary exits 0 on these ---
		{
			// Measured: the community binary folds this typo into the body and
			// executes "DROP TABLE a;", so the table is created and then dropped
			// and the migration is recorded as successful. A case error in a
			// directive must not silently roll back a migration.
			name:   "goose refuses a lowercase down as a near miss",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);\n-- +goose down\nDROP TABLE a;\n",
			assert: refuses(`(?s)migration file 1_init\.sql line 3: "-- \+goose down" is not a goose directive .*Write "-- \+goose Down" instead`),
		},
		{
			name:   "goose refuses a lowercase up as a near miss",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose up\n" + widgets,
			assert: refuses(`(?s)migration file 1_init\.sql line 1: "-- \+goose up" is not a goose directive .*Write "-- \+goose Up" instead`),
		},
		{
			// Scoping proof for the near-miss guard: prose that merely starts
			// with a directive-looking token stays a comment, because refusing it
			// would reject files the community binary runs safely.
			name:   "goose leaves prose after +goose alone",
			format: atlasmigrateimport.FormatGoose,
			file:   "-- +goose up to date\n" + widgets,
			assert: body("-- +goose up to date\n" + widgets),
		},
		{
			// Measured: the community binary exits 0, records revision 1 with
			// 0/0 statements and creates nothing, and `migrate import` writes a
			// ZERO-BYTE file over the 47 authored bytes. Ptah keeps refusing.
			name:   "dbmate refuses a file with no migrate:up directive",
			format: atlasmigrateimport.FormatDBMate,
			file:   widgets,
			assert: refuses(`migration file 1_init\.sql carries no "-- migrate:up" directive.*`),
		},
		{
			// But an empty up SECTION is legitimate and is recorded, exactly as
			// the community binary records it.
			name:   "dbmate empty up section is an entry, not a skip",
			format: atlasmigrateimport.FormatDBMate,
			file:   "-- migrate:up\n-- migrate:down\nDROP TABLE a;\n",
			assert: func(c *qt.C, loaded *atlasmigrateimport.Loaded, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(loaded.Entries, qt.HasLen, 1)
				c.Assert(loaded.Entries[0].Data, qt.HasLen, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source := fstest.MapFS{
				"1_init.sql": &fstest.MapFile{Data: []byte(tt.file)},
			}

			loaded, err := atlasmigrateimport.LoadFS(source, "migrations", tt.format)

			tt.assert(c, loaded, err)
		})
	}
}
