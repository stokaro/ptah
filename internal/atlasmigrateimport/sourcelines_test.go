package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

// A converted entry maps each of its lines to the source line it was read
// from, through the blank lines, directives and sections the conversion
// drops, and names the source file it holds.
func TestEntrySourceLines(t *testing.T) {
	tests := []struct {
		name       string
		format     atlasmigrateimport.Format
		file       string
		source     string
		wantName   string
		wantData   string
		wantSource []int
	}{
		{
			name:   "a Liquibase header, a blank line and a changeset",
			format: atlasmigrateimport.FormatLiquibase, file: "2_drop.sql",
			source:   "--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t;\n",
			wantName: "2_drop.sql", wantData: "--changeset a:2\nDROP TABLE t;\n", wantSource: []int{3, 4},
		},
		{
			name:   "two Liquibase changesets apart",
			format: atlasmigrateimport.FormatLiquibase, file: "2_drop.sql",
			source:   "--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t;\n\n\n--changeset a:3\n\nDROP TABLE u;\n",
			wantName: "2_drop.sql", wantData: "--changeset a:2\nDROP TABLE t;\n--changeset a:3\nDROP TABLE u;\n",
			wantSource: []int{3, 4, 7, 9},
		},
		{
			name:   "blank lines before the Liquibase header",
			format: atlasmigrateimport.FormatLiquibase, file: "2_drop.sql",
			source:   "\n\n--liquibase formatted sql\n--changeset a:2\nDROP TABLE t;\n",
			wantName: "2_drop.sql", wantData: "--changeset a:2\nDROP TABLE t;\n", wantSource: []int{4, 5},
		},
		{
			name:   "a golang-migrate up file",
			format: atlasmigrateimport.FormatGolangMigrate, file: "2_b.up.sql",
			source:   "-- note\n\nDROP TABLE t;\n",
			wantName: "2_b.sql", wantData: "-- note\nDROP TABLE t;\n", wantSource: []int{1, 3},
		},
		{
			name:   "a Flyway file",
			format: atlasmigrateimport.FormatFlyway, file: "V2__b.sql",
			source:   "-- note\n\nDROP TABLE t;\n",
			wantName: "4611686018427510315_b.sql", wantData: "-- note\nDROP TABLE t;\n", wantSource: []int{1, 3},
		},
		{
			name:   "a goose up section before a down section",
			format: atlasmigrateimport.FormatGoose, file: "2_b.sql",
			source:   "-- +goose Up\n\n-- note\nDROP TABLE t;\n-- +goose Down\nCREATE TABLE t (id int);\n",
			wantName: "2_b.sql", wantData: "-- note\nDROP TABLE t;\n", wantSource: []int{3, 4},
		},
		{
			name:   "a goose file without transaction, whose directive the conversion writes",
			format: atlasmigrateimport.FormatGoose, file: "2_b.sql",
			source:   "-- +goose NO TRANSACTION\n-- +goose Up\nCREATE INDEX CONCURRENTLY i ON t (id);\n",
			wantName: "2_b.sql", wantData: "-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY i ON t (id);\n",
			wantSource: []int{3, 3, 3},
		},
		{
			name:   "a written blank line before a blank line of the source further down",
			format: atlasmigrateimport.FormatGoose, file: "2_b.sql",
			source: "-- +goose NO TRANSACTION\n-- +goose Up\nCREATE INDEX CONCURRENTLY i ON t (id);\n\n" +
				"CREATE INDEX CONCURRENTLY j ON t (x);\nDROP TABLE q;\n",
			wantName: "2_b.sql",
			wantData: "-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY i ON t (id);\n" +
				"CREATE INDEX CONCURRENTLY j ON t (x);\nDROP TABLE q;\n",
			wantSource: []int{3, 3, 3, 5, 6},
		},
		{
			name:   "a dbmate up section",
			format: atlasmigrateimport.FormatDBMate, file: "2_b.sql",
			source:   "-- migrate:up\n\n-- note\nDROP TABLE t;\n-- migrate:down\n",
			wantName: "2_b.sql", wantData: "-- note\nDROP TABLE t;\n", wantSource: []int{3, 4},
		},
		{
			name:   "a line the source repeats",
			format: atlasmigrateimport.FormatGolangMigrate, file: "2_b.up.sql",
			source:   "SELECT 1;\n\nSELECT 1;\n",
			wantName: "2_b.sql", wantData: "SELECT 1;\nSELECT 1;\n", wantSource: []int{1, 3},
		},
		{
			name:   "a blank line inside a string literal",
			format: atlasmigrateimport.FormatGolangMigrate, file: "2_b.up.sql",
			source:   "\nINSERT INTO t VALUES ('a\n\nb');\n",
			wantName: "2_b.sql", wantData: "INSERT INTO t VALUES ('a\n\nb');\n", wantSource: []int{2, 3, 4},
		},
		{
			name:   "Windows line endings",
			format: atlasmigrateimport.FormatGolangMigrate, file: "2_b.up.sql",
			source:   "-- note\r\n\r\nDROP TABLE t;\r\n",
			wantName: "2_b.sql", wantData: "-- note\r\nDROP TABLE t;\n", wantSource: []int{1, 3},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			loaded, err := atlasmigrateimport.LoadFS(fstest.MapFS{
				test.file: &fstest.MapFile{Data: []byte(test.source)},
			}, "migrations", test.format)
			c.Assert(err, qt.IsNil)
			c.Assert(loaded.Entries, qt.HasLen, 1)
			entry := loaded.Entries[0]
			c.Assert(entry.Name, qt.Equals, test.wantName)
			c.Assert(string(entry.Data), qt.Equals, test.wantData)
			c.Assert(entry.Source, qt.Equals, test.file)
			c.Assert(string(entry.SourceData), qt.Equals, test.source)
			c.Assert(entry.SourceLines(), qt.DeepEquals, test.wantSource)
		})
	}
}

// An entry no single file holds maps no line.
func TestEntrySourceLines_WithoutASource(t *testing.T) {
	c := qt.New(t)
	entry := atlasmigrateimport.Entry{Name: "1_a.sql", Data: []byte("SELECT 1;\n")}
	c.Assert(entry.SourceLines(), qt.IsNil)
}
