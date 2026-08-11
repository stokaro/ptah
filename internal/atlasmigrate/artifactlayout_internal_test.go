package atlasmigrate

// White-box testing required: composeMigrationArtifacts is the unexported step
// between planning and staging, and the property under test — that every name
// it composes is one atlas.sum covers and the loader accepts — cannot be stated
// through GenerateDiff without a dev database per layout.

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// TestComposeMigrationArtifactsAreCoveredAndLoadable states, over every layout
// at once, the agreement the write path depends on: a file
// [composeMigrationArtifacts] names must be a file
// [atlasmigrateimport.SumFileNames] covers for that layout, and the directory
// it produces must be one [atlasmigrateimport.LoadFS] reads back.
//
// Both halves matter and they fail in opposite directions. A name outside the
// covered set produces an atlas.sum that omits the migration just written, so
// the community binary's own `migrate validate` reports it as added; a
// directory the loader cannot read is one `migrate apply` refuses after this
// verb has already reported success. Stating the property over the composer
// rather than checking the two by inspection is what keeps them from drifting
// when either side moves — the same reason
// atlasmigrateimport.TestSkeletonFilesAreCoveredAndLoadable exists beside
// SkeletonFiles.
//
// The forward half is what must be covered, and the rollback half must NOT be
// on the two layouts that keep it in a file of its own: golang-migrate's
// atlas.sum covers `*.up.sql` and flyway's covers the `V…` file, which is
// measured behavior of the community binary rather than a choice made here.
func TestComposeMigrationArtifactsAreCoveredAndLoadable(t *testing.T) {
	tests := []struct {
		name    string
		format  atlasmigrateimport.Format
		files   int
		covered int
	}{
		{name: "atlas", format: atlasmigrateimport.FormatAtlas, files: 1, covered: 1},
		{name: "golang-migrate", format: atlasmigrateimport.FormatGolangMigrate, files: 2, covered: 1},
		{name: "flyway", format: atlasmigrateimport.FormatFlyway, files: 2, covered: 1},
		{name: "goose", format: atlasmigrateimport.FormatGoose, files: 1, covered: 1},
		{name: "dbmate", format: atlasmigrateimport.FormatDBMate, files: 1, covered: 1},
		{name: "liquibase", format: atlasmigrateimport.FormatLiquibase, files: 1, covered: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			artifacts, err := composeMigrationArtifacts(tt.format, "addcol", 20240102030405, []MigrationFileContent{{
				SQL:               "CREATE TABLE widgets (id INTEGER);",
				DownSQL:           "DROP TABLE widgets;",
				Statements:        []string{"CREATE TABLE widgets (id INTEGER)"},
				ReverseStatements: []string{"DROP TABLE widgets"},
			}})
			c.Assert(err, qt.IsNil)
			c.Assert(artifacts, qt.HasLen, tt.files)

			dir := fstest.MapFS{}
			for _, artifact := range artifacts {
				dir[artifact.Name] = &fstest.MapFile{Data: artifact.Contents}
			}

			names, err := atlasmigrateimport.SumFileNames(dir, tt.format)
			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.HasLen, tt.covered)
			c.Assert(names[0], qt.Equals, artifacts[0].Name)

			loaded, err := atlasmigrateimport.LoadFS(dir, "composed", tt.format)
			c.Assert(err, qt.IsNil)
			c.Assert(loaded.Entries, qt.HasLen, 1)
			c.Assert(string(loaded.Entries[0].Data), qt.Contains, "CREATE TABLE widgets")
		})
	}
}

// TestComposeMigrationArtifactsCarriesTheRollbackHalf pins that each layout's
// rollback actually reaches the file, in the shape that layout reads it from.
//
// The test above would pass on a composer that dropped the reverse entirely:
// the covered set and the loader both look only at the forward half. This is
// the arm that fails when the rollback goes missing, which is the failure the
// refusal this change removed was standing in for.
func TestComposeMigrationArtifactsCarriesTheRollbackHalf(t *testing.T) {
	tests := []struct {
		name   string
		format atlasmigrateimport.Format
		// index of the artifact carrying the rollback, and the marker that
		// proves the layout's own rollback container is present rather than the
		// SQL merely being in the file somewhere.
		artifact int
		marker   string
	}{
		{name: "golang-migrate", format: atlasmigrateimport.FormatGolangMigrate, artifact: 1, marker: "DROP TABLE widgets;"},
		{name: "flyway", format: atlasmigrateimport.FormatFlyway, artifact: 1, marker: "DROP TABLE widgets;"},
		{name: "goose", format: atlasmigrateimport.FormatGoose, artifact: 0, marker: "-- +goose Down\nDROP TABLE widgets;"},
		{name: "dbmate", format: atlasmigrateimport.FormatDBMate, artifact: 0, marker: "-- migrate:down\nDROP TABLE widgets;"},
		{name: "liquibase", format: atlasmigrateimport.FormatLiquibase, artifact: 0, marker: "--rollback: DROP TABLE widgets;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			artifacts, err := composeMigrationArtifacts(tt.format, "addcol", 20240102030405, []MigrationFileContent{{
				SQL:               "CREATE TABLE widgets (id INTEGER);",
				DownSQL:           "DROP TABLE widgets;",
				Statements:        []string{"CREATE TABLE widgets (id INTEGER)"},
				ReverseStatements: []string{"DROP TABLE widgets"},
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(string(artifacts[tt.artifact].Contents), qt.Contains, tt.marker)
		})
	}
}

func TestComposeMigrationArtifacts_GooseRepresentsWholeFileNoTransaction(t *testing.T) {
	tests := []struct {
		name                 string
		sql                  string
		noTransaction        bool
		reverseNoTransaction bool
		want                 string
	}{
		{
			name: "ordinary file has no no-transaction directive",
			sql:  "CREATE TABLE widgets (id INTEGER);",
			want: "-- +goose Up\nCREATE TABLE widgets (id INTEGER);\n\n" +
				"-- +goose Down\nDROP TABLE widgets;\n",
		},
		{
			name:          "forward requirement marks the whole file",
			sql:           "-- atlas:txmode none\n\nCREATE TABLE widgets (id INTEGER);",
			noTransaction: true,
			want: "-- +goose NO TRANSACTION\n-- +goose Up\nCREATE TABLE widgets (id INTEGER);\n\n" +
				"-- +goose Down\nDROP TABLE widgets;\n",
		},
		{
			name:                 "rollback requirement marks the whole file",
			sql:                  "CREATE TABLE widgets (id INTEGER);",
			reverseNoTransaction: true,
			want: "-- +goose NO TRANSACTION\n-- +goose Up\nCREATE TABLE widgets (id INTEGER);\n\n" +
				"-- +goose Down\nDROP TABLE widgets;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			artifacts, err := composeMigrationArtifacts(
				atlasmigrateimport.FormatGoose,
				"widgets",
				20240102030405,
				[]MigrationFileContent{{
					SQL: test.sql, DownSQL: "DROP TABLE widgets;",
					NoTransaction: test.noTransaction, ReverseNoTransaction: test.reverseNoTransaction,
				}},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(artifacts, qt.HasLen, 1)
			c.Assert(string(artifacts[0].Contents), qt.Equals, test.want)
		})
	}
}

// TestComposeMigrationArtifactsKeepsTheAtlasLayoutUnchanged is the control on
// the whole file: the native layout's file name and bytes are what they were
// before any of this existed.
//
// It is here because the atlas branch runs on every native `migrate diff`, and
// a composer that quietly renamed or re-wrapped that file would move every
// existing migration directory while all five foreign rows above stayed green.
func TestComposeMigrationArtifactsKeepsTheAtlasLayoutUnchanged(t *testing.T) {
	c := qt.New(t)

	artifacts, err := composeMigrationArtifacts(
		atlasmigrateimport.FormatAtlas,
		"add col",
		20240102030405,
		[]MigrationFileContent{
			{SQL: "SELECT 1;", NameSuffix: "_transactional"},
			{
				SQL: "-- atlas:txmode none\n\nSELECT 2;", NameSuffix: "_concurrent_indexes", DownSQL: "SELECT 3;",
				NoTransaction:        true,
				ReverseNoTransaction: true,
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(artifacts, qt.DeepEquals, []PublicationArtifact{
		{Name: "20240102030405_add_col_transactional.sql", Contents: []byte("SELECT 1;")},
		{
			Name:     "20240102030406_add_col_concurrent_indexes.sql",
			Contents: []byte("-- atlas:txmode none\n\nSELECT 2;"),
		},
	})
}
