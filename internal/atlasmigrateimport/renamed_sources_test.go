package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

// TestLoadedRenamedSources maps each converted file whose name the conversion
// changed back to the file the source directory holds, and leaves out a file
// that keeps its name.
func TestLoadedRenamedSources(t *testing.T) {
	tests := []struct {
		name   string
		format atlasmigrateimport.Format
		source fstest.MapFS
		want   map[string]string
	}{
		{
			name:   "golang-migrate up files lose their direction",
			format: atlasmigrateimport.FormatGolangMigrate,
			source: fstest.MapFS{
				"1_a.up.sql":        {Data: []byte("CREATE TABLE a (id int);\n")},
				"1_a.down.sql":      {Data: []byte("DROP TABLE a;\n")},
				"000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id int);\n")},
				"000002_b.down.sql": {Data: []byte("DROP TABLE b;\n")},
			},
			want: map[string]string{"1_a.sql": "1_a.up.sql", "000002_b.sql": "000002_b.up.sql"},
		},
		{
			name:   "flyway versions become numeric",
			format: atlasmigrateimport.FormatFlyway,
			source: fstest.MapFS{
				"V1__a.sql":   {Data: []byte("CREATE TABLE a (id int);\n")},
				"V1.5__b.sql": {Data: []byte("CREATE TABLE b (id int);\n")},
			},
			want: map[string]string{"4611686018427469511_a.sql": "V1__a.sql", "4611686018427471935_b.sql": "V1.5__b.sql"},
		},
		{
			name:   "atlas files keep their names",
			format: atlasmigrateimport.FormatAtlas,
			source: fstest.MapFS{
				"1_a.sql": {Data: []byte("CREATE TABLE a (id int);\n")},
			},
			want: nil,
		},
		{
			name:   "goose files keep their names",
			format: atlasmigrateimport.FormatGoose,
			source: fstest.MapFS{
				"1_a.sql": {Data: []byte("-- +goose Up\nCREATE TABLE a (id int);\n-- +goose Down\nDROP TABLE a;\n")},
			},
			want: nil,
		},
		{
			name:   "dbmate files keep their names",
			format: atlasmigrateimport.FormatDBMate,
			source: fstest.MapFS{
				"20200101000000_a.sql": {Data: []byte("-- migrate:up\nCREATE TABLE a (id int);\n-- migrate:down\nDROP TABLE a;\n")},
			},
			want: nil,
		},
		{
			name:   "liquibase changesets are no single file",
			format: atlasmigrateimport.FormatLiquibase,
			source: fstest.MapFS{
				"1_init.sql": {Data: []byte("--liquibase formatted sql\n--changeset atlas:1-1\nCREATE TABLE a (id int);\n")},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			loaded, err := atlasmigrateimport.LoadFS(tt.source, "migrations", tt.format)
			c.Assert(err, qt.IsNil)
			c.Assert(loaded.RenamedSources(), qt.DeepEquals, tt.want)
		})
	}
}
