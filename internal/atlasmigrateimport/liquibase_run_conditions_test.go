package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

// A numbered Liquibase file is copied whole, and a copy of a changeset
// Liquibase runs on one database only, or runs again after its first run, runs
// everywhere, once (stokaro/ptah#3713). The conversion refuses it by name, as
// the changeset parser does; the pinned community binary v1.3.0 copies it and
// applies it everywhere. Direct apply and import both read through LoadFS.
func TestLoadFSLiquibaseNumberedRunConditions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		message string
	}{
		{
			name: "dbms",
			files: map[string]string{
				"1_only_mysql.sql": "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE only_on_mysql (id int);\n",
			},
			message: `liquibase changeset s:1 in "1_only_mysql\.sql" is conditional on dbms; .*`,
		},
		{
			name: "runOnChange in the second file",
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n",
				"2_view.sql": "--liquibase formatted sql\n--changeset s:2 runOnChange:true\nCREATE VIEW v AS SELECT 1;\n",
			},
			message: `liquibase changeset s:2 in "2_view\.sql" sets runOnChange, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fstest.MapFS{}
			for name, body := range test.files {
				source[name] = &fstest.MapFile{Data: []byte(body)}
			}

			loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(loaded, qt.IsNil)
		})
	}
}

// The refusal reads run conditions and nothing else. An attribute that decides
// nothing about running the changeset, the defaults spelled out, and a layout
// the changeset parser would refuse are carried by the copy as they are, so the
// converted file stays the one Atlas CE writes.
func TestLoadFSLiquibaseNumberedRunConditions_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\nSELECT 0;\n" +
			"--changeset atlas:1-1 runInTransaction:false runAlways:false dbms:\nCREATE TABLE t (id int);\n")},
		"2_skeleton.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\n")},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.IsNil)
	c.Assert(entryNames(loaded), qt.DeepEquals, []string{"1_init.sql", "2_skeleton.sql"})
	c.Assert(string(loaded.Entries[0].Data), qt.Equals,
		"SELECT 0;\n--changeset atlas:1-1 runInTransaction:false runAlways:false dbms:\nCREATE TABLE t (id int);\n")
}
