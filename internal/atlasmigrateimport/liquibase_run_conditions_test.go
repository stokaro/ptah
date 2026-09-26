package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/migration/importer"
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
		{
			name: "ignore",
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 ignore:true\nSELECT 2;\n",
			},
			message: `liquibase changeset s:2 in "1_init\.sql" sets ignore, so Liquibase never runs it, .*`,
		},
		{
			name: "transaction modes mixed in one file",
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n--changeset s:1 runInTransaction:false\nVACUUM;\n--changeset s:2\nSELECT 2;\n",
			},
			message: `liquibase file "1_init\.sql" sets runInTransaction to false on some changesets and not on others, .*`,
		},
		{
			name: "failOnError false",
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n--changeset s:1 failOnError:false\nSELECT 1;\n",
			},
			message: `liquibase changeset s:1 in "1_init\.sql" sets failOnError=false, which Ptah cannot carry .*`,
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

// The conversion reads attributes and nothing else. The defaults spelled out
// and a layout the changeset parser would refuse are carried by the copy as they
// are, so the converted file stays the one Atlas CE writes.
func TestLoadFSLiquibaseNumberedRunConditions_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\nSELECT 0;\n" +
			"--changeset atlas:1-1 runAlways:false ignore:false dbms:\nCREATE TABLE t (id int);\n")},
		"2_skeleton.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\n")},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.IsNil)
	c.Assert(entryNames(loaded), qt.DeepEquals, []string{"1_init.sql", "2_skeleton.sql"})
	c.Assert(string(loaded.Entries[0].Data), qt.Equals,
		"SELECT 0;\n--changeset atlas:1-1 runAlways:false ignore:false dbms:\nCREATE TABLE t (id int);\n")
}

// A file whose changesets all set runInTransaction to false converts to a
// no-transaction Atlas migration. `ptah-compat migrate diff` writes that
// attribute for a statement that cannot run in a transaction, and Atlas CE
// v1.3.0 reads it as nothing: its apply of such a file fails with "cannot
// VACUUM from within a transaction" on SQLite (stokaro/ptah#3714).
func TestLoadFSLiquibaseNumberedNoTransaction_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"1_vacuum.sql": &fstest.MapFile{Data: []byte(
			"--liquibase formatted sql\n--changeset atlas:1-1 runInTransaction:false\nVACUUM;\n")},
		"2_table.sql": &fstest.MapFile{Data: []byte(
			"--liquibase formatted sql\n--changeset atlas:2-1\nCREATE TABLE t (id int);\n")},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.IsNil)
	c.Assert(entryNames(loaded), qt.DeepEquals, []string{"1_vacuum.sql", "2_table.sql"})
	c.Assert(string(loaded.Entries[0].Data), qt.Equals,
		"-- atlas:txmode none\n\n--changeset atlas:1-1 runInTransaction:false\nVACUUM;\n")
	c.Assert(string(loaded.Entries[1].Data), qt.Equals, "--changeset atlas:2-1\nCREATE TABLE t (id int);\n")
}

// The import that splits a Liquibase changelog into changesets carries each
// changeset's runInTransaction:false as the Atlas no-transaction directive, and
// leaves an ignored changeset out while naming it in the result, whichever of
// the two changelog shapes it reads.
func TestImportLiquibaseChangesetAttributes_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{
			name: "conventional formatted sql",
			file: "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 runInTransaction:false\nVACUUM;\n" +
				"--changeset s:2 ignore:true\nDROP TABLE t;\n",
		},
		{
			name: "xml changelog",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runInTransaction="false"><sql>VACUUM;</sql></changeSet>` +
				`<changeSet id="2" author="s" ignore="true"><sql>DROP TABLE t;</sql></changeSet></databaseChangeLog>`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := t.TempDir()
			target := t.TempDir()
			writeFile(c, source, test.file, test.content)

			result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
				FromURL: "file://" + source + "?format=liquibase",
				ToURL:   "file://" + target,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(baseNames(result.Files), qt.DeepEquals, []string{"1_s_1.sql"})
			c.Assert(readFile(c, target, "1_s_1.sql"), qt.Equals, "-- atlas:txmode none\n\nVACUUM;\n")
			c.Assert(result.SkippedChangesets, qt.DeepEquals, []importer.SkippedChangeset{{
				Path: test.file, Changeset: "s:2", Reason: `ignore="true": Liquibase never runs this changeset`,
			}})
			assertAtlasSumOK(c, target, result.SumFile)
		})
	}
}

// A numbered file is copied with only the lines Liquibase reads: an
// --ignoreLines directive and the lines it skips are left out, where Atlas CE
// v1.3.0 copies them and runs them (stokaro/ptah#3727).
func TestLoadFSLiquibaseNumberedIgnoreLines_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t1 (id int);\n" +
			"--ignoreLines:start\nCREATE TABLE ignored_block (id int);\n--ignoreLines:end\n--changeset s:2\n" +
			"--ignoreLines:1\nCREATE TABLE ignored_count (id int);\nCREATE TABLE t2 (id int);\n")},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.IsNil)
	c.Assert(string(loaded.Entries[0].Data), qt.Equals,
		"--changeset s:1\nCREATE TABLE t1 (id int);\n--changeset s:2\nCREATE TABLE t2 (id int);\n")
}

// A property reference in a numbered file, and a directive Liquibase refuses,
// refuse the conversion.
func TestLoadFSLiquibaseNumberedIgnoreLines_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		message string
	}{
		{
			name:    "a property reference",
			body:    "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE ${tbl} (id int);\n",
			message: `liquibase changeset s:1 in "1_init\.sql" uses the property reference \$\{tbl\}; .*`,
		},
		{
			name:    "start in upper case",
			body:    "--liquibase formatted sql\n--changeset s:1\n--ignoreLines:START\nSELECT 1;\n",
			message: `liquibase changelog "1_init\.sql" line 3: "--ignoreLines:START" names neither start nor a number of lines, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fstest.MapFS{"1_init.sql": &fstest.MapFile{Data: []byte(test.body)}}

			loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(loaded, qt.IsNil)
		})
	}
}
