package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func TestDiscoverAtlasRepeatables_HappyPath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_init.sql":       &fstest.MapFile{Data: []byte("CREATE TABLE t (id int);\n")},
		"2R_seed.sql":      &fstest.MapFile{Data: []byte("INSERT INTO t VALUES (1);\n")},
		"R__view.sql":      &fstest.MapFile{Data: []byte("CREATE VIEW v AS SELECT id FROM t;\n")},
		"R__a-first.sql":   &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		"sub/R__inner.sql": &fstest.MapFile{Data: []byte("SELECT 2;\n")},
		"atlas.sum":        &fstest.MapFile{Data: []byte("h1:unused\n")},
	}

	files, repeatables, err := migrationfile.DiscoverAtlasRepeatables(fsys)

	c.Assert(err, qt.IsNil)
	versions := make([]string, 0, len(files))
	for _, file := range files {
		versions = append(versions, file.RevisionVersion())
	}
	c.Assert(versions, qt.DeepEquals, []string{"1", "2R"})
	c.Assert(repeatables, qt.DeepEquals, []migrationfile.RepeatableFile{
		{Path: "R__a-first.sql", Identity: "a-first"},
		{Path: "R__view.sql", Identity: "view"},
	})
}

func TestDiscoverAtlasRepeatables_FailurePath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_init.sql":     &fstest.MapFile{Data: []byte("CREATE TABLE t (id int);\n")},
		"R__view.up.sql": &fstest.MapFile{Data: []byte("CREATE VIEW v AS SELECT id FROM t;\n")},
	}

	files, repeatables, err := migrationfile.DiscoverAtlasRepeatables(fsys)

	c.Assert(err, qt.ErrorMatches, `repeatable migration file "R__view.up.sql": the identity after R__ takes only .*`)
	c.Assert(files, qt.IsNil)
	c.Assert(repeatables, qt.IsNil)
}
