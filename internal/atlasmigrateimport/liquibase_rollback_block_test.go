package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

// A numbered Liquibase file is copied with neither its --rollback lines nor its
// /* liquibase rollback block, since an Atlas migration is up-only
// (stokaro/ptah#3735). Kept in, the block below would not be a SQL comment:
// SQL ends it at `a last */`, so `migrate apply` would run the rest of the
// rollback as up SQL and fail on the closing `*/`. Liquibase 5.0.4 ends the
// block at the line that ends in `*/`, and its update created both tables.
func TestLoadFSLiquibaseNumberedRollback_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n" +
			"--rollback DROP TABLE a;\n/* liquibase rollback\nDROP TABLE a; /* a last */ DROP TABLE b;\n*/\n" +
			"CREATE TABLE b (id int);\n")},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.IsNil)
	c.Assert(string(loaded.Entries[0].Data), qt.Equals,
		"--changeset s:1\nCREATE TABLE a (id int);\nCREATE TABLE b (id int);\n")
}

// A block no line closes is refused, as Liquibase 5.0.4 refuses the changelog.
func TestLoadFSLiquibaseNumberedRollback_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: []byte("--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n" +
			"/* liquibase rollback\nDROP TABLE t;\n")},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.ErrorMatches, `liquibase changeset s:1 in "1_init\.sql" opens a /\* liquibase rollback block `+
		`that no line closes, and Liquibase refuses the changelog -- end the block with a line that ends in \*/`)
	c.Assert(loaded, qt.IsNil)
}
