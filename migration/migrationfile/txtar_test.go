package migrationfile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func TestParseAtlasTxtar(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := migrationfile.ParseAtlasTxtar("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.HasDown, qt.IsTrue)
	c.Assert(parsed.MigrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.DownSQL, qt.Contains, "DELETE FROM users")
}

func TestParseAtlasTxtarWithoutDown(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := migrationfile.ParseAtlasTxtar("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.HasDown, qt.IsFalse)
	c.Assert(parsed.MigrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.DownSQL, qt.Equals, "")
}

func TestParseAtlasTxtarRequiresMigrationSection(t *testing.T) {
	c := qt.New(t)

	_, ok, err := migrationfile.ParseAtlasTxtar("20240305171146_seed.sql", `-- atlas:txtar

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(ok, qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `invalid Atlas txtar migration 20240305171146_seed.sql: missing migration.sql section`)
}

func TestParseAtlasTxtarRejectsSQLBeforeSection(t *testing.T) {
	c := qt.New(t)

	_, ok, err := migrationfile.ParseAtlasTxtar("20240305171146_seed.sql", `-- atlas:txtar
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- migration.sql --
SELECT 1;
`)
	c.Assert(ok, qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `invalid Atlas txtar migration 20240305171146_seed.sql: SQL appears before the first txtar section`)
}

func TestParseAtlasTxtarIgnoresUnknownCommentMarkers(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := migrationfile.ParseAtlasTxtar("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
-- keep this comment --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.MigrationSQL, qt.Contains, "-- keep this comment --")
	c.Assert(parsed.MigrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.HasDown, qt.IsTrue)
}

func TestParseAtlasTxtarIgnoresUnknownFileSections(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := migrationfile.ParseAtlasTxtar("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- schema.sql --
THIS IS NOT MIGRATION SQL;

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.MigrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.MigrationSQL, qt.Not(qt.Contains), "THIS IS NOT MIGRATION SQL")
	c.Assert(parsed.DownSQL, qt.Contains, "DELETE FROM users")
}
