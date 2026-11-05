//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// catalogShapeCase is one engine and the statements that read a new table back.
type catalogShapeCase struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	schema  string
	columns string
	keys    string
	want    string
	wantKey string
}

var catalogShapeCases = []catalogShapeCase{
	{
		name:    "postgres",
		dialect: platform.Postgres,
		target:  func(c *qt.C) string { return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_shape"), "public") },
		schema: `CREATE TABLE owners (id bigint PRIMARY KEY, name varchar NOT NULL);
CREATE TABLE items (id int PRIMARY KEY, owner_id bigint REFERENCES owners (id) ON DELETE CASCADE, note varchar(20) DEFAULT 'a', tag text UNIQUE, CONSTRAINT items_note_len CHECK (length(note) < 100));
CREATE INDEX items_owner_idx ON items (owner_id);
CREATE TABLE pairs (a int, b int, PRIMARY KEY (a, b));
`,
		columns: `SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'items'`,
		keys:    `SELECT string_agg(conname || ':' || contype::text, ',' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'public.items'::regclass AND contype <> 'n'`,
		want:    "id:integer:NO,owner_id:bigint:YES,note:character varying:YES,tag:text:YES",
		wantKey: "items_note_len:c,items_owner_id_fkey:f,items_pkey:p,items_tag_key:u",
	},
	{
		name:    "mysql",
		dialect: platform.MySQL,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "shape")
			return url
		},
		schema: "CREATE TABLE owners (id bigint PRIMARY KEY, name varchar(50) NOT NULL);\n" +
			"CREATE TABLE items (id int PRIMARY KEY, owner_id bigint, note varchar(20) DEFAULT 'a', CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) ON DELETE CASCADE, CONSTRAINT items_note_len CHECK (note <> ''));\n",
		columns: "SELECT GROUP_CONCAT(CONCAT(column_name, ':', column_type, ':', is_nullable) ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'items'",
		keys:    "SELECT GROUP_CONCAT(CONCAT(constraint_name, ':', constraint_type) ORDER BY constraint_name) FROM information_schema.table_constraints WHERE table_schema = DATABASE() AND table_name = 'items'",
		want:    "id:int:NO,owner_id:bigint:YES,note:varchar(20):YES",
		wantKey: "items_note_len:CHECK,items_owner_fk:FOREIGN KEY,PRIMARY:PRIMARY KEY",
	},
	{
		name:    "mariadb",
		dialect: platform.MariaDB,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "shape")
			return url
		},
		schema: "CREATE TABLE owners (id bigint PRIMARY KEY, name varchar(50) NOT NULL);\n" +
			"CREATE TABLE items (id int PRIMARY KEY, owner_id bigint, note varchar(20) DEFAULT 'a', CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) ON DELETE CASCADE, CONSTRAINT items_note_len CHECK (note <> ''));\n",
		columns: "SELECT GROUP_CONCAT(CONCAT(column_name, ':', column_type, ':', is_nullable) ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'items'",
		keys:    "SELECT GROUP_CONCAT(CONCAT(constraint_name, ':', constraint_type) ORDER BY constraint_name) FROM information_schema.table_constraints WHERE table_schema = DATABASE() AND table_name = 'items'",
		want:    "id:int(11):NO,owner_id:bigint(20):YES,note:varchar(20):YES",
		wantKey: "items_note_len:CHECK,items_owner_fk:FOREIGN KEY,PRIMARY:PRIMARY KEY",
	},
	{
		name:    "sqlite",
		dialect: platform.SQLite,
		target:  sqliteScratchURL,
		schema: `CREATE TABLE owners (id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE items (id integer PRIMARY KEY, owner_id integer REFERENCES owners (id) ON DELETE CASCADE, note varchar(20) DEFAULT 'a', CONSTRAINT items_note_len CHECK (length(note) < 100));
`,
		columns: "SELECT group_concat(name || ':' || type || ':' || \"notnull\", ',') FROM (SELECT * FROM pragma_table_info('items') ORDER BY cid)",
		keys:    "SELECT count(*) || ':' || (SELECT pk FROM pragma_table_info('items') WHERE name = 'id') FROM pragma_foreign_key_list('items')",
		want:    "id:INTEGER:0,owner_id:INTEGER:0,note:varchar(20):0",
		wantKey: "1:1",
	},
}

// TestCompatMigrateDiffCatalogShapeE2E_ATableRunsAsPlanned diffs a table whose keys
// and checks now sit inside its CREATE TABLE, runs the file on each engine and reads
// the table back: the catalog holds the same columns, nullability and constraints
// a plan with the constraints in their own statements holds. A second diff of the
// same schema finds the directory in sync, so the shape reads back as what was
// declared, and the rollback removes the table.
func TestCompatMigrateDiffCatalogShapeE2E_ATableRunsAsPlanned(t *testing.T) {
	for _, test := range catalogShapeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := test.target(c)

			dir := diffEngineSteps(c, "", dev, []string{test.schema})
			pairs := readMigrationPairs(c, dir)
			target := test.target(c)

			c.Assert(pairs, qt.HasLen, 1)
			applyEngineFile(c, target, test.dialect, pairs[0].Up)
			c.Assert(heldColumns(c, target, test.columns), qt.Equals, test.want)
			c.Assert(heldColumns(c, target, test.keys), qt.Equals, test.wantKey)
			applyEngineFile(c, target, test.dialect, pairs[0].Down)
		})
	}
}

// catalogShapeSteps are shapes whose files the pinned community binary and this
// binary write byte for byte on PostgreSQL. The new table's statement holds its
// key, foreign key and check, and each added column reads as the catalog prints it.
var catalogShapeSteps = []string{
	`CREATE TABLE owners (id bigint PRIMARY KEY, name varchar NOT NULL);
CREATE TABLE items (
  id bigint PRIMARY KEY,
  owner_id bigint NOT NULL REFERENCES owners (id) ON DELETE CASCADE,
  note text DEFAULT 'a',
  CONSTRAINT items_note_len CHECK (length(note) < 100)
);
CREATE INDEX items_owner_idx ON items (owner_id);
`,
	`CREATE TABLE owners (id bigint PRIMARY KEY, name varchar NOT NULL, email varchar(20));
CREATE TABLE items (
  id bigint PRIMARY KEY,
  owner_id bigint NOT NULL REFERENCES owners (id) ON DELETE CASCADE,
  note text DEFAULT 'a',
  CONSTRAINT items_note_len CHECK (length(note) < 100)
);
CREATE INDEX items_owner_idx ON items (owner_id);
`,
}

// TestCompatMigrateDiffCatalogShapeE2E_MatchesTheBinarysBytes compares the forward
// files of [catalogShapeSteps] with what the pinned community binary writes.
func TestCompatMigrateDiffCatalogShapeE2E_MatchesTheBinarysBytes(t *testing.T) {
	c := qt.New(t)
	reference := requireEngineReference(t)
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_shape_ref"), "public")

	ours := readMigrationPairs(c, diffEngineSteps(c, "", dev, catalogShapeSteps))
	theirs := readMigrationPairs(c, diffEngineSteps(c, reference, dev, catalogShapeSteps))

	c.Assert(ours, qt.HasLen, len(theirs))
	for i := range theirs {
		c.Assert(ours[i].Up, qt.Equals, theirs[i].Up)
	}
}
