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

// catalogBytesCases are plans whose two files the pinned community binary and this
// binary write byte for byte on PostgreSQL, so the shape and the order of both
// directions are the binary's. A column that gains NOT NULL is left out: Ptah writes
// a report above that statement. So is a check over a column that changes type, which
// Ptah keeps and the binary drops and adds again.
var catalogBytesCases = []struct {
	name  string
	steps []string
}{
	{name: "a new table with its keys and an added column", steps: []string{
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
	}},
	{name: "new tables without a key ahead of the ones with", steps: []string{
		"CREATE TABLE owners (id bigint PRIMARY KEY);\nCREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint REFERENCES owners (id));\nCREATE TABLE pairs (a int, b int, PRIMARY KEY (a, b));\n",
	}},
	{name: "the changes of one table in one statement", steps: []string{
		"CREATE TABLE t (id int PRIMARY KEY, a int, b int, c text);\nCREATE TABLE u (id int PRIMARY KEY, x int);\n",
		"CREATE TABLE t (id int PRIMARY KEY, a bigint, c text DEFAULT 'x', d int, e int);\nCREATE TABLE u (id int PRIMARY KEY, y int);\nCREATE TABLE n (id int PRIMARY KEY);\n",
		"CREATE TABLE t (id int PRIMARY KEY, a bigint, c text DEFAULT 'x', d int, e int);\nCREATE TABLE n (id int PRIMARY KEY);\n",
	}},
	{name: "a primary key that moves", steps: []string{
		"CREATE TABLE t (id int PRIMARY KEY, k int NOT NULL);\n",
		"CREATE TABLE t (id int NOT NULL, k int NOT NULL, x int, PRIMARY KEY (k));\n",
	}},
	{name: "a key into a column a table that sorts later gains", steps: []string{
		"CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE z (id int PRIMARY KEY);\n",
		"CREATE TABLE z (id int PRIMARY KEY, code int, CONSTRAINT z_code_key UNIQUE (code));\nCREATE TABLE a (id int PRIMARY KEY, z_code int, CONSTRAINT a_z_fk FOREIGN KEY (z_code) REFERENCES z (code));\n",
	}},
	{name: "changed, new and dropped tables in order", steps: []string{
		"CREATE TABLE a (id int PRIMARY KEY, x int);\nCREATE TABLE b (id int PRIMARY KEY, x int);\nCREATE INDEX b_x ON b (x);\nCREATE TABLE d (id int PRIMARY KEY);\n",
		"CREATE TABLE a (id int PRIMARY KEY, y int);\nCREATE TABLE b (id int PRIMARY KEY, x bigint);\nCREATE INDEX b_x2 ON b (x);\nCREATE TABLE c (id int PRIMARY KEY, b_id int REFERENCES b (id));\nCREATE INDEX c_b ON c (b_id);\n",
	}},
	{name: "indexes by name", steps: []string{
		"CREATE TABLE t (id int PRIMARY KEY, a int, b int);\nCREATE INDEX i1 ON t (a);\nCREATE UNIQUE INDEX i2 ON t (b);\n",
		"CREATE TABLE t (id int PRIMARY KEY, a int, b int);\nCREATE INDEX i3 ON t (a);\nCREATE UNIQUE INDEX i2 ON t (b, a);\n",
	}},
	{name: "tables that reference each other", steps: []string{
		`CREATE TABLE p (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE q (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int, b int, c text UNIQUE, CONSTRAINT uq_ab UNIQUE (a, b), CONSTRAINT fk_p FOREIGN KEY (a, b) REFERENCES p (a, b) ON DELETE SET NULL ON UPDATE CASCADE, CONSTRAINT ck CHECK (a > 0));
CREATE TABLE r (id int PRIMARY KEY, parent int REFERENCES r (id));
CREATE TABLE z1 (id int PRIMARY KEY, other int);
CREATE TABLE z2 (id int PRIMARY KEY, other int REFERENCES z1 (id));
ALTER TABLE z1 ADD CONSTRAINT z1_other_fk FOREIGN KEY (other) REFERENCES z2 (id);
`,
	}},
	{name: "a foreign key that changes", steps: []string{
		"CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id));\n",
		"CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id) ON DELETE CASCADE);\n",
	}},
}

// catalogForwardBytesCases are plans whose forward file matches the pinned community
// binary and whose rollback does not, deliberately. The binary writes the rollback as the
// forward file backwards, `DROP COLUMN "d", ... DROP CONSTRAINT "ck"`, and PostgreSQL 18
// answers `constraint "ck" of relation "t" does not exist`: dropping a column takes the
// check over it. This binary drops the check first.
var catalogForwardBytesCases = []struct {
	name  string
	steps []string
}{
	{name: "a check over a column the plan adds", steps: []string{
		"CREATE TABLE t (id int PRIMARY KEY, a int, b int, c text);\n",
		"CREATE TABLE t (id int PRIMARY KEY, a bigint, c text DEFAULT 'x', d int, e int, CONSTRAINT ck CHECK (d > 0));\n",
	}},
}

// TestCompatMigrateDiffCatalogShapeE2E_MatchesTheBinarysForwardBytes compares the forward
// file of each step of [catalogForwardBytesCases] with what the pinned community binary
// writes.
func TestCompatMigrateDiffCatalogShapeE2E_MatchesTheBinarysForwardBytes(t *testing.T) {
	reference := requireEngineReference(t)
	for _, test := range catalogForwardBytesCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_shape_ref"), "public")

			ours := readMigrationPairs(c, diffEngineSteps(c, "", dev, test.steps))
			theirs := readMigrationPairs(c, diffEngineSteps(c, reference, dev, test.steps))

			c.Assert(ours, qt.HasLen, len(theirs))
			for i := range theirs {
				c.Assert(ours[i].Up, qt.Equals, theirs[i].Up)
			}
		})
	}
}

// TestCompatMigrateDiffCatalogShapeE2E_MatchesTheBinarysBytes compares both files of
// each step of [catalogBytesCases] with what the pinned community binary writes.
func TestCompatMigrateDiffCatalogShapeE2E_MatchesTheBinarysBytes(t *testing.T) {
	reference := requireEngineReference(t)
	for _, test := range catalogBytesCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_shape_ref"), "public")

			ours := readMigrationPairs(c, diffEngineSteps(c, "", dev, test.steps))
			theirs := readMigrationPairs(c, diffEngineSteps(c, reference, dev, test.steps))

			c.Assert(ours, qt.HasLen, len(theirs))
			for i := range theirs {
				c.Assert(ours[i].Up, qt.Equals, theirs[i].Up)
				c.Assert(ours[i].Down, qt.Equals, theirs[i].Down)
			}
		})
	}
}

// catalogRunCases are plans that group and move statements, on each engine that takes
// the shape. Each row ends where it started, so the files have to run both ways.
var catalogRunCases = []struct {
	name    string
	dialect string
	engine  string
	steps   []string
	tables  string
	want    []string
}{
	{
		name: "postgres", dialect: platform.Postgres, engine: "postgres",
		steps: []string{
			"CREATE TABLE a (id int PRIMARY KEY, x int, g int GENERATED ALWAYS AS (x * 2) STORED);\nCREATE TABLE b (id int PRIMARY KEY, x int);\nCREATE INDEX b_x ON b (x);\nCREATE TABLE d (id int PRIMARY KEY, a_id int REFERENCES a (id));\n",
			"CREATE TABLE a (id int PRIMARY KEY, y int);\nCREATE TABLE b (id int PRIMARY KEY, x bigint);\nCREATE INDEX b_x2 ON b (x);\nCREATE TABLE c (id int PRIMARY KEY, b_id int REFERENCES b (id));\n",
		},
		tables: "SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = 'public'",
		want: []string{
			"a.id,a.x,a.g,b.id,b.x,d.id,d.a_id",
			"a.id,a.y,b.id,b.x,c.id,c.b_id",
		},
	},
	{
		name: "mysql", dialect: platform.MySQL, engine: "mysql",
		steps: []string{
			"CREATE TABLE a (id int PRIMARY KEY, x int);\nCREATE TABLE b (id int PRIMARY KEY, x int);\nCREATE INDEX b_x ON b (x);\nCREATE TABLE d (id int PRIMARY KEY, a_id int, CONSTRAINT d_a FOREIGN KEY (a_id) REFERENCES a (id));\n",
			"CREATE TABLE a (id int PRIMARY KEY, y int);\nCREATE TABLE b (id int PRIMARY KEY, x bigint);\nCREATE INDEX b_x2 ON b (x);\nCREATE TABLE c (id int PRIMARY KEY, b_id int, CONSTRAINT c_b FOREIGN KEY (b_id) REFERENCES b (id));\n",
		},
		tables: "SELECT GROUP_CONCAT(CONCAT(table_name, '.', column_name) ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE()",
		want: []string{
			"a.id,a.x,b.id,b.x,d.id,d.a_id",
			"a.id,a.y,b.id,b.x,c.id,c.b_id",
		},
	},
	{
		name: "mariadb", dialect: platform.MariaDB, engine: "mariadb",
		steps: []string{
			"CREATE TABLE a (id int PRIMARY KEY, x int);\nCREATE TABLE b (id int PRIMARY KEY, x int);\nCREATE INDEX b_x ON b (x);\nCREATE TABLE d (id int PRIMARY KEY, a_id int, CONSTRAINT d_a FOREIGN KEY (a_id) REFERENCES a (id));\n",
			"CREATE TABLE a (id int PRIMARY KEY, y int);\nCREATE TABLE b (id int PRIMARY KEY, x bigint);\nCREATE INDEX b_x2 ON b (x);\nCREATE TABLE c (id int PRIMARY KEY, b_id int, CONSTRAINT c_b FOREIGN KEY (b_id) REFERENCES b (id));\n",
		},
		tables: "SELECT GROUP_CONCAT(CONCAT(table_name, '.', column_name) ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE()",
		want: []string{
			"a.id,a.x,b.id,b.x,d.id,d.a_id",
			"a.id,a.y,b.id,b.x,c.id,c.b_id",
		},
	},
	{
		name: "sqlite", dialect: platform.SQLite, engine: "sqlite",
		steps: []string{
			"CREATE TABLE a (id integer PRIMARY KEY, x int);\nCREATE TABLE b (id integer PRIMARY KEY, x int);\nCREATE TABLE d (id integer PRIMARY KEY);\n",
			"CREATE TABLE a (id integer PRIMARY KEY, x int, y int, z int);\nCREATE TABLE b (id integer PRIMARY KEY, x int, w int);\nCREATE TABLE c (id integer PRIMARY KEY, b_id int REFERENCES b (id));\n",
		},
		tables: "SELECT group_concat(item, ',') FROM (SELECT m.name || '.' || p.name AS item FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type = 'table' ORDER BY m.name, p.cid)",
		want: []string{
			"a.id,a.x,b.id,b.x,d.id",
			"a.id,a.x,a.y,a.z,b.id,b.x,b.w,c.id,c.b_id",
		},
	},
}

// catalogRunTarget is an empty database of engine.
func catalogRunTarget(c *qt.C, engine string) string {
	c.Helper()
	for _, candidate := range catalogShapeCases {
		if candidate.name == engine {
			return candidate.target(c)
		}
	}
	c.Fatalf("no target for %s", engine)
	return ""
}

// TestCompatMigrateDiffCatalogShapeE2E_AGroupedPlanRunsBothWays runs each step's
// forward file and then its rollback on the engine, reading the columns back after
// each. The rollback reads as the forward file backwards, so a statement that moved
// over one it needs stops here.
func TestCompatMigrateDiffCatalogShapeE2E_AGroupedPlanRunsBothWays(t *testing.T) {
	for _, test := range catalogRunCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", catalogRunTarget(c, test.engine), test.steps))
			target := catalogRunTarget(c, test.engine)

			c.Assert(pairs, qt.HasLen, len(test.steps))
			applyEngineFile(c, target, test.dialect, pairs[0].Up)
			c.Assert(heldColumns(c, target, test.tables), qt.Equals, test.want[0])
			applyEngineFile(c, target, test.dialect, pairs[1].Up)
			c.Assert(heldColumns(c, target, test.tables), qt.Equals, test.want[1])
			applyEngineFile(c, target, test.dialect, pairs[1].Down)
			c.Assert(heldColumns(c, target, test.tables), qt.Equals, test.want[0])
		})
	}
}

// TestCompatSchemaVerbsE2E_WriteTheCatalogsForm holds `schema diff` and `schema apply`
// to the form `migrate diff` writes, and runs the apply on the server.
func TestCompatSchemaVerbsE2E_WriteTheCatalogsForm(t *testing.T) {
	c := qt.New(t)
	from, to := schemaVerbsFiles(c)
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_shape_verbs_dev"), "public")
	target := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_shape_verbs_target"), "public")
	const want = `ADD COLUMN "b" integer NULL`

	diffed, err := runCompatVerb("schema", "diff", "--from", from, "--to", to, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", diffed))
	c.Assert(diffed, qt.Contains, want)

	first, err := runCompatVerb("schema", "apply", "--url", target, "--to", from, "--dev-url", dev, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	applied, err := runCompatVerb("schema", "apply", "--url", target, "--to", to, "--dev-url", dev, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(applied, qt.Contains, want)
	c.Assert(heldColumns(c, target, "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns "+
		"WHERE table_schema = 'public' AND table_name = 't'"), qt.Equals, "id,a,b")
}
