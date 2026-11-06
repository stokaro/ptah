//go:build integration

package integration_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// catalogRowCases are plans that run over rows, read back after each direction. A statement
// the server takes on an empty table can still lose a row's value, and a backfill splits a
// table's changes into several statements, so a plan with no rows says nothing about either.
// rows holds what the query reads forward and back holds what it reads after the rollback,
// which leaves a backfill's values in place: nothing can tell them from values written by
// hand.
var catalogRowCases = []struct {
	name    string
	engines []string
	from    string
	seed    string
	to      string
	query   string
	rows    []string
	back    []string
}{
	{
		name: "a backfill and a check over a new column", engines: []string{"postgres"},
		from:  "CREATE TABLE orders (id int PRIMARY KEY, status text, total int);\n",
		seed:  "INSERT INTO orders VALUES (1, NULL, 10), (2, 'paid', 20);\n",
		to:    "CREATE TABLE orders (id int PRIMARY KEY, status text NOT NULL DEFAULT 'new', total int, discount int, CONSTRAINT discount_ok CHECK (discount <= total));\n",
		query: "SELECT id || ':' || status || ':' || total FROM orders",
		rows:  []string{"1:new:10", "2:paid:20"},
		back:  []string{"1:new:10", "2:paid:20"},
	},
	{
		name: "two backfills and a check over two new columns", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY, b int, a int);\n",
		seed:  "INSERT INTO t VALUES (1, NULL, NULL);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, b int NOT NULL DEFAULT 1, a int NOT NULL DEFAULT 2, z int, y int, CONSTRAINT ck CHECK (y > z));\n",
		query: "SELECT id || ':' || b || ':' || a FROM t",
		rows:  []string{"1:1:2"},
		back:  []string{"1:1:2"},
	},
	{
		name: "a backfill from a default the column already has", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY, a int DEFAULT 3);\n",
		seed:  "INSERT INTO t VALUES (1, 5), (2, NULL);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT 3, z int, CONSTRAINT ck CHECK (z > 0));\n",
		query: "SELECT id || ':' || a FROM t",
		rows:  []string{"1:5", "2:3"},
		back:  []string{"1:5", "2:3"},
	},
	{
		name: "a backfill of an enum and a check over a new column", engines: []string{"postgres"},
		from:  "CREATE TYPE mood AS ENUM ('a', 'b');\nCREATE TABLE t (id int PRIMARY KEY, m mood);\n",
		seed:  "INSERT INTO t VALUES (1, NULL);\n",
		to:    "CREATE TYPE mood AS ENUM ('a', 'b');\nCREATE TABLE t (id int PRIMARY KEY, m mood NOT NULL DEFAULT 'a', z int, CONSTRAINT ck CHECK (z > 0));\n",
		query: "SELECT id || ':' || m FROM t",
		rows:  []string{"1:a"},
		back:  []string{"1:a"},
	},
	{
		name: "a backfill and a check over a generated column", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
		seed:  "INSERT INTO t VALUES (1, NULL);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT 2, b int, g int GENERATED ALWAYS AS (b + 1) STORED, CONSTRAINT ck CHECK (g > 0));\n",
		query: "SELECT id || ':' || a FROM t",
		rows:  []string{"1:2"},
		back:  []string{"1:2"},
	},
	{
		name: "a comment on a check over a new column", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY);\n",
		seed:  "INSERT INTO t VALUES (1);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, z int, CONSTRAINT ck CHECK (z > 0));\nCOMMENT ON CONSTRAINT ck ON t IS 'why';\n",
		query: "SELECT id::text FROM t",
		rows:  []string{"1"},
		back:  []string{"1"},
	},
	{
		name: "a dotted table and the table its name reads as", engines: []string{"postgres-server"},
		from: "CREATE SCHEMA a;\nCREATE TABLE \"a.b\" (id int PRIMARY KEY, v int, secret text);\nCREATE TABLE a.b (id int PRIMARY KEY, secret text);\n",
		seed: "INSERT INTO \"a.b\" VALUES (1, 1, 'dotted');\nINSERT INTO a.b VALUES (1, 'schema');\n",
		to:   "CREATE SCHEMA a;\nCREATE TABLE \"a.b\" (id int PRIMARY KEY, v int, secret text, CONSTRAINT ck CHECK (v > 0));\nCREATE TABLE a.b (id int PRIMARY KEY);\n",
		query: "SELECT 'dotted:' || secret FROM \"a.b\" UNION ALL " +
			"SELECT 'schema:' || count(*) FROM information_schema.columns WHERE table_schema = 'a' AND table_name = 'b' AND column_name = 'secret'",
		rows: []string{"dotted:dotted", "schema:0"},
		back: []string{"dotted:dotted", "schema:1"},
	},
	{
		name: "a table with a quote in its name and the table it reads as without", engines: []string{"postgres"},
		from: "CREATE TABLE \"a\"\"b\" (id int PRIMARY KEY, v int, secret text);\nCREATE TABLE ab (id int PRIMARY KEY, secret text);\n",
		seed: "INSERT INTO \"a\"\"b\" VALUES (1, 1, 'quoted');\nINSERT INTO ab VALUES (1, 'plain');\n",
		to:   "CREATE TABLE \"a\"\"b\" (id int PRIMARY KEY, v int, secret text, CONSTRAINT ck CHECK (v > 0));\nCREATE TABLE ab (id int PRIMARY KEY);\n",
		query: "SELECT 'quoted:' || secret FROM \"a\"\"b\" UNION ALL " +
			"SELECT 'plain:' || count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'ab' AND column_name = 'secret'",
		rows: []string{"plain:0", "quoted:quoted"},
		back: []string{"plain:1", "quoted:quoted"},
	},
	{
		name: "a table with a backtick in its name and the table it reads as without", engines: []string{"mysql", "mariadb"},
		from: "CREATE TABLE `a``b` (id int PRIMARY KEY, v int, secret text);\nCREATE TABLE ab (id int PRIMARY KEY, secret text);\n",
		seed: "INSERT INTO `a``b` VALUES (1, 1, 'quoted');\nINSERT INTO ab VALUES (1, 'plain');\n",
		to:   "CREATE TABLE `a``b` (id int PRIMARY KEY, v int, secret text, CONSTRAINT ck CHECK (v > 0));\nCREATE TABLE ab (id int PRIMARY KEY);\n",
		query: "SELECT CONCAT('quoted:', secret) FROM `a``b` UNION ALL " +
			"SELECT CONCAT('plain:', count(*)) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'ab' AND column_name = 'secret'",
		rows: []string{"plain:0", "quoted:quoted"},
		back: []string{"plain:1", "quoted:quoted"},
	},
	{
		name: "a check over a column whose name holds a space", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY);\n",
		seed:  "INSERT INTO t VALUES (1);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, \"my col\" int, CONSTRAINT ck CHECK (\"my col\" > 0));\n",
		query: "SELECT id::text FROM t",
		rows:  []string{"1"},
		back:  []string{"1"},
	},
	{
		name: "a check over a column whose name holds a hyphen", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY);\n",
		seed:  "INSERT INTO t VALUES (1);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, \"my-col\" int, CONSTRAINT ck CHECK (\"my-col\" > 0));\n",
		query: "SELECT id::text FROM t",
		rows:  []string{"1"},
		back:  []string{"1"},
	},
	{
		name: "a check named with capitals and a space over such a column", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY);\n",
		seed:  "INSERT INTO t VALUES (1);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, \"Upper Col\" int, CONSTRAINT \"My Check\" CHECK (\"Upper Col\" > 0));\n",
		query: "SELECT id::text FROM t",
		rows:  []string{"1"},
		back:  []string{"1"},
	},
	{
		name: "a check whose escape string holds an apostrophe ahead of a quoted column", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY);\n",
		seed:  "INSERT INTO t VALUES (1);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, \"my col\" text, CONSTRAINT ck CHECK (E'it\\'s' <> \"my col\"));\n",
		query: "SELECT id::text FROM t",
		rows:  []string{"1"},
		back:  []string{"1"},
	},
	{
		name: "a check whose dollar quote holds an apostrophe ahead of a quoted column, with a backfill", engines: []string{"postgres"},
		from:  "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
		seed:  "INSERT INTO t VALUES (1, NULL);\n",
		to:    "CREATE TABLE t (id int PRIMARY KEY, a int NOT NULL DEFAULT 1, \"my col\" text, CONSTRAINT ck CHECK ($$it's$$ <> \"my col\"));\n",
		query: "SELECT id || ':' || a FROM t",
		rows:  []string{"1:1"},
		back:  []string{"1:1"},
	},
	{
		name: "a foreign key and the unique index under it, dropped", engines: []string{"postgres", "mysql", "mariadb"},
		from:  "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT c_p FOREIGN KEY (p_id) REFERENCES p (id));\nCREATE UNIQUE INDEX c_u ON c (p_id);\n",
		seed:  "INSERT INTO p VALUES (1);\nINSERT INTO c VALUES (1, 1);\n",
		to:    "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int);\n",
		query: "SELECT CONCAT(id, ':', p_id) FROM c",
		rows:  []string{"1:1"},
		back:  []string{"1:1"},
	},
	{
		name: "a default calling a routine that reads a column the rollback adds", engines: []string{"postgres"},
		from: "CREATE TABLE a (id int PRIMARY KEY, x int);\n" +
			"CREATE FUNCTION next_code() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT count(x) FROM a); END $$;\n" +
			"CREATE TABLE z (id int PRIMARY KEY, d int DEFAULT next_code());\n",
		seed:  "INSERT INTO a VALUES (1, 7);\nINSERT INTO z VALUES (1, 1);\n",
		to:    "CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE z (id int PRIMARY KEY);\n",
		query: "SELECT id::text FROM z",
		rows:  []string{"1"},
		back:  []string{"1"},
	},
	{
		name: "the name of a primary key taken from an index", engines: []string{"postgres"},
		from:  "CREATE TABLE a (id int PRIMARY KEY, x int);\nCREATE INDEX z_pkey ON a (x);\nCREATE TABLE b (id int, y int);\n",
		seed:  "INSERT INTO a VALUES (1, 5);\n",
		to:    "CREATE TABLE a (id int PRIMARY KEY, x int);\nCREATE TABLE z (id int PRIMARY KEY);\nCREATE TABLE b (id int PRIMARY KEY, y int);\n",
		query: "SELECT id || ':' || x FROM a",
		rows:  []string{"1:5"},
		back:  []string{"1:5"},
	},
}

// catalogRowCasesOf is the rows that run on engine.
func catalogRowCasesOf(engine string) []int {
	var rows []int
	for at, test := range catalogRowCases {
		if slices.Contains(test.engines, engine) {
			rows = append(rows, at)
		}
	}
	return rows
}

// TestCompatMigrateDiffCatalogShapeE2E_EveryPlanKeepsItsRows runs each plan over rows, both
// ways, and reads them back. A plan that needs a backfill or a comment beside its ALTER TABLE
// cannot be one statement, so what is free inside one statement stops being free here. The
// next plan has to find nothing left to do in either direction.
func TestCompatMigrateDiffCatalogShapeE2E_EveryPlanKeepsItsRows(t *testing.T) {
	for _, engine := range catalogOrderEngines {
		for _, at := range catalogRowCasesOf(engine.name) {
			test := catalogRowCases[at]
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dev := engine.target(c)
				pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{test.from, test.to}))
				target := engine.target(c)
				c.Assert(pairs, qt.HasLen, 2)

				applyEngineFile(c, target, engine.dialect, pairs[0].Up)
				applyEngineFile(c, target, engine.dialect, test.seed)
				applyEngineFile(c, target, engine.dialect, pairs[1].Up)

				c.Assert(catalogReadBack(c, target, []string{test.query}), qt.DeepEquals, [][]string{test.rows})
				c.Assert(nextPlanOf(c, target, dev, test.to), qt.Contains, "synced")

				applyEngineFile(c, target, engine.dialect, pairs[1].Down)

				c.Assert(catalogReadBack(c, target, []string{test.query}), qt.DeepEquals, [][]string{test.back})
				c.Assert(nextPlanOf(c, target, dev, test.from), qt.Contains, "synced")
			})
		}
	}
}

// TestCompatMigrateDiffCatalogShapeE2E_ALiteralStaysWhole writes two literals into one
// statement, where an escape in the first turns a reader that does not know it inside out, so
// that it finds a statement's end in the second. The rows it writes are the check. No next
// plan is read here: a server reports a literal holding an escape in another spelling than the
// schema file's, and the plan after it compares those two on every build.
func TestCompatMigrateDiffCatalogShapeE2E_ALiteralStaysWhole(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		to     string
		query  string
		rows   string
	}{
		{
			name: "a backslash escape on MySQL", engine: "mysql",
			to:    "CREATE TABLE t (id int PRIMARY KEY, a varchar(20) DEFAULT 'it\\'s', c varchar(20) DEFAULT 'y;z');\n",
			query: "SELECT CONCAT(a, '|', c) FROM t",
			rows:  "it's|y;z",
		},
		{
			name: "a backslash escape on MariaDB", engine: "mariadb",
			to:    "CREATE TABLE t (id int PRIMARY KEY, a varchar(20) DEFAULT 'it\\'s', c varchar(20) DEFAULT 'y;z');\n",
			query: "SELECT CONCAT(a, '|', c) FROM t",
			rows:  "it's|y;z",
		},
		{
			name: "one literal holding an escape and a semicolon", engine: "mysql",
			to:    "CREATE TABLE t (id int PRIMARY KEY, a varchar(20) DEFAULT 'it\\';s');\n",
			query: "SELECT a FROM t",
			rows:  "it';s",
		},
		{
			name: "an escape string on PostgreSQL", engine: "postgres",
			to:    "CREATE TABLE t (id int PRIMARY KEY, a text DEFAULT 'p', c text DEFAULT 'y;z', CONSTRAINT ck CHECK (a <> E'it\\'s'));\n",
			query: "SELECT a || '|' || c FROM t",
			rows:  "p|y;z",
		},
		{
			name: "a dollar quote on PostgreSQL", engine: "postgres",
			to:    "CREATE TABLE t (id int PRIMARY KEY, a text DEFAULT 'p', c text DEFAULT 'y;', CONSTRAINT ck CHECK (a <> $$it's$$));\n",
			query: "SELECT a || '|' || c FROM t",
			rows:  "p|y;",
		},
	}
	engines := make(map[string]catalogOrderEngine, len(catalogOrderEngines))
	for _, engine := range catalogOrderEngines {
		engines[engine.name] = engine
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			engine := engines[test.engine]
			pairs := readMigrationPairs(c, diffEngineSteps(c, "", engine.target(c), []string{"CREATE TABLE t (id int PRIMARY KEY);\n", test.to}))
			target := engine.target(c)
			c.Assert(pairs, qt.HasLen, 2)

			applyEngineFile(c, target, engine.dialect, pairs[0].Up)
			applyEngineFile(c, target, engine.dialect, "INSERT INTO t VALUES (1);\n")
			applyEngineFile(c, target, engine.dialect, pairs[1].Up)

			c.Assert(catalogReadBack(c, target, []string{test.query}), qt.DeepEquals, [][]string{{test.rows}})

			applyEngineFile(c, target, engine.dialect, pairs[1].Down)

			c.Assert(catalogReadBack(c, target, []string{"SELECT CONCAT(id, '') FROM t"}), qt.DeepEquals, [][]string{{"1"}})
		})
	}
}
