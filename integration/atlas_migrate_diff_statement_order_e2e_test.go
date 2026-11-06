//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// catalogOrderEngine is an engine a plan runs on, with what reads a database back.
type catalogOrderEngine struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	reads   []string
}

// catalogOrderEngines are the engines the statement order of a plan can go wrong on.
// postgres-server names no schema, so a plan spans the schemas it declares. What a
// MySQL family read leaves out is a plain index, since a key that goes leaves one
// behind on either order; the next plan answers for those.
var catalogOrderEngines = []catalogOrderEngine{
	{
		name: "postgres", dialect: platform.Postgres, reads: postgresCatalogReads,
		target: func(c *qt.C) string { return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_order"), "public") },
	},
	{
		name: "postgres-server", dialect: platform.Postgres, reads: postgresCatalogReads,
		target: func(c *qt.C) string { return postgresScratchDevURL(c, "ptah_order_server") },
	},
	{
		name: "mysql", dialect: platform.MySQL, reads: mysqlFamilyCatalogReads,
		target: func(c *qt.C) string { return catalogRunTarget(c, "mysql") },
	},
	{
		name: "mariadb", dialect: platform.MariaDB, reads: mysqlFamilyCatalogReads,
		target: func(c *qt.C) string { return catalogRunTarget(c, "mariadb") },
	},
}

var postgresCatalogReads = []string{
	`SELECT table_schema || '.' || table_name || '.' || column_name || ':' || data_type || ':' || is_nullable
FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY 1`,
	`SELECT conrelid::regclass::text || ':' || conname || ':' || pg_get_constraintdef(oid)
FROM pg_constraint WHERE connamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace) AND contype <> 'n' ORDER BY 1`,
	`SELECT indexdef FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY 1`,
}

var mysqlFamilyCatalogReads = []string{
	`SELECT CONCAT(table_name, '.', column_name, ':', column_type, ':', is_nullable)
FROM information_schema.columns WHERE table_schema = DATABASE() ORDER BY 1`,
	`SELECT CONCAT(table_name, ':', constraint_name, ':', constraint_type)
FROM information_schema.table_constraints WHERE table_schema = DATABASE() ORDER BY 1`,
	`SELECT CONCAT(table_name, ':', constraint_name, ':', column_name, ':', COALESCE(referenced_table_name, ''), ':', COALESCE(referenced_column_name, ''))
FROM information_schema.key_column_usage WHERE table_schema = DATABASE() ORDER BY 1`,
}

// catalogOrderCases are plans where the community binary's order by table name would put
// a statement ahead of what it needs, on the engines each one runs on both ways. Some
// hold a table the binary's order leaves alone, as a control. An engine a row leaves
// out refuses the plan's order on any binary this repository has built, for a reason
// that lies outside the order this pass decides.
var catalogOrderCases = []struct {
	name    string
	engines []string
	from    string
	to      string
}{
	// A foreign key and what it needs from another table.
	{
		name: "fk to new unique later table", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY);
CREATE TABLE z (id int PRIMARY KEY);
`,
		to: `CREATE TABLE z (id int PRIMARY KEY, code int, CONSTRAINT z_code_key UNIQUE (code));
CREATE TABLE a (id int PRIMARY KEY, z_code int, CONSTRAINT a_z_fk FOREIGN KEY (z_code) REFERENCES z (code));
`,
	},
	{
		name: "fk to new unique earlier table", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY);
CREATE TABLE z (id int PRIMARY KEY);
`,
		to: `CREATE TABLE a (id int PRIMARY KEY, code int, CONSTRAINT a_code_key UNIQUE (code));
CREATE TABLE z (id int PRIMARY KEY, a_code int, CONSTRAINT z_a_fk FOREIGN KEY (a_code) REFERENCES a (code));
`,
	},
	{
		name: "fk to new unique index later table", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY, z_code int);
CREATE TABLE z (id int PRIMARY KEY, code int);
`,
		to: `CREATE TABLE z (id int PRIMARY KEY, code int);
CREATE UNIQUE INDEX z_code_uq ON z (code);
CREATE TABLE a (id int PRIMARY KEY, z_code int, CONSTRAINT a_z_fk FOREIGN KEY (z_code) REFERENCES z (code));
`,
	},
	{
		name: "pk moves under fk child sorts first", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE z (id int NOT NULL, k int NOT NULL, PRIMARY KEY (id));
CREATE TABLE a (id int PRIMARY KEY, z_id int, CONSTRAINT a_z_fk FOREIGN KEY (z_id) REFERENCES z (id));
`,
		to: `CREATE TABLE z (id int NOT NULL, k int NOT NULL, PRIMARY KEY (k));
CREATE TABLE a (id int PRIMARY KEY, z_id int, CONSTRAINT a_z_fk FOREIGN KEY (z_id) REFERENCES z (k));
`,
	},
	{
		name: "fk repointed to new unique later table", engines: []string{"postgres"},
		from: `CREATE TABLE z (id int PRIMARY KEY);
CREATE TABLE a (id int PRIMARY KEY, z_ref int, CONSTRAINT a_z_fk FOREIGN KEY (z_ref) REFERENCES z (id));
`,
		to: `CREATE TABLE z (id int PRIMARY KEY, code int, CONSTRAINT z_code_key UNIQUE (code));
CREATE TABLE a (id int PRIMARY KEY, z_ref int, CONSTRAINT a_z_fk FOREIGN KEY (z_ref) REFERENCES z (code));
`,
	},
	{
		name: "type change both ends of fk child first", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE z (id int PRIMARY KEY);
CREATE TABLE a (id int PRIMARY KEY, z_id int, CONSTRAINT a_z_fk FOREIGN KEY (z_id) REFERENCES z (id));
`,
		to: `CREATE TABLE z (id bigint PRIMARY KEY);
CREATE TABLE a (id int PRIMARY KEY, z_id bigint, CONSTRAINT a_z_fk FOREIGN KEY (z_id) REFERENCES z (id));
`,
	},
	{
		name: "type change both ends of fk parent first", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY);
CREATE TABLE z (id int PRIMARY KEY, a_id int, CONSTRAINT z_a_fk FOREIGN KEY (a_id) REFERENCES a (id));
`,
		to: `CREATE TABLE a (id bigint PRIMARY KEY);
CREATE TABLE z (id int PRIMARY KEY, a_id bigint, CONSTRAINT z_a_fk FOREIGN KEY (a_id) REFERENCES a (id));
`,
	},
	{
		name: "new table fk to changed table new unique", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE z (id int PRIMARY KEY);
`,
		to: `CREATE TABLE z (id int PRIMARY KEY, code int, CONSTRAINT z_code_key UNIQUE (code));
CREATE TABLE a_new (id int PRIMARY KEY, z_code int, CONSTRAINT a_new_fk FOREIGN KEY (z_code) REFERENCES z (code));
`,
	},
	// A foreign key of a table to itself.
	{
		name: "self fk to new unique index", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int);
`,
		to: `CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int);
CREATE UNIQUE INDEX t_code_uq ON t (code);
ALTER TABLE t ADD CONSTRAINT t_parent_fk FOREIGN KEY (parent_code) REFERENCES t (code);
`,
	},
	{
		name: "self fk to new unique constraint", engines: []string{"postgres"},
		from: `CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int);
`,
		to: `CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int, CONSTRAINT t_code_key UNIQUE (code), CONSTRAINT t_parent_fk FOREIGN KEY (parent_code) REFERENCES t (code));
`,
	},
	{
		name: "fk on a new unique column same table new col", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE t (id int PRIMARY KEY);
`,
		to: `CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int, CONSTRAINT t_code_key UNIQUE (code), CONSTRAINT t_parent_fk FOREIGN KEY (parent_code) REFERENCES t (code));
`,
	},
	{
		name: "self fk column dropped", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE t (id int PRIMARY KEY, parent_id int, CONSTRAINT t_parent FOREIGN KEY (parent_id) REFERENCES t (id));
`,
		to: `CREATE TABLE t (id int PRIMARY KEY);
`,
	},
	{
		name: "self referencing ids widened", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE t (id int PRIMARY KEY, parent_id int, CONSTRAINT t_parent FOREIGN KEY (parent_id) REFERENCES t (id));
`,
		to: `CREATE TABLE t (id bigint PRIMARY KEY, parent_id bigint, CONSTRAINT t_parent FOREIGN KEY (parent_id) REFERENCES t (id));
`,
	},
	// A column a foreign key is on, dropped.
	{
		name: "drop fk column", engines: []string{"postgres"},
		from: `CREATE TABLE owners (id int PRIMARY KEY);
CREATE TABLE items (id int PRIMARY KEY, owner_id int, name text, CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id));
`,
		to: `CREATE TABLE owners (id int PRIMARY KEY);
CREATE TABLE items (id int PRIMARY KEY, name text);
`,
	},
	{
		name: "drop fk column", engines: []string{"mysql", "mariadb"},
		from: `CREATE TABLE owners (id int PRIMARY KEY);
CREATE TABLE items (id int PRIMARY KEY, owner_id int, name varchar(20), CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id));
`,
		to: `CREATE TABLE owners (id int PRIMARY KEY);
CREATE TABLE items (id int PRIMARY KEY, name varchar(20));
`,
	},
	{
		name: "two fk columns dropped one table", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE q (id int PRIMARY KEY);
CREATE TABLE t (id int PRIMARY KEY, p_id int, q_id int, x int, CONSTRAINT t_p FOREIGN KEY (p_id) REFERENCES p (id), CONSTRAINT t_q FOREIGN KEY (q_id) REFERENCES q (id));
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE q (id int PRIMARY KEY);
CREATE TABLE t (id int PRIMARY KEY, x int);
`,
	},
	{
		name: "multi column fk one column dropped", engines: []string{"postgres"},
		from: `CREATE TABLE p (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE c (id int PRIMARY KEY, pa int, pb int, x int, CONSTRAINT c_fk FOREIGN KEY (pa, pb) REFERENCES p (a, b));
`,
		to: `CREATE TABLE p (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE c (id int PRIMARY KEY, pb int, x int);
`,
	},
	{
		name: "fk column dropped and other table changed", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE owners (id int PRIMARY KEY);
CREATE TABLE items (id int PRIMARY KEY, owner_id int, CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id));
`,
		to: `CREATE TABLE owners (id int PRIMARY KEY, x int);
CREATE TABLE items (id int PRIMARY KEY);
`,
	},
	// An index a foreign key stands on.
	{
		name: "drop fk and its index", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_p_idx ON c (p_id);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int);
`,
	},
	{
		name: "swap index backing fk", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, x int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_i1 ON c (p_id);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, x int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_i2 ON c (p_id, x);
`,
	},
	{
		name: "drop fk and index and other table changed", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_p_idx ON c (p_id);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY, x int);
CREATE TABLE c (id int PRIMARY KEY, p_id int);
`,
	},
	// One foreign key name dropped and added.
	{
		name: "fk action change same name", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id));
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id) ON DELETE CASCADE);
`,
	},
	{
		name: "fk action change and other table changed", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id));
`,
		to: `CREATE TABLE p (id int PRIMARY KEY, x int);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id) ON DELETE CASCADE);
`,
	},
	// A table only an index statement touches.
	{
		name: "drop fk on z and unique index on a", engines: []string{"postgres"},
		from: `CREATE TABLE a (id int PRIMARY KEY, code int);
CREATE UNIQUE INDEX a_code_uq ON a (code);
CREATE TABLE z (id int PRIMARY KEY, a_code int, CONSTRAINT z_a_fk FOREIGN KEY (a_code) REFERENCES a (code));
`,
		to: `CREATE TABLE a (id int PRIMARY KEY, code int);
CREATE TABLE z (id int PRIMARY KEY, a_code int);
`,
	},
	{
		name: "swap parent unique index", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY, code int);
CREATE UNIQUE INDEX i1 ON p (code);
CREATE TABLE c (id int PRIMARY KEY, code int, CONSTRAINT c_fk FOREIGN KEY (code) REFERENCES p (code));
`,
		to: `CREATE TABLE p (id int PRIMARY KEY, code int);
CREATE UNIQUE INDEX i2 ON p (code);
CREATE TABLE c (id int PRIMARY KEY, code int, CONSTRAINT c_fk FOREIGN KEY (code) REFERENCES p (code));
`,
	},
	// An index name that moves between tables.
	{
		name: "index name moves to earlier table", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY, x int);
CREATE TABLE z (id int PRIMARY KEY, x int);
CREATE INDEX idx_x ON z (x);
`,
		to: `CREATE TABLE a (id int PRIMARY KEY, x int);
CREATE TABLE z (id int PRIMARY KEY, x int);
CREATE INDEX idx_x ON a (x);
`,
	},
	{
		name: "index name reused on changed table", engines: []string{"mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY, x int);
CREATE TABLE old_t (id int PRIMARY KEY, x int);
CREATE INDEX idx_x ON old_t (x);
`,
		to: `CREATE TABLE a (id int PRIMARY KEY, x int);
CREATE INDEX idx_x ON a (x);
`,
	},
	// Rollbacks the forward file read backwards would break.
	{
		name: "drop two tables parent sorts first", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE keep (id int PRIMARY KEY);
CREATE TABLE a (id int PRIMARY KEY);
CREATE TABLE b (id int PRIMARY KEY, a_id int, CONSTRAINT b_a_fk FOREIGN KEY (a_id) REFERENCES a (id));
`,
		to: `CREATE TABLE keep (id int PRIMARY KEY);
`,
	},
	{
		name: "drop two tables child sorts first", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE keep (id int PRIMARY KEY);
CREATE TABLE b (id int PRIMARY KEY);
CREATE TABLE a (id int PRIMARY KEY, b_id int, CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES b (id));
`,
		to: `CREATE TABLE keep (id int PRIMARY KEY);
`,
	},
	{
		name: "new child table fk plus explicit index", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_p_idx ON c (p_id);
`,
	},
	{
		name: "new table chain with indexes", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE keep (id int PRIMARY KEY);
`,
		to: `CREATE TABLE keep (id int PRIMARY KEY);
CREATE TABLE c3 (id int PRIMARY KEY, code int UNIQUE);
CREATE TABLE b2 (id int PRIMARY KEY, c_code int, CONSTRAINT b_c FOREIGN KEY (c_code) REFERENCES c3 (code));
CREATE INDEX b2_c ON b2 (c_code);
CREATE TABLE a1 (id int PRIMARY KEY, b_id int, self_id int, keep_id int, CONSTRAINT a_b FOREIGN KEY (b_id) REFERENCES b2 (id), CONSTRAINT a_self FOREIGN KEY (self_id) REFERENCES a1 (id), CONSTRAINT a_keep FOREIGN KEY (keep_id) REFERENCES keep (id));
`,
	},
	{
		name: "fk and explicit index on new column", engines: []string{"postgres", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_p_idx ON c (p_id);
`,
	},
	{
		name: "new column fk plus index other table changed", engines: []string{"postgres", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY, x int);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p (id));
CREATE INDEX c_p_idx ON c (p_id);
`,
	},
	{
		name: "drop child table and parent column", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY, code int, CONSTRAINT p_code_key UNIQUE (code));
CREATE TABLE child (id int PRIMARY KEY, code int, CONSTRAINT child_fk FOREIGN KEY (code) REFERENCES p (code));
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
`,
	},
	{
		name: "drop child table and parent index", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY, code int);
CREATE UNIQUE INDEX p_code_uq ON p (code);
CREATE TABLE child (id int PRIMARY KEY, code int, CONSTRAINT child_fk FOREIGN KEY (code) REFERENCES p (code));
`,
		to: `CREATE TABLE p (id int PRIMARY KEY, code int);
`,
	},
	{
		name: "rollback fk drop and other change on child", engines: []string{"postgres"},
		from: `CREATE TABLE a (id int PRIMARY KEY, code int, CONSTRAINT a_code_key UNIQUE (code));
CREATE TABLE z (id int PRIMARY KEY, a_code int, CONSTRAINT z_a_fk FOREIGN KEY (a_code) REFERENCES a (code));
`,
		to: `CREATE TABLE a (id int PRIMARY KEY);
CREATE TABLE z (id int PRIMARY KEY, a_code int, x int);
`,
	},
	{
		name: "rollback cycle with new table", engines: []string{"postgres"},
		from: `CREATE TABLE a (id int PRIMARY KEY, code int);
`,
		to: `CREATE TABLE a (id int PRIMARY KEY, code int, n_id int, CONSTRAINT a_code_key UNIQUE (code));
CREATE TABLE n (id int PRIMARY KEY, a_code int, CONSTRAINT n_a_fk FOREIGN KEY (a_code) REFERENCES a (code));
ALTER TABLE a ADD CONSTRAINT a_n_fk FOREIGN KEY (n_id) REFERENCES n (id);
`,
	},
	{
		name: "changed to new and new to changed new column", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE a (id int PRIMARY KEY);
`,
		to: `CREATE TABLE a (id int PRIMARY KEY, code int, n_id int, CONSTRAINT a_code_key UNIQUE (code));
CREATE TABLE n (id int PRIMARY KEY, a_code int, CONSTRAINT n_a_fk FOREIGN KEY (a_code) REFERENCES a (code));
ALTER TABLE a ADD CONSTRAINT a_n_fk FOREIGN KEY (n_id) REFERENCES n (id);
`,
	},
	{
		name: "check on new column", engines: []string{"postgres", "mysql", "mariadb"},
		from: `CREATE TABLE t (id int PRIMARY KEY);
`,
		to: `CREATE TABLE t (id int PRIMARY KEY, n int, CONSTRAINT ck CHECK (n > 0));
`,
	},
	{
		name: "one table every kind of change", engines: []string{"postgres", "mariadb"},
		from: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE t (id int PRIMARY KEY, a int, b int, c int, d int, CONSTRAINT ck_a CHECK (a > 0), CONSTRAINT uq_b UNIQUE (b));
CREATE INDEX t_c_idx ON t (c);
`,
		to: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE t (id int PRIMARY KEY, a bigint, c int, d int DEFAULT 4, e int, f int, g int, CONSTRAINT ck_e CHECK (e > 0), CONSTRAINT uq_f UNIQUE (f), CONSTRAINT t_g_fk FOREIGN KEY (g) REFERENCES p (id));
CREATE INDEX t_c_idx2 ON t (c, e);
`,
	},
	{
		name: "mixed case table gains pk", engines: []string{"postgres"},
		from: `CREATE TABLE "Tbl" (id int, x int);
`,
		to: `CREATE TABLE "Tbl" (id int PRIMARY KEY, x int);
`,
	},
	{
		name: "mixed case table gains pk", engines: []string{"mysql", "mariadb"},
		from: "CREATE TABLE `Tbl` (id int, x int);\n",
		to:   "CREATE TABLE `Tbl` (id int PRIMARY KEY, x int);\n",
	},
	{
		name: "pk moves to new column", engines: []string{"postgres"},
		from: `CREATE TABLE t (id int PRIMARY KEY, x int);
`,
		to: `CREATE TABLE t (id int, x int, k int NOT NULL, PRIMARY KEY (k));
`,
	},
	// A column typed by a table the plan creates.
	{
		name: "column of new table rowtype", engines: []string{"postgres"},
		from: `CREATE TABLE a (id int PRIMARY KEY);
`,
		to: `CREATE TABLE n (id int, label text);
CREATE TABLE a (id int PRIMARY KEY, payload n);
`,
	},
	// Two tables one name stands for.
	{
		name: "case variant tables", engines: []string{"postgres"},
		from: `CREATE TABLE "Users" (id int PRIMARY KEY);
CREATE TABLE users (id int PRIMARY KEY);
`,
		to: `CREATE TABLE "Users" (id int PRIMARY KEY, upper_col int);
CREATE TABLE users (id int PRIMARY KEY, lower_col int);
`,
	},
	{
		name: "case variant tables", engines: []string{"mysql", "mariadb"},
		from: "CREATE TABLE `Users` (id int PRIMARY KEY);\n" +
			"CREATE TABLE users (id int PRIMARY KEY);\n",
		to: "CREATE TABLE `Users` (id int PRIMARY KEY, upper_col int);\n" +
			"CREATE TABLE users (id int PRIMARY KEY, lower_col int);\n",
	},
	{
		name: "two schemas same table changed", engines: []string{"postgres-server"},
		from: `CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.t (id int PRIMARY KEY);
CREATE TABLE b.t (id int PRIMARY KEY);
`,
		to: `CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.t (id int PRIMARY KEY, in_a int);
CREATE TABLE b.t (id int PRIMARY KEY, in_b int);
`,
	},
	{
		name: "two schemas same table new", engines: []string{"postgres-server"},
		from: `CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.keep (id int PRIMARY KEY);
CREATE TABLE a.t (id int PRIMARY KEY, in_a int);
CREATE TABLE b.t (id int PRIMARY KEY, in_b int);
`,
		to: `CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.keep (id int PRIMARY KEY, x int);
CREATE TABLE a.t (id int PRIMARY KEY, in_a int);
CREATE TABLE b.t (id int PRIMARY KEY, in_b int);
`,
	},
}

// catalogOrderCasesOf is the rows that run on engine.
func catalogOrderCasesOf(engine string) []int {
	var rows []int
	for at, test := range catalogOrderCases {
		if slices.Contains(test.engines, engine) {
			rows = append(rows, at)
		}
	}
	return rows
}

// catalogReadBack is what target holds, read with each of reads, in a stable order.
func catalogReadBack(c *qt.C, target string, reads []string) [][]string {
	c.Helper()
	conn := connectDevDialect(c, target)
	held := make([][]string, 0, len(reads))
	for _, read := range reads {
		rows, err := conn.QueryContext(c.Context(), read)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", read))
		lines := make([]string, 0, 8)
		for rows.Next() {
			var line string
			c.Assert(rows.Scan(&line), qt.IsNil)
			lines = append(lines, line)
		}
		c.Assert(rows.Err(), qt.IsNil)
		c.Assert(rows.Close(), qt.IsNil)
		// The server's order ties names that differ in case under its collation.
		slices.Sort(lines)
		held = append(held, lines)
	}
	return held
}

// nextPlanOf is what the next plan from target to schema says.
func nextPlanOf(c *qt.C, target, dev, schema string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	output, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(path), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	return output
}

// TestCompatMigrateDiffCatalogShapeE2E_EveryPlanRunsBothWays writes each plan, runs the
// forward file and the rollback on the engine and reads the database back after each.
// The forward file has to leave what the same schema applied directly leaves, and the
// rollback what the database held, so the expected value comes from the server. The next
// plan has to find nothing left to do in either direction.
func TestCompatMigrateDiffCatalogShapeE2E_EveryPlanRunsBothWays(t *testing.T) {
	for _, engine := range catalogOrderEngines {
		for _, at := range catalogOrderCasesOf(engine.name) {
			test := catalogOrderCases[at]
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dev := engine.target(c)
				pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{test.from, test.to}))
				target, declared := engine.target(c), engine.target(c)
				c.Assert(pairs, qt.HasLen, 2)

				applyEngineFile(c, declared, engine.dialect, test.to)
				applyEngineFile(c, target, engine.dialect, pairs[0].Up)
				before := catalogReadBack(c, target, engine.reads)
				applyEngineFile(c, target, engine.dialect, pairs[1].Up)

				c.Assert(catalogReadBack(c, target, engine.reads), qt.DeepEquals, catalogReadBack(c, declared, engine.reads))
				c.Assert(nextPlanOf(c, target, dev, test.to), qt.Contains, "synced")

				applyEngineFile(c, target, engine.dialect, pairs[1].Down)

				c.Assert(catalogReadBack(c, target, engine.reads), qt.DeepEquals, before)
				c.Assert(nextPlanOf(c, target, dev, test.from), qt.Contains, "synced")
			})
		}
	}
}

// TestCompatMigrateDiffCatalogShapeE2E_ARollbackAddsColumnsInOrder reads a table's columns
// back in position. A rollback's columns go back in the order the table held them, since
// that is what `SELECT *` shows, whatever order the forward file dropped them in.
func TestCompatMigrateDiffCatalogShapeE2E_ARollbackAddsColumnsInOrder(t *testing.T) {
	const (
		from = "CREATE TABLE t (id int PRIMARY KEY, a int, b int, c int);\n"
		to   = "CREATE TABLE t (id int PRIMARY KEY, c int);\n"
	)
	tests := []struct {
		engine  string
		columns string
	}{
		{engine: "postgres", columns: "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't'"},
		{engine: "mysql", columns: "SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 't'"},
		{engine: "mariadb", columns: "SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 't'"},
	}
	for _, test := range tests {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			dialect := catalogOrderDialect(test.engine)
			target := catalogRunTarget(c, test.engine)
			pairs := readMigrationPairs(c, diffEngineSteps(c, "", catalogRunTarget(c, test.engine), []string{from, to}))
			c.Assert(pairs, qt.HasLen, 2)

			applyEngineFile(c, target, dialect, pairs[0].Up)
			applyEngineFile(c, target, dialect, pairs[1].Up)
			applyEngineFile(c, target, dialect, pairs[1].Down)

			c.Assert(heldColumns(c, target, test.columns), qt.Equals, "id,c,a,b")
		})
	}
}

// catalogOrderDialect is the dialect of an engine by its name.
func catalogOrderDialect(name string) string {
	for _, engine := range catalogOrderEngines {
		if engine.name == name {
			return engine.dialect
		}
	}
	return ""
}

// TestCompatSchemaApplyE2E_ChangesEachTableOfOneName is two tables of one name in two
// schemas, with rows. Grouped under their bare name, the changes for b.t land on a.t, a
// column's data is lost and the apply reports success.
func TestCompatSchemaApplyE2E_ChangesEachTableOfOneName(t *testing.T) {
	c := qt.New(t)
	const (
		from = `CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.t (id int PRIMARY KEY, secret text);
CREATE TABLE b.t (id int PRIMARY KEY, secret text);
`
		rows = `INSERT INTO a.t VALUES (1, 'a-keeps-this');
INSERT INTO b.t VALUES (1, 'b-drops-this');
`
		to = `CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.t (id int PRIMARY KEY, secret text, added_to_a int);
CREATE TABLE b.t (id int PRIMARY KEY);
`
	)
	dev := postgresScratchDevURL(c, "ptah_order_rows_dev")
	target := postgresScratchDevURL(c, "ptah_order_rows")
	work := c.TempDir()
	path := filepath.Join(work, "to.sql")
	c.Assert(os.WriteFile(path, []byte(to), 0o600), qt.IsNil)
	applyEngineFile(c, target, platform.Postgres, from+rows)

	output, err := runCompatVerb("schema", "apply", "--url", target, "--to", "file://"+filepath.ToSlash(path), "--dev-url", dev, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(heldColumns(c, target, "SELECT secret || ':' || COALESCE(added_to_a::text, 'null') FROM a.t"), qt.Equals, "a-keeps-this:null")
	c.Assert(heldColumns(c, target, "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'b' AND table_name = 't'"), qt.Equals, "id")
	c.Assert(nextPlanOf(c, target, dev, to), qt.Contains, "synced")
}
