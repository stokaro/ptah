//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// mysqlKeyMoveEngines are the servers the move is measured on. MySQL refuses the move
// while a foreign key stands over the old key, and MariaDB takes it either way.
var mysqlKeyMoveEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

const (
	// mysqlKeyMoveColumns reads the columns of the primary key of parent back.
	mysqlKeyMoveColumns = "SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) FROM information_schema.STATISTICS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'parent' AND INDEX_NAME = 'PRIMARY'"
	// mysqlKeyMoveReferences reads back what the foreign keys of child reference.
	mysqlKeyMoveReferences = "SELECT COALESCE(GROUP_CONCAT(CONCAT(CONSTRAINT_NAME, ':', REFERENCED_COLUMN_NAME)), '') FROM information_schema.KEY_COLUMN_USAGE " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'child' AND REFERENCED_TABLE_NAME IS NOT NULL"
	// mysqlKeyMoveChild is a table with a foreign key over parent.id.
	mysqlKeyMoveChild = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	// mysqlKeyMoveRows are rows the foreign key holds.
	mysqlKeyMoveRows = "INSERT INTO parent VALUES (1, 10);\nINSERT INTO child VALUES (1, 1);\n"
)

// mysqlKeyMoveApply runs `schema apply` of desired over the database at target.
func mysqlKeyMoveApply(c *qt.C, target, desired string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
}

// TestSchemaApplyKeyMovesUnderForeignKeyE2E_HappyPath moves the primary key of a
// table a foreign key references. MySQL 8.4 and 26.10 answered the move with Error
// 1553, `Cannot drop index 'PRIMARY': needed in a foreign key constraint`, where the
// plan added what the foreign key would use, or dropped the foreign key, only after
// the move. Each row reads the key and the foreign key back from the catalog, and a
// second run finds nothing to do.
func TestSchemaApplyKeyMovesUnderForeignKeyE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, wantKey, wantReferences string }{
		{
			name:  "a UNIQUE the plan adds takes the foreign key over",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id));\n" + mysqlKeyMoveChild + mysqlKeyMoveRows,
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code), CONSTRAINT id_key UNIQUE (id));\n" +
				mysqlKeyMoveChild,
			wantKey: "code", wantReferences: "child_fk:id",
		},
		{
			name: "a UNIQUE the table holds takes the foreign key over",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id), CONSTRAINT id_key UNIQUE (id));\n" +
				mysqlKeyMoveChild + mysqlKeyMoveRows,
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code), CONSTRAINT id_key UNIQUE (id));\n" +
				mysqlKeyMoveChild,
			wantKey: "code", wantReferences: "child_fk:id",
		},
		{
			name: "a column's own UNIQUE takes the foreign key over",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE);\n" +
				mysqlKeyMoveChild + mysqlKeyMoveRows,
			desired: "CREATE TABLE parent (id bigint NOT NULL UNIQUE, code bigint PRIMARY KEY);\n" + mysqlKeyMoveChild,
			wantKey: "code", wantReferences: "child_fk:id",
		},
		{
			name: "the foreign key moves to the new key",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id), CONSTRAINT code_key UNIQUE (code));\n" +
				mysqlKeyMoveChild + "INSERT INTO parent VALUES (1, 1);\nINSERT INTO child VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			wantKey: "code", wantReferences: "child_fk:code",
		},
		{
			name: "a foreign key over the UNIQUE the key replaces",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id), CONSTRAINT code_key UNIQUE (code));\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			wantKey: "code", wantReferences: "child_fk:code",
		},
		{
			name: "a table's foreign key over itself",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, up bigint, PRIMARY KEY (id), " +
				"CONSTRAINT parent_up FOREIGN KEY (up) REFERENCES parent (id));\nCREATE TABLE child (id bigint PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, up bigint, PRIMARY KEY (code), CONSTRAINT id_key UNIQUE (id), " +
				"CONSTRAINT parent_up FOREIGN KEY (up) REFERENCES parent (id));\nCREATE TABLE child (id bigint PRIMARY KEY);\n",
			wantKey: "code", wantReferences: "",
		},
	}
	for _, engine := range mysqlKeyMoveEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, test.built)

				run := mysqlKeyMoveApply(c, target, test.desired)

				c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
				c.Assert(columnKeyString(c, target, mysqlKeyMoveColumns), qt.Equals, test.wantKey)
				c.Assert(columnKeyString(c, target, mysqlKeyMoveReferences), qt.Equals, test.wantReferences)
				again := mysqlKeyMoveApply(c, target, test.desired)
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestSchemaApplyKeyMovesUnderForeignKeyE2E_TheForeignKeyLeaves removes the foreign
// key in the plan that moves the key: the foreign key goes first.
func TestSchemaApplyKeyMovesUnderForeignKeyE2E_TheForeignKeyLeaves(t *testing.T) {
	for _, engine := range mysqlKeyMoveEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c,
				"CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id));\n"+mysqlKeyMoveChild+mysqlKeyMoveRows)

			run := mysqlKeyMoveApply(c, target, "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));\n"+
				"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint);\n")

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, mysqlKeyMoveColumns), qt.Equals, "code")
			c.Assert(columnKeyString(c, target, mysqlKeyMoveReferences), qt.Equals, "")
		})
	}
}

// TestSchemaApplyKeyMovesUnderForeignKeyE2E_AKeyThePlanOnlyRemoves removes the key
// and the foreign key over it, on a table that sorts after parent, so the removals
// would otherwise name the key first.
func TestSchemaApplyKeyMovesUnderForeignKeyE2E_AKeyThePlanOnlyRemoves(t *testing.T) {
	for _, engine := range mysqlKeyMoveEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c,
				"CREATE TABLE parent (id bigint NOT NULL, PRIMARY KEY (id));\n"+
					"CREATE TABLE zchild (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (id));\n")

			run := mysqlKeyMoveApply(c, target, "CREATE TABLE parent (id bigint NOT NULL);\nCREATE TABLE zchild (id bigint PRIMARY KEY, pid bigint);\n")

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'parent' AND INDEX_NAME = 'PRIMARY'"), qt.Equals, "0")
		})
	}
}

// TestSchemaApplyKeyMovesUnderForeignKeyE2E_FailurePath keeps a foreign key that
// nothing would serve once the key moves. The plan is refused before the server
// changes, so the key reads back where it was.
func TestSchemaApplyKeyMovesUnderForeignKeyE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name, want string
		engine     dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, want: `mysql requires referenced columns id on table "parent" to be declared unique`},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, want: `mariadb requires referenced columns id on table "parent" to be the full leftmost prefix of an index`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, test.engine).builtFrom(c,
				"CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id));\n"+mysqlKeyMoveChild+mysqlKeyMoveRows)

			run := mysqlKeyMoveApply(c, target, "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));\n"+mysqlKeyMoveChild)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, test.want)
			c.Assert(columnKeyString(c, target, mysqlKeyMoveColumns), qt.Equals, "id")
		})
	}
}
