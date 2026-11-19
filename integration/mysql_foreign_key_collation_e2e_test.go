//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// foreignKeyCollationRows are a parent key and the foreign key that
// references it, both ends changing collation together: by a COLLATE each
// declares, by their tables' default collation, which the columns take, and
// by the character set each names. The server refuses a change of either end
// while the key stands, with Error 3780 on MySQL and Error 1832 on MariaDB.
// want is the collation both ends hold afterwards, on each engine.
var foreignKeyCollationRows = []struct {
	name        string
	before      string
	declaration string
	want        map[string]string
}{
	{
		name: "a declared collation",
		before: "CREATE TABLE parent (code varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n" +
			"CREATE TABLE child (id int PRIMARY KEY, code varchar(10) COLLATE utf8mb4_unicode_ci NULL, " +
			"CONSTRAINT child_code_fk FOREIGN KEY (code) REFERENCES parent (code)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n",
		declaration: "CREATE TABLE parent (code varchar(10) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n" +
			"CREATE TABLE child (id int PRIMARY KEY, code varchar(10) COLLATE utf8mb4_bin NULL, " +
			"CONSTRAINT child_code_fk FOREIGN KEY (code) REFERENCES parent (code)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n",
		want: map[string]string{"MySQL": "utf8mb4_bin", "MariaDB": "utf8mb4_bin"},
	},
	{
		name: "the tables' default collation",
		before: "CREATE TABLE parent (code varchar(10) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n" +
			"CREATE TABLE child (id int PRIMARY KEY, code varchar(10) NULL, " +
			"CONSTRAINT child_code_fk FOREIGN KEY (code) REFERENCES parent (code)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n",
		declaration: "CREATE TABLE parent (code varchar(10) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n" +
			"CREATE TABLE child (id int PRIMARY KEY, code varchar(10) NULL, " +
			"CONSTRAINT child_code_fk FOREIGN KEY (code) REFERENCES parent (code)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n",
		want: map[string]string{"MySQL": "utf8mb4_bin", "MariaDB": "utf8mb4_bin"},
	},
	{
		name: "a character set",
		before: "CREATE TABLE parent (code varchar(10) CHARACTER SET latin1 NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n" +
			"CREATE TABLE child (id int PRIMARY KEY, code varchar(10) CHARACTER SET latin1 NULL, " +
			"CONSTRAINT child_code_fk FOREIGN KEY (code) REFERENCES parent (code)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n",
		declaration: "CREATE TABLE parent (code varchar(10) CHARACTER SET utf8mb4 NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n" +
			"CREATE TABLE child (id int PRIMARY KEY, code varchar(10) CHARACTER SET utf8mb4 NULL, " +
			"CONSTRAINT child_code_fk FOREIGN KEY (code) REFERENCES parent (code)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n",
		want: map[string]string{"MySQL": "utf8mb4_0900_ai_ci", "MariaDB": "utf8mb4_uca1400_ai_ci"},
	},
}

// foreignKeyCollationHeld reads, in database, the collation of code in child
// and in parent, the number of child rows that still find their parent, and
// the foreign keys of child.
func foreignKeyCollationHeld(c *qt.C, scratch mysqlScratch, database string) []string {
	c.Helper()
	var child, parent, joined, keys string
	err := scratch.admin.QueryRowContext(c.Context(), `
		SELECT
			(SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'child' AND COLUMN_NAME = 'code'),
			(SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'parent' AND COLUMN_NAME = 'code'),
			(SELECT COUNT(*) FROM `+"`"+database+"`"+`.child JOIN `+"`"+database+"`"+`.parent USING (code)),
			(SELECT COALESCE(GROUP_CONCAT(CONSTRAINT_NAME), '') FROM information_schema.REFERENTIAL_CONSTRAINTS
			 WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = 'child')`,
		database, database, database).Scan(&child, &parent, &joined, &keys)
	c.Assert(err, qt.IsNil)
	return []string{child, parent, joined, keys}
}

// TestSchemaApplyChangesTheCollationOfAForeignKeyE2E changes the collation of
// both ends of a foreign key. The plan drops the key, changes both ends, and
// adds the key back, so one apply converges: both ends hold the new
// collation, every row still finds its parent, and the key stands. Without the
// release, the first MODIFY COLUMN fails and the apply stops.
func TestSchemaApplyChangesTheCollationOfAForeignKeyE2E(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		for _, row := range foreignKeyCollationRows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				database, url := scratch.builtFrom(c, row.before+
					"INSERT INTO parent VALUES ('a'), ('B');\nINSERT INTO child VALUES (1, 'a'), (2, 'B');\n")
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(row.declaration), 0o600), qt.IsNil)
				args := []string{"schema", "apply", "--db-url", url, "--schema-file", schema}

				applied := runPtahNative(c, append(args, "--auto-approve")...)

				c.Assert(foreignKeyCollationHeld(c, scratch, database), qt.DeepEquals,
					[]string{row.want[engine.name], row.want[engine.name], "2", "child_code_fk"}, qt.Commentf("%s", applied))
				c.Assert(runPtahNative(c, append(args, "--dry-run")...), qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}
