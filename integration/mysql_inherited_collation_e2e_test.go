//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// inheritedCollationBefore builds the three tables the tests below compare:
// reset, whose column holds utf8mb4_bin in a utf8mb4_unicode_ci table;
// retable, whose default collation is utf8mb4_bin, held by its column too; and
// latin, whose column takes latin1_swedish_ci from its character set.
const inheritedCollationBefore = `
CREATE TABLE reset (id int PRIMARY KEY, a varchar(20) COLLATE utf8mb4_bin) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
CREATE TABLE retable (id int PRIMARY KEY, n varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE latin (id int PRIMARY KEY, l varchar(20) CHARACTER SET latin1, j json) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

// inheritedCollationSQL declares the same tables with no column collation:
// reset keeps its table default, retable changes it to utf8mb4_general_ci, and
// latin keeps its own character set. A json column holds no collation the
// declaration can name.
const inheritedCollationSQL = `
CREATE TABLE reset (id int PRIMARY KEY, a varchar(20) NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
CREATE TABLE retable (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
CREATE TABLE latin (id int PRIMARY KEY, l varchar(20) CHARACTER SET latin1 NULL, j json NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

// inheritedCollationHCL is that declaration as an Atlas HCL document for the
// database named database, the input both ptah-compat and the pinned community
// binary read.
func inheritedCollationHCL(database string) string {
	return strings.ReplaceAll(`schema "DB" {}
table "reset" {
  schema  = schema.DB
  charset = "utf8mb4"
  collate = "utf8mb4_unicode_ci"
  column "id" { type = int }
  column "a" {
    type = varchar(20)
    null = true
  }
  primary_key { columns = [column.id] }
}
table "retable" {
  schema  = schema.DB
  charset = "utf8mb4"
  collate = "utf8mb4_general_ci"
  column "id" { type = int }
  column "n" {
    type = varchar(20)
    null = true
  }
  primary_key { columns = [column.id] }
}
table "latin" {
  schema  = schema.DB
  charset = "utf8mb4"
  collate = "utf8mb4_unicode_ci"
  column "id" { type = int }
  column "l" {
    type    = varchar(20)
    null    = true
    charset = "latin1"
  }
  column "j" {
    type = json
    null = true
  }
  primary_key { columns = [column.id] }
}
`, "DB", database)
}

// inheritedCollationWant is what every table holds after a plan that
// converges: the table's default collation and the collation of its column.
var inheritedCollationWant = map[string]string{
	"reset":   "utf8mb4_unicode_ci utf8mb4_unicode_ci",
	"retable": "utf8mb4_general_ci utf8mb4_general_ci",
	"latin":   "utf8mb4_unicode_ci latin1_swedish_ci",
}

// inheritedCollationEngines are the servers the tests run on.
var inheritedCollationEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
}

// heldInheritedCollations reads, for each table of database, its default
// collation and the collation of its column a, n or l.
func heldInheritedCollations(c *qt.C, scratch mysqlScratch, database string) map[string]string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(), `
		SELECT t.TABLE_NAME, CONCAT(t.TABLE_COLLATION, ' ', c.COLLATION_NAME)
		FROM information_schema.TABLES t JOIN information_schema.COLUMNS c
		  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
		WHERE t.TABLE_SCHEMA = ? AND c.COLUMN_NAME IN ('a', 'n', 'l')`, database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	held := make(map[string]string)
	for rows.Next() {
		var table, collations string
		c.Assert(rows.Scan(&table, &collations), qt.IsNil)
		held[table] = collations
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// plannedAlters are the ALTER TABLE statements of a printed plan, with the
// database's qualifier taken out, so ptah-compat's qualified names read as the
// community binary's.
func plannedAlters(output, database string) []string {
	var alters []string
	for line := range strings.Lines(output) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ALTER TABLE ") {
			alters = append(alters, strings.ReplaceAll(line, "`"+database+"`.", ""))
		}
	}
	return alters
}

// convergesInOneApply plans with apply, applies, and plans again: the first
// plan is want, the apply leaves every table holding
// [inheritedCollationWant], and the second plan is empty. apply runs the
// command with one more argument, --dry-run or --auto-approve, and returns
// what it printed.
func convergesInOneApply(c *qt.C, scratch mysqlScratch, database string, apply func(extra string) string, want []string) {
	c.Helper()
	planned := apply("--dry-run")
	c.Assert(plannedAlters(planned, database), qt.DeepEquals, want, qt.Commentf("%s", planned))
	applied := apply("--auto-approve")
	c.Assert(heldInheritedCollations(c, scratch, database), qt.DeepEquals, inheritedCollationWant, qt.Commentf("%s", applied))
	again := apply("--dry-run")
	c.Assert(plannedAlters(again, database), qt.HasLen, 0, qt.Commentf("%s", again))
}

// TestSchemaApplyResetsAnInheritedMySQLCollationE2E applies a declaration that
// leaves column collations to the server. A column that holds another
// collation than the one it inherits from its table is reset; a changed table
// default is planned in one statement with the reset of the column that takes
// it, so one apply converges; and a column that takes its collation from its
// own character set is left alone.
func TestSchemaApplyResetsAnInheritedMySQLCollationE2E(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.builtFrom(c, inheritedCollationBefore)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(inheritedCollationSQL), 0o600), qt.IsNil)

			convergesInOneApply(c, scratch, database, func(extra string) string {
				return runPtahNative(c, "schema", "apply", "--db-url", url, "--schema-file", schema, extra)
			}, []string{
				"ALTER TABLE `reset` MODIFY COLUMN `a` varchar(20);",
				"ALTER TABLE `retable` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20);",
			})
		})
	}
}

// TestCompatSchemaApplyResetsAnInheritedMySQLCollationE2E is the same apply
// through ptah-compat, which reads the declaration as an Atlas HCL document
// and plans the same statements.
func TestCompatSchemaApplyResetsAnInheritedMySQLCollationE2E(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.builtFrom(c, inheritedCollationBefore)
			schema := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(schema, []byte(inheritedCollationHCL(database)), 0o600), qt.IsNil)

			convergesInOneApply(c, scratch, database, func(extra string) string {
				applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "-u", url, "--to", "file://"+filepath.ToSlash(schema), extra)
				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				return applied.Stdout
			}, []string{
				"ALTER TABLE `reset` MODIFY COLUMN `a` varchar(20) NULL;",
				"ALTER TABLE `retable` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;",
			})
		})
	}
}

// TestInheritedMySQLCollationOnTheCommunityBinaryE2E holds the measurement
// ptah-compat is compared with: the pinned community binary, on the same HCL
// document and the same tables. It resets the column of reset and leaves
// latin alone, as ptah-compat does. Given a dev database, it plans the change
// of retable's default collation in one statement with the reset of its
// column, as ptah-compat does, and converges in one apply. Without one, it
// plans the table's change alone, so the column keeps utf8mb4_bin after the
// apply, and only the next plan resets it: two applies where ptah-compat
// needs one. ptah-compat does not copy that plan, because it describes a
// state the apply does not reach.
func TestInheritedMySQLCollationOnTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	tests := []struct {
		name    string
		withDev bool
		planned []string
		held    map[string]string
		again   []string
	}{
		{
			name:    "with a dev database",
			withDev: true,
			planned: []string{
				"ALTER TABLE `reset` MODIFY COLUMN `a` varchar(20) NULL;",
				"ALTER TABLE `retable` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;",
			},
			held: inheritedCollationWant,
		},
		{
			name: "without a dev database",
			planned: []string{
				"ALTER TABLE `reset` MODIFY COLUMN `a` varchar(20) NULL;",
				"ALTER TABLE `retable` COLLATE utf8mb4_general_ci;",
			},
			held: map[string]string{
				"reset":   "utf8mb4_unicode_ci utf8mb4_unicode_ci",
				"retable": "utf8mb4_general_ci utf8mb4_bin",
				"latin":   "utf8mb4_unicode_ci latin1_swedish_ci",
			},
			again: []string{"ALTER TABLE `retable` MODIFY COLUMN `n` varchar(20) NULL;"},
		},
	}
	for _, engine := range inheritedCollationEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				database, url := scratch.builtFrom(c, inheritedCollationBefore)
				_, devURL := scratch.database(c, "collation_dev")
				schema := filepath.Join(c.TempDir(), "schema.hcl")
				c.Assert(os.WriteFile(schema, []byte(inheritedCollationHCL(database)), 0o600), qt.IsNil)
				apply := []string{"schema", "apply", "-u", referenceMySQLURL(c, url), "--to", "file://" + filepath.ToSlash(schema)}
				apply = append(apply, map[bool][]string{true: {"--dev-url", referenceMySQLURL(c, devURL)}}[test.withDev]...)
				community := func(extra string) string {
					output, err := exec.CommandContext(c.Context(), reference, append(apply, extra)...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary
					c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
					return string(output)
				}

				planned := community("--dry-run")

				c.Assert(plannedAlters(planned, database), qt.DeepEquals, test.planned, qt.Commentf("%s", planned))
				applied := community("--auto-approve")
				c.Assert(heldInheritedCollations(c, scratch, database), qt.DeepEquals, test.held, qt.Commentf("%s", applied))
				again := community("--dry-run")
				c.Assert(plannedAlters(again, database), qt.DeepEquals, test.again, qt.Commentf("%s", again))
			})
		}
	}
}

// referenceMySQLURL is url in the mysql:// form the community binary reads.
func referenceMySQLURL(c *qt.C, url string) string {
	c.Helper()
	parsed, err := atlasurl.ParseMySQLURL(url)
	c.Assert(err, qt.IsNil)
	address := parsed.URL()
	address.Scheme = "mysql"
	return address.String()
}

// TestSchemaApplyReadsWhatUTF8NamesInTheSessionE2E declares utf8_bin on two
// MariaDB columns, one holding utf8mb3_bin and one utf8mb4_bin. Under the
// default old_mode, which holds UTF8_IS_UTF8MB3, utf8 names utf8mb3, so the
// second column is the one that changes. A URL that sets old_mode to empty
// makes utf8 name utf8mb4 in every session Ptah opens, and the first column is
// the one that changes. Each apply converges.
func TestSchemaApplyReadsWhatUTF8NamesInTheSessionE2E(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		planned []string
		want    map[string]string
	}{
		{
			name:    "the default old_mode",
			planned: []string{"ALTER TABLE `t` MODIFY COLUMN `d` varchar(20) COLLATE utf8_bin;"},
			want:    map[string]string{"c": "utf8mb3_bin", "d": "utf8mb3_bin"},
		},
		{
			name:    "old_mode empty",
			query:   "?old_mode=%27%27",
			planned: []string{"ALTER TABLE `t` MODIFY COLUMN `c` varchar(20) COLLATE utf8_bin;"},
			want:    map[string]string{"c": "utf8mb4_bin", "d": "utf8mb4_bin"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
			database, url := scratch.builtFrom(c,
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20) COLLATE utf8mb3_bin, d varchar(20) COLLATE utf8mb4_bin) "+
					"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20) COLLATE utf8_bin NULL, d varchar(20) COLLATE utf8_bin NULL) "+
					"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n"), 0o600), qt.IsNil)
			args := []string{"schema", "apply", "--db-url", url + test.query, "--schema-file", schema}

			planned := runPtahNative(c, append(args, "--dry-run")...)

			c.Assert(plannedAlters(planned, database), qt.DeepEquals, test.planned, qt.Commentf("%s", planned))
			applied := runPtahNative(c, append(args, "--auto-approve")...)
			c.Assert(mysqlColumnCollations(c, scratch, database, "t"), qt.DeepEquals, test.want, qt.Commentf("%s", applied))
			c.Assert(runPtahNative(c, append(args, "--dry-run")...), qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSchemaApplyReadsTheSessionDefaultOfUTF8MB4E2E declares a MySQL table
// with DEFAULT CHARSET=utf8mb4 and no collation, which takes the session's
// default_collation_for_utf8mb4. information_schema.CHARACTER_SETS reports
// utf8mb4_0900_ai_ci whatever the variable says, so the read takes the
// variable. A URL that sets it to utf8mb4_general_ci finds a table holding
// utf8mb4_general_ci synced, and one without it plans the change to
// utf8mb4_0900_ai_ci that a CREATE TABLE in that session would give.
func TestSchemaApplyReadsTheSessionDefaultOfUTF8MB4E2E(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		planned []string
	}{
		{name: "the default variable", planned: []string{"ALTER TABLE `t` COLLATE utf8mb4_0900_ai_ci;"}},
		{name: "utf8mb4_general_ci", query: "?default_collation_for_utf8mb4=utf8mb4_general_ci"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
			database, url := scratch.builtFrom(c, "CREATE TABLE t (id int PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id int PRIMARY KEY) DEFAULT CHARSET=utf8mb4;\n"), 0o600), qt.IsNil)

			planned := runPtahNative(c, "schema", "apply", "--db-url", url+test.query, "--schema-file", schema, "--dry-run")

			c.Assert(plannedAlters(planned, database), qt.DeepEquals, test.planned, qt.Commentf("%s", planned))
		})
	}
}

// tableCharsetHeld reads the default collation of table t of database, the
// collation of its column c, and the bytes of c in its one row, in hex.
func tableCharsetHeld(c *qt.C, scratch mysqlScratch, database string) []string {
	c.Helper()
	var table, column, value string
	err := scratch.admin.QueryRowContext(c.Context(), `
		SELECT t.TABLE_COLLATION, c.COLLATION_NAME, (SELECT HEX(c) FROM `+"`"+database+"`"+`.t)
		FROM information_schema.TABLES t JOIN information_schema.COLUMNS c
		  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
		WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = 't' AND c.COLUMN_NAME = 'c'`, database).Scan(&table, &column, &value)
	c.Assert(err, qt.IsNil)
	return []string{table, column, value}
}

// TestSchemaApplyConvertsATableCharsetE2E declares a latin1 table in utf8mb4.
// The table's character set and collation are planned in one statement with
// the reset of the column that takes them, the column's value is converted,
// and one apply converges.
func TestSchemaApplyConvertsATableCharsetE2E(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.builtFrom(c, "CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin;\n"+
				"INSERT INTO t VALUES (1, 'abc');")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20) NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n"), 0o600), qt.IsNil)
			args := []string{"schema", "apply", "--db-url", url, "--schema-file", schema}

			planned := runPtahNative(c, append(args, "--dry-run")...)

			c.Assert(plannedAlters(planned, database), qt.DeepEquals,
				[]string{"ALTER TABLE `t` CHARSET utf8mb4, COLLATE utf8mb4_bin, MODIFY COLUMN `c` varchar(20);"}, qt.Commentf("%s", planned))
			applied := runPtahNative(c, append(args, "--auto-approve")...)
			c.Assert(tableCharsetHeld(c, scratch, database), qt.DeepEquals, []string{"utf8mb4_bin", "utf8mb4_bin", "616263"}, qt.Commentf("%s", applied))
			c.Assert(runPtahNative(c, append(args, "--dry-run")...), qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSchemaApplyConvertsATableCharsetE2E_FailurePath declares a utf8mb4 table
// in latin1 while its column holds `x😀`, which latin1 cannot hold. The
// comparison rates the conversion destructive. The apply fails with Error 1366
// on the one statement that changes the table and its column, so the table
// keeps its default, the column its collation, and the row its value.
// Planned as two statements, the first changes the table's default before the
// second fails.
func TestSchemaApplyConvertsATableCharsetE2E_FailurePath(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.builtFrom(c, "CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n"+
				"INSERT INTO t VALUES (1, 'x😀');")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20) NULL) DEFAULT CHARSET=latin1 COLLATE=latin1_bin;\n"), 0o600), qt.IsNil)

			drift, err := runPtahNativeWithError("schema", "drift", "--db-url", url, "--schema-file", schema, "--exit-code=false")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", drift))
			applied, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

			c.Assert(drift, qt.Contains, "- column_charsets_converted: 1 (destructive)\n")
			c.Assert(err, qt.ErrorMatches, `(?s).*Error 1366.*`)
			c.Assert(tableCharsetHeld(c, scratch, database), qt.DeepEquals, []string{"utf8mb4_bin", "utf8mb4_bin", "78F09F9880"}, qt.Commentf("%s", applied))
			c.Assert(plannedAlters(applied, database), qt.DeepEquals,
				[]string{"ALTER TABLE `t` CHARSET latin1, COLLATE latin1_bin, MODIFY COLUMN `c` varchar(20);"}, qt.Commentf("%s", applied))
		})
	}
}
