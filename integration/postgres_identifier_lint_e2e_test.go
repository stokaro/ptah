//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/sqllint"
	"ptah.run/internal/sqlschema"
)

// sqlLintFindings runs `ptah sql lint` over sql as PostgreSQL and returns the
// findings of rule.
func sqlLintFindings(c *qt.C, sql, rule string) []sqllint.Finding {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	_, stdout, stderr := runAsOneRun(c, nativeTree, "sql", "lint", "--dialect", "postgres", "--format", "json", path)
	var report struct {
		Findings []sqllint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	return slices.DeleteFunc(report.Findings, func(finding sqllint.Finding) bool { return finding.Rule != rule })
}

// lintStoredNames is the name each DDL003 finding says the server stores.
func lintStoredNames(findings []sqllint.Finding) []string {
	var stored []string
	for _, finding := range findings {
		stored = append(stored, lintStoredAs.FindStringSubmatch(finding.Message)[1])
	}
	return stored
}

var lintStoredAs = regexp.MustCompile(`it is stored as "([^"]*)"`)

// TestSQLLintIdentifierCollisionsAgreeWithTheServerE2E runs each file on the
// server and through `ptah sql lint`: DDL004 reports exactly the files the
// server refuses for a name cut to one in use, and nothing for the same cut
// name in a namespace of its own or under a guard the server skips.
func TestSQLLintIdentifierCollisionsAgreeWithTheServerE2E(t *testing.T) {
	long := strings.Repeat("k", 63)
	tests := []struct {
		name    string
		sql     string
		refused bool
	}{
		{
			name:    "two indexes in one schema",
			sql:     "CREATE TABLE t (a int, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\nCREATE INDEX \"" + long + "_beta\" ON t (b);",
			refused: true,
		},
		{
			name: "two foreign keys on one table",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT \"" + long +
				"_alpha\" FOREIGN KEY (a) REFERENCES p (id), CONSTRAINT \"" + long + "_beta\" FOREIGN KEY (b) REFERENCES p (id));",
			refused: true,
		},
		{
			name:    "an index cut to the name of a table",
			sql:     "CREATE TABLE \"" + long + "\" (a int);\nCREATE INDEX \"" + long + "_x\" ON \"" + long + "\" (a);",
			refused: true,
		},
		{
			name: "a unique constraint and an index",
			sql: "CREATE TABLE t (a int, b int, CONSTRAINT \"" + long + "_alpha\" UNIQUE (a));\n" +
				"CREATE INDEX \"" + long + "_beta\" ON t (b);",
			refused: true,
		},
		{
			name:    "two columns of one table",
			sql:     "CREATE TABLE t (\"" + long + "_alpha\" int, \"" + long + "_beta\" int);",
			refused: true,
		},
		{
			name:    "a name written as another name cut",
			sql:     "CREATE TABLE t (a int, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\nCREATE INDEX \"" + long + "\" ON t (b);",
			refused: true,
		},
		{
			name:    "a name cut to another name written",
			sql:     "CREATE TABLE t (a int, b int);\nCREATE INDEX \"" + long + "\" ON t (a);\nCREATE INDEX \"" + long + "_beta\" ON t (b);",
			refused: true,
		},
		{
			name: "a rename onto a cut name",
			sql: "CREATE TABLE t (a int, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\nCREATE INDEX ix ON t (b);\n" +
				"ALTER INDEX ix RENAME TO \"" + long + "_beta\";",
			refused: true,
		},
		{
			name: "two triggers on one table",
			sql: "CREATE TABLE t (a int);\n" + longNamesTriggerFunction +
				`CREATE TRIGGER "` + long + `_alpha" BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();` + "\n" +
				`CREATE TRIGGER "` + long + `_beta" BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();`,
			refused: true,
		},
		{
			name: "foreign keys on two tables",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				`CREATE TABLE c1 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `_alpha" FOREIGN KEY (a) REFERENCES p (id));` + "\n" +
				`CREATE TABLE c2 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `_beta" FOREIGN KEY (a) REFERENCES p (id));`,
		},
		{
			name: "indexes in two schemas",
			sql: "CREATE SCHEMA s1;\nCREATE SCHEMA s2;\nCREATE TABLE s1.t (a int);\nCREATE TABLE s2.t (a int);\n" +
				`CREATE INDEX "` + long + `_alpha" ON s1.t (a);` + "\n" + `CREATE INDEX "` + long + `_beta" ON s2.t (a);`,
		},
		{
			name: "triggers on two tables",
			sql: "CREATE TABLE t1 (a int);\nCREATE TABLE t2 (a int);\n" + longNamesTriggerFunction +
				`CREATE TRIGGER "` + long + `_alpha" BEFORE INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION trg();` + "\n" +
				`CREATE TRIGGER "` + long + `_beta" BEFORE INSERT ON t2 FOR EACH ROW EXECUTE FUNCTION trg();`,
		},
		{
			name: "two indexes guarded by IF NOT EXISTS",
			sql: "CREATE TABLE t (a int, b int);\nCREATE INDEX IF NOT EXISTS \"" + long + "_alpha\" ON t (a);\n" +
				"CREATE INDEX IF NOT EXISTS \"" + long + "_beta\" ON t (b);",
		},
		{
			name: "two views written OR REPLACE",
			sql:  `CREATE OR REPLACE VIEW "` + long + `_alpha" AS SELECT 1 AS a;` + "\n" + `CREATE OR REPLACE VIEW "` + long + `_beta" AS SELECT 2 AS a;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			_, serverErr := conn.ExecContext(c.Context(), test.sql)
			collisions := sqlLintFindings(c, test.sql, sqllint.RuleIdentifierCollision)

			c.Assert(serverErr != nil, qt.Equals, test.refused, qt.Commentf("server: %v", serverErr))
			c.Assert(len(collisions) > 0, qt.Equals, test.refused, qt.Commentf("lint: %v", collisions))
		})
	}
}

// TestSQLLintCutNamesMatchTheCatalogE2E builds a database from a file of long
// names and holds each name DDL003 says the server stores against its
// catalog, one finding per name the file writes past the limit.
func TestSQLLintCutNamesMatchTheCatalogE2E(t *testing.T) {
	c := qt.New(t)

	stored := lintStoredNames(sqlLintFindings(c, longNames, sqllint.RuleIdentifierTruncated))
	catalog := catalogNames(c, databaseBuiltFrom(c, longNames))

	c.Assert(stored, qt.HasLen, 5)
	for _, name := range stored {
		c.Assert(catalog, qt.Contains, name, qt.Commentf("%q is not in the catalog %q", name, catalog))
	}
}

// TestSQLLintOpenMeterExportE2E lints the SQL OpenMeter's Ent exporter writes,
// pinned in testdata: DDL003 reports each name the export writes past 63
// bytes, under the names the catalog of a database the same SQL built holds,
// and DDL004 reports nothing, because the server accepts the file.
func TestSQLLintOpenMeterExportE2E(t *testing.T) {
	c := qt.New(t)
	export, err := os.ReadFile(filepath.Join("testdata", "openmeter", "ent-export.sql"))
	c.Assert(err, qt.IsNil)

	truncated := sqlLintFindings(c, string(export), sqllint.RuleIdentifierTruncated)
	collisions := sqlLintFindings(c, string(export), sqllint.RuleIdentifierCollision)
	cut := serverCutNames(catalogCutNames(c, databaseBuiltFrom(c, string(export))), string(export))

	c.Assert(truncated, qt.HasLen, len(longIdentifiers(string(export))))
	c.Assert(collisions, qt.HasLen, 0)
	stored := lintStoredNames(truncated)
	slices.Sort(stored)
	c.Assert(stored, qt.DeepEquals, catalogStored(cut))
}

// catalogStored is the stored names of names, sorted.
func catalogStored(names []sqlschema.StoredName) []string {
	stored := make([]string, 0, len(names))
	for _, name := range names {
		stored = append(stored, name.Stored)
	}
	slices.Sort(stored)
	return stored
}

// identifierMigrations writes a migration directory whose second version
// builds an index named second, beside the index the first version names
// with 63 bytes and `_alpha`.
func identifierMigrations(c *qt.C, second string) string {
	c.Helper()
	dir := c.TempDir()
	long := strings.Repeat("k", 63)
	files := map[string]string{
		"0000000001_init.up.sql":    "CREATE TABLE t (id int PRIMARY KEY, a int, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\n",
		"0000000001_init.down.sql":  "DROP TABLE t;\n",
		"0000000002_index.up.sql":   "CREATE INDEX \"" + second + "\" ON t (b);\n",
		"0000000002_index.down.sql": "DROP INDEX \"" + second + "\";\n",
	}
	for name, sql := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	}
	return dir
}

// TestMigrationsLintIdentifierCollisionAgreesWithApplyE2E lints a directory
// and applies it to the server: PG402P reports the second version exactly
// when the server refuses it, because its index is cut to the name the first
// version's holds. The control renames the index within the first 63 bytes,
// and both lint and apply pass.
func TestMigrationsLintIdentifierCollisionAgreesWithApplyE2E(t *testing.T) {
	long := strings.Repeat("k", 63)
	tests := []struct {
		name    string
		second  string
		refused bool
	}{
		{name: "cut to the name of the first index", second: long + "_beta", refused: true},
		{name: "a name of its own", second: strings.Repeat("j", 63) + "_beta"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := identifierMigrations(c, test.second)
			target, _ := scratchReplayDatabase(c)

			lintCode, lintOut, _ := runAsOneRun(c, nativeTree, "migrations", "lint", "--dir", dir, "--dir-format", "ptah",
				"--dialect", "postgres", "--format", "json")
			upCode, upOut, upErr := runAsOneRun(c, nativeTree, "migrations", "up", "--migrations-dir", dir, "--dir-format", "ptah", "--db-url", target)

			var report struct {
				Findings []struct {
					Rule string `json:"rule"`
					File string `json:"file"`
				} `json:"findings"`
			}
			c.Assert(json.Unmarshal([]byte(lintOut), &report), qt.IsNil, qt.Commentf("%s", lintOut))
			var collisions []string
			for _, finding := range report.Findings {
				collisions = append(collisions, finding.Rule+" "+filepath.Base(finding.File))
			}
			wantCode := map[bool]int{false: 0, true: 1}[test.refused]
			c.Assert(slices.Contains(collisions, "PG402P 0000000002_index.up.sql"), qt.Equals, test.refused, qt.Commentf("%v", collisions))
			c.Assert(lintCode, qt.Equals, wantCode)
			c.Assert(upCode != 0, qt.Equals, test.refused, qt.Commentf("stdout:\n%s\nstderr:\n%s", upOut, upErr))
		})
	}
}
