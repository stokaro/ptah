//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// hclActionRules reads the actions of the foreign key c_p, as each engine says them.
var hclActionRules = map[string]string{
	"postgres": "SELECT update_rule || ':' || delete_rule FROM information_schema.referential_constraints WHERE constraint_name = 'c_p'",
	"mysql":    "SELECT CONCAT(update_rule, ':', delete_rule) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND constraint_name = 'c_p'",
	"mariadb":  "SELECT CONCAT(update_rule, ':', delete_rule) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND constraint_name = 'c_p'",
}

// hclActionSchema is a table gaining a foreign key with actions as an HCL schema spells them.
func hclActionSchema(schema, onUpdate, onDelete string) string {
	return fmt.Sprintf(`schema %[1]q {}
table "p" {
  schema = schema.%[1]s
  column "id" { type = int }
  primary_key { columns = [column.id] }
}
table "c" {
  schema = schema.%[1]s
  column "id" { type = int }
  column "p_id" {
    type = int
    null = true
  }
  primary_key { columns = [column.id] }
  foreign_key "c_p" {
    columns = [column.p_id]
    ref_columns = [table.p.column.id]
    on_update = %[2]s
    on_delete = %[3]s
  }
}
`, schema, onUpdate, onDelete)
}

// hclSchemaOf is the schema block an HCL file over target names: the database on the MySQL
// family, and on PostgreSQL the schema these targets are pinned to.
func hclSchemaOf(engine, target string) string {
	if engine == "postgres" {
		return "public"
	}
	name := target[strings.LastIndex(target, "/")+1:]
	name, _, _ = strings.Cut(name, "?")
	return name
}

// TestCompatMigrateDiffE2E_AForeignKeyFromHCLOnEveryEngine writes a file from an HCL schema that
// gives an existing table a foreign key, applies it over a row, reads the actions back and rolls
// back. `NO_ACTION` is HCL's spelling and no server's, so a form of the node nobody prepared shows
// here as a statement every server refuses, in a file `migrate diff` reports as written.
//
// The file runs on the dev database, which the plan leaves empty: on the MySQL family an HCL
// schema names its database, and the plan and its statements name that one.
func TestCompatMigrateDiffE2E_AForeignKeyFromHCLOnEveryEngine(t *testing.T) {
	const from = "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int);\n"
	tests := []struct {
		name     string
		engine   string
		onUpdate string
		onDelete string
		want     string
	}{
		{name: "PostgreSQL, no action", engine: "postgres", onUpdate: "NO_ACTION", onDelete: "NO_ACTION", want: "NO ACTION:NO ACTION"},
		{name: "PostgreSQL, set null", engine: "postgres", onUpdate: "CASCADE", onDelete: "SET_NULL", want: "CASCADE:SET NULL"},
		{name: "PostgreSQL, set default", engine: "postgres", onUpdate: "SET_DEFAULT", onDelete: "SET_DEFAULT", want: "SET DEFAULT:SET DEFAULT"},
		{name: "MySQL, no action", engine: "mysql", onUpdate: "NO_ACTION", onDelete: "NO_ACTION", want: "NO ACTION:NO ACTION"},
		{name: "MySQL, set null", engine: "mysql", onUpdate: "CASCADE", onDelete: "SET_NULL", want: "CASCADE:SET NULL"},
		{name: "MariaDB, no action", engine: "mariadb", onUpdate: "NO_ACTION", onDelete: "NO_ACTION", want: "NO ACTION:NO ACTION"},
		{name: "MariaDB, set null", engine: "mariadb", onUpdate: "CASCADE", onDelete: "SET_NULL", want: "CASCADE:SET NULL"},
	}
	engines := make(map[string]catalogOrderEngine, len(catalogOrderEngines))
	for _, engine := range catalogOrderEngines {
		engines[engine.name] = engine
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			engine := engines[test.engine]
			target := engine.target(c)
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			schema := hclActionSchema(hclSchemaOf(test.engine, target), test.onUpdate, test.onDelete)
			steps := []struct{ name, path, content string }{
				{"s1", filepath.Join(work, "s1.sql"), from},
				{"s2", filepath.Join(work, "s2.hcl"), schema},
			}
			for _, step := range steps {
				c.Assert(os.WriteFile(step.path, []byte(step.content), 0o600), qt.IsNil)
				output, err := runEngineVerb("", []string{
					"migrate", "diff", step.name, "--dir", "file://" + filepath.ToSlash(dir) + "?format=golang-migrate",
					"--to", "file://" + filepath.ToSlash(step.path), "--dev-url", target, "--format", `{{ sql . "  " }}`,
				})
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			}
			pairs := readMigrationPairs(c, dir)
			c.Assert(pairs, qt.HasLen, 2)
			rules := []string{hclActionRules[test.engine]}

			applyEngineFile(c, target, engine.dialect, pairs[0].Up)
			applyEngineFile(c, target, engine.dialect, "INSERT INTO p VALUES (1);\nINSERT INTO c VALUES (1, 1);\n")
			applyEngineFile(c, target, engine.dialect, pairs[1].Up)

			c.Assert(catalogReadBack(c, target, rules), qt.DeepEquals, [][]string{{test.want}})

			applyEngineFile(c, target, engine.dialect, pairs[1].Down)

			c.Assert(catalogReadBack(c, target, rules), qt.DeepEquals, [][]string{{}})
		})
	}
}
