package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A missing index owner is a declaration fault on every schema command. Plan
// and drift must name it as validate does, before comparison or rendering.
func TestSchemaPlanAndDriftNameAnUndeclaredIndexTable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE INDEX i ON missing (a);\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "entities.go"), []byte("package entities\n\ntype Ghost struct {\n //ptah:schema:index name=\"i\" fields=\"a\"\n _ int\n}\n"), 0o600), qt.IsNil)
	for _, source := range []struct {
		name    string
		args    []string
		problem string
	}{
		{name: "SQL", args: []string{"--schema-file", schemaPath}, problem: `sqlite: index "i": names table "missing", which no declaration defines`},
		{name: "Go", args: []string{"--root-dir", dir}, problem: `sqlite: index "i": names table "Ghost", which no declaration defines`},
	} {
		t.Run(source.name, func(t *testing.T) {
			c := qt.New(t)
			validated, code := validateExitCode(c, append(source.args, "--dialect", "sqlite")...)
			c.Assert(code, qt.Equals, 1)
			c.Assert(validated, qt.Contains, source.problem)
			for _, test := range []struct {
				verb  string
				extra []string
			}{{verb: "plan", extra: []string{"--dry-run"}}, {verb: "drift"}} {
				t.Run(test.verb, func(t *testing.T) {
					c := qt.New(t)
					args := []string{test.verb, "--db-url", "sqlite://" + filepath.Join(dir, source.name+test.verb+".db")}
					args = append(args, source.args...)
					args = append(args, test.extra...)
					_, err := runSchema("", args...)
					c.Assert(err, qt.IsNotNil)
					c.Assert(err.Error(), qt.Contains, source.problem)
				})
			}
		})
	}
}
