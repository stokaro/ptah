//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyOfAColumnCheckOverTwoColumnsConvergesE2E applies a YAML
// schema whose column CHECK names another column, twice. The renderer writes
// the CHECK unnamed and PostgreSQL names it `e_check`. A comparison looking for
// `e_a_check` would rename it on every apply after the first, and a `checks`
// entry also named `e_check` would fail the first apply: the server names the
// column's CHECK first and then refuses the entry with `check constraint
// "e_check" already exists` (stokaro/ptah#3750).
//
// The CHECKs read back are compared with a database the equivalent SQL built,
// so the names asserted are the server's.
func TestSchemaApplyOfAColumnCheckOverTwoColumnsConvergesE2E(t *testing.T) {
	tests := []struct {
		name       string
		yaml       string
		equivalent string
	}{
		{
			name: "a column CHECK over two columns",
			yaml: `tables:
  e:
    columns:
      a:
        type: INTEGER
        check: a IS NULL OR b IS NOT NULL
      b:
        type: INTEGER
`,
			equivalent: `CREATE TABLE e (a INTEGER CHECK (a IS NULL OR b IS NOT NULL), b INTEGER);`,
		},
		{
			name: "beside a checks entry",
			yaml: `tables:
  e:
    columns:
      a:
        type: INTEGER
        check: a IS NULL OR b IS NOT NULL
      b:
        type: INTEGER
        check: b > 0
    checks:
      - a > 0
`,
			equivalent: `CREATE TABLE e (a INTEGER CHECK (a IS NULL OR b IS NOT NULL), b INTEGER CHECK (b > 0),
  CONSTRAINT e_check1 CHECK (a > 0));`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := checkConstraintsOf(c, databaseBuiltFrom(c, test.equivalent))
			target, _ := scratchReplayDatabase(c)
			schema := filepath.Join(c.TempDir(), "schema.yaml")
			c.Assert(os.WriteFile(schema, []byte(test.yaml), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(checkConstraintsOf(c, target), qt.DeepEquals, want)
			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}
