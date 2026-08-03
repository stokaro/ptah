package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// writeSchemaTestScopeFixture writes a desired schema whose two tables live in
// two different schemas, plus a case that only needs the one in "main".
//
// The fixture is discriminating on purpose. On SQLite only "main" exists, so
// provisioning the unscoped schema fails outright on the "other" table: a
// --schema that was parsed and then ignored cannot make this pass, and a
// --schema that kept both tables cannot either.
func writeSchemaTestScopeFixture(c *qt.C, modelsDir, testsDir string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "models.go"), []byte(`package models

//ptah:schema:table name="users" schema="main"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}

//ptah:schema:table name="audit" schema="other"
type Audit struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(testsDir, "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users is provisioned\n"+
			"    steps:\n"+
			"      - name: users is empty\n"+
			"        assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"0\"\n"), 0o600), qt.IsNil)
}

// TestSchemaTestSchemaSelection pins what -s/--schema does to the desired
// schema a test run provisions.
//
// Reverted, the flagged rows fail with `unknown flag: --schema`, because the
// spelling reaches the native runner untranslated.
func TestSchemaTestSchemaSelection(t *testing.T) {
	tests := []struct {
		name    string
		extra   []string
		wantErr bool
		want    string
	}{
		{
			name:    "no selection provisions every schema and fails on SQLite",
			wantErr: true,
			want:    `unknown database "other"`,
		},
		{
			name:    "long flag keeps only the selected schema",
			extra:   []string{"--schema", "main"},
			wantErr: false,
			want:    `PASS  case "users is provisioned"`,
		},
		{
			name:    "shorthand keeps only the selected schema",
			extra:   []string{"-s", "main"},
			wantErr: false,
			want:    `PASS  case "users is provisioned"`,
		},
		{
			name:    "comma-separated values union",
			extra:   []string{"--schema", "main,nosuch"},
			wantErr: false,
			want:    `PASS  case "users is provisioned"`,
		},
		{
			name: "repeated values accumulate rather than the last one winning",
			// With last-wins forwarding this run would be scoped to "nosuch"
			// alone and refuse; it passes only if "main" survived too.
			extra:   []string{"-s", "main", "-s", "nosuch"},
			wantErr: false,
			want:    `PASS  case "users is provisioned"`,
		},
		{
			name:    "a selection that keeps nothing is refused",
			extra:   []string{"--schema", "nosuch"},
			wantErr: true,
			want:    "selects no tables out of the desired schema",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			modelsDir, testsDir := c.TempDir(), c.TempDir()
			writeSchemaTestScopeFixture(c, modelsDir, testsDir)
			devDB := "sqlite://" + filepath.Join(c.TempDir(), "dev.db")

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			args := append([]string{
				"schema", "test", testsDir,
				"-u", "file://" + modelsDir,
				"--dev-url", devDB,
			}, test.extra...)
			cmd.SetArgs(args)

			err := cmd.Execute()

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("%s", out.String()))
			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}
