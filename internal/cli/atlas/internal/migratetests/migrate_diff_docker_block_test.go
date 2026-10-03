package migratetests_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
)

// TestMigrateDiffReadsADockerBlockOnlyOutsideStrictCE drives `migrate diff`
// through an atlas.hcl whose env names a docker block's url
// (stokaro/ptah#4041). The default surface reads the block, and refuses an
// engine it starts no dev database of before anything is started. Strict CE
// answers as the pinned community binary v1.3.0 does, measured: `Unsupported
// attribute; This object does not have an attribute named "url".`
func TestMigrateDiffReadsADockerBlockOnlyOutsideStrictCE(t *testing.T) {
	tests := []struct {
		name    string
		policy  atlascompatpolicy.Policy
		wantErr string
	}{
		{
			name:    "default",
			policy:  atlascompatpolicy.Full(),
			wantErr: `unsupported atlas.hcl construct "docker.sqlserver"`,
		},
		{
			name:    "strict",
			policy:  atlascompatpolicy.StrictCE(),
			wantErr: `Unsupported attribute; This object does not have an attribute named "url".`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE widgets (id int);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`docker "sqlserver" "dev" {
  image = "mcr.microsoft.com/mssql/server:2022-latest"
}

env "dev" {
  dev = docker.sqlserver.dev.url
  migration {
    dir = "file://migrations"
  }
  schema {
    src = "file://schema.sql"
  }
}
`), 0o600), qt.IsNil)
			t.Chdir(dir)
			cmd := atlas.NewCompatCommandWithPolicy("atlas", test.policy)
			var out, errOut bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)
			cmd.SetArgs([]string{"migrate", "diff", "--env", "dev", "demo"})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(test.wantErr)+`.*`, qt.Commentf("stderr: %s", errOut.String()))
		})
	}
}
