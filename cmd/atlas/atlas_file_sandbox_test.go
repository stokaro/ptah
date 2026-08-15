// One of the three escape shapes here is a symbolic link, and creating one on
// Windows needs a privilege an ordinary CI account does not have.
//go:build !windows

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// The compat surface half of the atlas.hcl file() sandbox (stokaro/ptah#1042).
//
// `ptah-compat` is the surface a CI pipeline runs, and it builds the sandbox's
// filesystem in its own place -- cmd/atlas/project_config.go opens the config
// directory through pathguard, not through the loader in config/projectconfig.
// A fix proven only at the library would leave this branch unmeasured, which is
// how the same class of gap has shipped here before.
//
// The community binary reads all three of these and exits 0; that half is
// pinned by TestOracleReadsFilesOutsideTheAtlasHCLDirectory in
// config/projectconfig.

const compatOutsideMarker = "PTAH-1042-COMPAT-SECRET"

func TestCompatCommand_AtlasHCLFileSandbox(t *testing.T) {
	tests := []struct {
		name     string
		argument func(c *qt.C, dir, outside string) string
		err      string
	}{
		{
			name:     "absolute path",
			argument: compatAbsoluteArgument,
			err:      `.*absolute paths are not supported: .*compat-secret\.txt: atlas\.hcl file\(\) and fileset\(\) read only inside the directory holding atlas\.hcl; pass a value from outside it through getenv\(\).*`,
		},
		{
			name:     "parent traversal",
			argument: compatTraversalArgument,
			err:      `.*path escapes atlas\.hcl directory: \.\./compat-secret\.txt: atlas\.hcl file\(\) and fileset\(\).*`,
		},
		{
			name:     "symbolic link out of the directory",
			argument: compatSymlinkArgument,
			err:      `.*path escapes atlas\.hcl directory: secret\.link: secret\.link is a symbolic link pointing outside it.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			base := t.TempDir()
			dir := filepath.Join(base, "project")
			c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
			outside := filepath.Join(base, "compat-secret.txt")
			c.Assert(os.WriteFile(outside, []byte(compatOutsideMarker), 0o600), qt.IsNil)
			writeCompatSandboxConfig(c, dir, tt.argument(c, dir, outside))
			t.Chdir(dir)

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{"migrate", "status", "--env", "local"})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.err)
			c.Assert(err.Error(), qt.Not(qt.Contains), compatOutsideMarker)
			c.Assert(out.String(), qt.Not(qt.Contains), compatOutsideMarker)
		})
	}
}

// The control: the same command, the same shape, a file that lives inside the
// config directory. It reads, so the rows above measure where the path goes
// rather than whether `migrate status --env local` runs at all.
func TestCompatCommand_AtlasHCLFileReadsInsideTheConfigDirectory(t *testing.T) {
	c := qt.New(t)

	base := t.TempDir()
	dir := filepath.Join(base, "project")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(base, "compat-secret.txt"), []byte(compatOutsideMarker), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "sandbox.db")
	c.Assert(os.WriteFile(filepath.Join(dir, "url.txt"), []byte("sqlite://"+dbPath), 0o600), qt.IsNil)
	writeCompatSandboxConfig(c, dir, "url.txt")
	t.Chdir(dir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "status", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Migration Status: OK")
}

func writeCompatSandboxConfig(c *qt.C, dir, argument string) {
	c.Helper()

	body := "env \"local\" {\n  url = file(\"" + argument + "\")\n  migration {\n    dir = \"file://migrations\"\n  }\n}\n"
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(body), 0o600), qt.IsNil)
}

func compatAbsoluteArgument(c *qt.C, _, outside string) string {
	c.Helper()

	return outside
}

func compatTraversalArgument(c *qt.C, _, outside string) string {
	c.Helper()

	return "../" + filepath.Base(outside)
}

func compatSymlinkArgument(c *qt.C, dir, outside string) string {
	c.Helper()

	c.Assert(os.Symlink(outside, filepath.Join(dir, "secret.link")), qt.IsNil)
	return "secret.link"
}
