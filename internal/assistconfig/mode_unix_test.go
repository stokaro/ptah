//go:build !windows

package assistconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/assistconfig"
)

// The permission checks live in their own file because they are real on one
// platform and vacuous on the other. Written as one test with a conditional
// inside, the Windows half would assert that a file holding a key is readable
// by everyone and pass.

func TestLoad_RefusesAConfigurationOtherUsersCanRead(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), assistconfig.FileName)
	// #nosec G306 -- the permissive mode IS the fixture: the refusal is what is
	// being measured.
	c.Assert(os.WriteFile(path, []byte("profiles: {}\n"), 0o644), qt.IsNil)

	config, err := assistconfig.Load(assistconfig.Options{
		Path:    path,
		Environ: func(string) (string, bool) { return "", false },
	})

	c.Assert(err, qt.ErrorIs, assistconfig.ErrInsecureFile)
	c.Assert(err, qt.ErrorMatches, `.*is 0644; chmod 600 it`)
	c.Assert(config, qt.IsNil)
}

func TestProvider_RefusesACredentialFileOtherUsersCanRead(t *testing.T) {
	// The same refusal ssh makes about a private key: a file mode is the only
	// thing between a shared machine and the credential.
	c := qt.New(t)
	dir := c.TempDir()
	keyPath := filepath.Join(dir, "key")
	// #nosec G306 -- the group-readable mode IS the fixture.
	c.Assert(os.WriteFile(keyPath, []byte("not-a-real-key\n"), 0o640), qt.IsNil)

	configPath := filepath.Join(dir, assistconfig.FileName)
	content := "profiles:\n  work:\n    type: anthropic\n    model: m\n    credential: file:" + keyPath + "\n"
	c.Assert(os.WriteFile(configPath, []byte(content), 0o600), qt.IsNil)

	opts := assistconfig.Options{
		Path:    configPath,
		Environ: func(string) (string, bool) { return "", false },
	}
	config, err := assistconfig.Load(opts)
	c.Assert(err, qt.IsNil)
	profile, err := config.Profile("work")
	c.Assert(err, qt.IsNil)

	provider, err := config.Provider(profile, opts)

	c.Assert(err, qt.ErrorIs, assistconfig.ErrInsecureFile)
	c.Assert(provider, qt.IsNil)
}
