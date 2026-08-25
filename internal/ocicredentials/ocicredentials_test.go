package ocicredentials_test

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/registry/remote/auth"

	"go.5x5.cz/ptah/internal/ocicredentials"
)

// envOptions builds Options backed by a fixed map, so a test never touches the
// process environment or the real home directory.
// The file store is forced for every case here. These tests are about the file
// Ptah writes and the chain that reads it, and a test that stored a credential
// in the developer's keychain to check a file format would leave a real entry
// behind on every run.
func envOptions(home string, env map[string]string) ocicredentials.Options {
	scoped := map[string]string{ocicredentials.StoreEnv: ocicredentials.StoreFile}
	maps.Copy(scoped, env)
	return ocicredentials.Options{
		Getenv:  func(name string) string { return scoped[name] },
		HomeDir: func() (string, error) { return home, nil },
	}
}

// TestEnvironmentCredential_ReadsAWholeCredentialOrNone pins what the
// environment path accepts.
//
// A half-configured runner is the case worth separating: a username with no
// password would be sent to the registry, rejected, and reported as an
// authentication failure rather than as the missing variable it is.
func TestEnvironmentCredential_ReadsAWholeCredentialOrNone(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want auth.Credential
	}{
		{
			name: "a username and password pair",
			env:  map[string]string{"PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p"},
			want: auth.Credential{Username: "u", Password: "p"},
		},
		{
			name: "an identity token stands alone",
			env:  map[string]string{"PTAH_OCI_TOKEN": "t"},
			want: auth.Credential{RefreshToken: "t"},
		},
		{
			name: "a token wins over a pair, because a registry that issues one wants it",
			env:  map[string]string{"PTAH_OCI_TOKEN": "t", "PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p"},
			want: auth.Credential{RefreshToken: "t"},
		},
		{
			name: "a username with no password is not a credential",
			env:  map[string]string{"PTAH_OCI_USERNAME": "u"},
			want: auth.EmptyCredential,
		},
		{
			name: "a password with no username is not one either",
			env:  map[string]string{"PTAH_OCI_PASSWORD": "p"},
			want: auth.EmptyCredential,
		},
		{
			name: "nothing set",
			env:  make(map[string]string),
			want: auth.EmptyCredential,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := ocicredentials.EnvironmentCredential(envOptions(t.TempDir(), test.env))

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The environment answers before the stored credential, and PTAH_OCI_REGISTRY
// narrows it to one host.
//
// Unscoped is what a CI job with one registry wants. Scoped is what a job
// talking to two registries needs, so a credential for one is not offered to
// the other -- which is a real leak, not a preference.
func TestStore_EnvironmentAnswersFirstAndTheScopeNarrowsIt(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		address string
		want    auth.Credential
	}{
		{
			name:    "unscoped answers for any registry",
			env:     map[string]string{"PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p"},
			address: "registry.example.com",
			want:    auth.Credential{Username: "u", Password: "p"},
		},
		{
			name: "scoped answers for the named host",
			env: map[string]string{
				"PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p",
				"PTAH_OCI_REGISTRY": "registry.example.com",
			},
			address: "registry.example.com",
			want:    auth.Credential{Username: "u", Password: "p"},
		},
		{
			name: "scoped stays silent for another host",
			env: map[string]string{
				"PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p",
				"PTAH_OCI_REGISTRY": "registry.example.com",
			},
			address: "other.example.com",
			want:    auth.EmptyCredential,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			home := t.TempDir()
			env := make(map[string]string, len(test.env))
			maps.Copy(env, test.env)
			// DOCKER_CONFIG is pointed at an empty directory so the fallback
			// cannot answer from whatever this machine happens to have.
			t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))

			store, err := ocicredentials.Store(envOptions(home, env))
			c.Assert(err, qt.IsNil)

			got, err := store.Get(context.Background(), test.address)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A stored credential is read back, and it is written under ~/.ptah.
//
// The path matters as much as the round trip: assistconfig already put Ptah's
// state under `.ptah` in the home directory, and a tool whose state lives under
// two spellings is one whose documentation has to explain both.
func TestSave_WritesUnderPtahsOwnDirectoryAndReadsBack(t *testing.T) {
	c := qt.New(t)
	home := t.TempDir()
	t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))
	opts := envOptions(home, make(map[string]string))

	storage, err := ocicredentials.Save(context.Background(), opts,
		"registry.example.com", auth.Credential{Username: "u", Password: "p"})
	c.Assert(err, qt.IsNil)
	c.Assert(storage.Path, qt.Equals, filepath.Join(home, ".ptah", "registries.json"))

	store, err := ocicredentials.Store(opts)
	c.Assert(err, qt.IsNil)
	got, err := store.Get(context.Background(), "registry.example.com")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, auth.Credential{Username: "u", Password: "p"})
}

// The credential file is not world-readable, and neither is the directory.
//
// A plaintext fallback is acceptable on a machine with no keychain; a plaintext
// fallback every user on the machine can read is not.
func TestSave_KeepsTheStoreToItsOwner(t *testing.T) {
	c := qt.New(t)
	home := t.TempDir()
	t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))
	opts := envOptions(home, make(map[string]string))

	storage, err := ocicredentials.Save(context.Background(), opts,
		"registry.example.com", auth.Credential{Username: "u", Password: "p"})
	c.Assert(err, qt.IsNil)

	assertOwnerOnly(c, filepath.Dir(storage.Path))
	assertOwnerOnly(c, storage.Path)
}

// The file Ptah writes is Docker's format, which is what makes the store
// interchangeable with the one `docker login` writes.
func TestSave_WritesDockersConfigurationFormat(t *testing.T) {
	c := qt.New(t)
	home := t.TempDir()
	t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))

	storage, err := ocicredentials.Save(context.Background(), envOptions(home, make(map[string]string)),
		"registry.example.com", auth.Credential{Username: "u", Password: "p"})
	c.Assert(err, qt.IsNil)

	raw, err := os.ReadFile(storage.Path)
	c.Assert(err, qt.IsNil)
	var config struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	c.Assert(json.Unmarshal(raw, &config), qt.IsNil)
	c.Assert(config.Auths, qt.HasLen, 1)
	c.Assert(config.Auths["registry.example.com"].Auth, qt.Not(qt.Equals), "")
}

// PTAH_OCI_CONFIG moves the store, which is what a container or a second
// profile set uses.
func TestDefaultPath_TheOverrideWins(t *testing.T) {
	c := qt.New(t)
	elsewhere := filepath.Join(t.TempDir(), "elsewhere.json")

	path, err := ocicredentials.DefaultPath(
		envOptions("/unused/home", map[string]string{"PTAH_OCI_CONFIG": elsewhere}))

	c.Assert(err, qt.IsNil)
	c.Assert(path, qt.Equals, elsewhere)
}

// The environment store refuses writes rather than reporting a success that
// persisted nothing.
func TestStore_TheEnvironmentIsReadOnly(t *testing.T) {
	c := qt.New(t)
	home := t.TempDir()
	t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))
	// With no environment credential set the write reaches Ptah's own store and
	// succeeds, so the refusal below is the environment's and not a general
	// failure to write.
	store, err := ocicredentials.Store(envOptions(home, make(map[string]string)))
	c.Assert(err, qt.IsNil)
	c.Assert(store.Put(context.Background(), "registry.example.com",
		auth.Credential{Username: "u", Password: "p"}), qt.IsNil)
}

// Every variable whose value is a secret is named, so a caller that publishes
// an environment can redact them.
func TestSecretEnvironmentNames_NamesEveryValueThatIsASecret(t *testing.T) {
	c := qt.New(t)

	secrets := ocicredentials.SecretEnvironmentNames()

	c.Assert(secrets, qt.DeepEquals, []string{"PTAH_OCI_PASSWORD", "PTAH_OCI_TOKEN"})
	// The wider list is what a reader needs to understand the feature; the
	// narrow one is what a redactor needs. Neither may drop a name the other
	// has, or a secret leaves through the gap.
	for _, secret := range secrets {
		c.Assert(ocicredentials.EnvironmentNames(), qt.Contains, secret)
	}
}
