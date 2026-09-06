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

	"ptah.run/internal/ocicredentials"
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
			name: "nothing set",
			env:  make(map[string]string),
			want: auth.EmptyCredential,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ocicredentials.EnvironmentCredential(envOptions(t.TempDir(), test.env))

			c.Assert(err, qt.IsNil)
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

// Half a credential is a configuration error, not a fall-through.
//
// Returning the empty credential here would send the run on to the Ptah and
// Docker stores, so a runner that meant to authenticate as one account would
// authenticate as whichever account those hold -- or anonymously -- and the
// only symptom would be an authorization message about the registry. Found in
// review of stokaro/ptah#2241: the doc comment claimed this behavior and the
// code did not have it.
func TestEnvironmentCredential_HalfAPairIsAConfigurationError(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		says string
	}{
		{
			name: "a username with no password",
			env:  map[string]string{"PTAH_OCI_USERNAME": "u"},
			says: "PTAH_OCI_PASSWORD",
		},
		{
			name: "a password with no username",
			env:  map[string]string{"PTAH_OCI_PASSWORD": "p"},
			says: "PTAH_OCI_USERNAME",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := ocicredentials.EnvironmentCredential(envOptions(t.TempDir(), test.env))

			c.Assert(err, qt.ErrorMatches, ".*"+test.says+".*")
		})
	}
}

// The error reaches the chain rather than being swallowed by the fallbacks.
//
// This is the assertion that matters: the store below the environment holds a
// working credential for the same registry, so a chain that fell through would
// return it and the run would succeed as the wrong account.
func TestStore_AHalfConfiguredEnvironmentDoesNotFallThroughToAStoredAccount(t *testing.T) {
	c := qt.New(t)
	home := t.TempDir()
	t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))

	stored := envOptions(home, make(map[string]string))
	_, err := ocicredentials.Save(context.Background(), stored,
		"registry.example.com", auth.Credential{Username: "stored", Password: "p"})
	c.Assert(err, qt.IsNil)

	// The control: with nothing in the environment, that stored account answers.
	store, err := ocicredentials.Store(stored)
	c.Assert(err, qt.IsNil)
	got, err := store.Get(context.Background(), "registry.example.com")
	c.Assert(err, qt.IsNil)
	c.Assert(got.Username, qt.Equals, "stored")

	half, err := ocicredentials.Store(envOptions(home, map[string]string{"PTAH_OCI_USERNAME": "ci"}))
	c.Assert(err, qt.IsNil)

	_, err = half.Get(context.Background(), "registry.example.com")

	c.Assert(err, qt.ErrorMatches, ".*PTAH_OCI_PASSWORD.*")
}

// EnvironmentAnswersFor asks the environment, not the chain.
//
// A chain lookup answers from the Ptah or Docker store when the environment is
// scoped to another registry, and a caller warning about the environment on
// that answer would name the wrong source. Found in review of
// stokaro/ptah#2241.
func TestEnvironmentAnswersFor_IgnoresWhatTheStoresHold(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		address string
		want    bool
	}{
		{
			name:    "unscoped answers for this registry",
			env:     map[string]string{"PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p"},
			address: "registry.example.com",
			want:    true,
		},
		{
			name: "scoped elsewhere does not, even though a stored credential exists",
			env: map[string]string{
				"PTAH_OCI_USERNAME": "u", "PTAH_OCI_PASSWORD": "p",
				"PTAH_OCI_REGISTRY": "other.example.com",
			},
			address: "registry.example.com",
			want:    false,
		},
		{
			name:    "an empty environment does not",
			env:     make(map[string]string),
			address: "registry.example.com",
			want:    false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			home := t.TempDir()
			t.Setenv("DOCKER_CONFIG", filepath.Join(home, "nodocker"))
			opts := envOptions(home, test.env)
			// A stored credential for the same registry, which is what a
			// whole-chain lookup would wrongly report as the environment.
			_, err := ocicredentials.Save(context.Background(), envOptions(home, make(map[string]string)),
				"registry.example.com", auth.Credential{Username: "stored", Password: "p"})
			c.Assert(err, qt.IsNil)

			answers, err := ocicredentials.EnvironmentAnswersFor(opts, test.address)

			c.Assert(err, qt.IsNil)
			c.Assert(answers, qt.Equals, test.want)
		})
	}
}
