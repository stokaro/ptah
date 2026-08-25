// Package ocicredentials resolves the registry credentials every OCI verb uses,
// and owns the store `ptah oci login` writes to.
//
// Ptah used to read credentials from exactly one place, Docker's credential
// configuration, on the stated premise that such a store "already exists on any
// machine that can pull an image". That premise does not hold for Ptah: pulling
// a Ptah artifact requires no Docker, so a deploy host or CI runner that only
// runs `ptah migrations up oci://...` has no reason to have Docker installed,
// and on such a machine there was no supported way to authenticate at all
// (stokaro/ptah#2241).
//
// The Docker store is still read, and still read the same way -- credential
// helpers included. Compatibility never removes a capability: a user who
// authenticates with `docker login` today sees no change.
package ocicredentials

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

const (
	// UsernameEnv and PasswordEnv carry a credential for a runner that should
	// not need a login step, a keychain, or a config file.
	//
	// An environment value appears in neither shell history nor the process
	// list, which is what the argv rule in ociartifact/transport.go is about.
	// A password on a command line lands in both, and there is still no flag
	// for one.
	UsernameEnv = "PTAH_OCI_USERNAME"
	PasswordEnv = "PTAH_OCI_PASSWORD" // #nosec G101 -- the name of a variable, not a credential

	// TokenEnv carries a registry identity token, which is how a token-issuing
	// registry authenticates without a username.
	TokenEnv = "PTAH_OCI_TOKEN" // #nosec G101 -- the name of a variable, not a credential

	// RegistryEnv scopes the environment credential to one host.
	//
	// Unset, the credential answers for every registry, which is what a CI job
	// with one registry in play wants. Set, it answers only for that host --
	// the setting to reach for when a job talks to more than one registry, so a
	// credential for one is not offered to the other.
	RegistryEnv = "PTAH_OCI_REGISTRY"

	// ConfigEnv overrides where Ptah's own credential file lives, which is what
	// a container or a second profile set uses.
	ConfigEnv = "PTAH_OCI_CONFIG"

	// StoreEnv set to "file" keeps the credential out of the platform keychain
	// and in Ptah's own file.
	//
	// A keychain is the better default and stays the default. This exists for
	// the cases where it is the wrong one: a shared desktop session, a machine
	// whose keychain prompts on every read, and Ptah's own tests, which must
	// not write a credential into the developer's keychain to check a file
	// format.
	StoreEnv = "PTAH_OCI_CREDENTIAL_STORE"

	// StoreFile is the StoreEnv value that selects the file.
	StoreFile = "file"
)

// dirName and fileName follow assistconfig: the home directory rather than a
// platform configuration directory, and `.ptah` rather than `.config/ptah`, so
// Ptah's state lives under one spelling.
const (
	dirName  = ".ptah"
	fileName = "registries.json"
)

// Options lets a test supply the environment and home directory.
type Options struct {
	Getenv  func(string) string
	HomeDir func() (string, error)
}

func (o Options) getenv(name string) string {
	if o.Getenv != nil {
		return o.Getenv(name)
	}
	return os.Getenv(name)
}

// fileStoreRequested reports whether the caller asked to skip the keychain.
func (o Options) fileStoreRequested() bool {
	return strings.EqualFold(strings.TrimSpace(o.getenv(StoreEnv)), StoreFile)
}

func (o Options) homeDir() (string, error) {
	if o.HomeDir != nil {
		return o.HomeDir()
	}
	return os.UserHomeDir()
}

// DefaultPath returns the file `ptah oci login` writes to.
func DefaultPath(opts Options) (string, error) {
	if explicit := strings.TrimSpace(opts.getenv(ConfigEnv)); explicit != "" {
		return explicit, nil
	}
	home, err := opts.homeDir()
	if err != nil {
		return "", fmt.Errorf("resolve the home directory: %w", err)
	}
	return filepath.Join(home, dirName, fileName), nil
}

// PtahStore opens the store `ptah oci login` writes to and `logout` clears.
//
// A native OS credential helper is preferred over the file: DetectDefaultNativeStore
// picks the platform's keychain when the config carries no authentication of
// its own. Plaintext is permitted as the fallback rather than refused, because
// a machine with no keychain -- a container, a minimal runner -- is exactly the
// one this feature exists for; [DescribeStorage] is how the command tells the
// user which of the two happened.
func PtahStore(opts Options) (*credentials.DynamicStore, error) {
	if opts.fileStoreRequested() {
		return FileStore(opts)
	}
	path, err := DefaultPath(opts)
	if err != nil {
		return nil, err
	}
	store, err := credentials.NewStore(path, credentials.StoreOptions{
		AllowPlaintextPut:        true,
		DetectDefaultNativeStore: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open the Ptah credential store at %s: %w", path, err)
	}
	return store, nil
}

// Store returns the chain every OCI verb resolves a registry challenge through:
// the environment, then Ptah's own store, then Docker's.
//
// The order is the order of explicitness. An environment value was set for this
// process; a Ptah store entry was written by `ptah oci login`; the Docker store
// is whatever the machine already had. A later entry answers only when the ones
// before it hold nothing for that registry, so adding the first two cannot take
// away a credential that works today.
func Store(opts Options) (credentials.Store, error) {
	ptahStore, err := PtahStore(opts)
	if err != nil {
		return nil, err
	}
	dockerStore, err := credentials.NewStoreFromDocker(credentials.StoreOptions{})
	if err != nil {
		return nil, fmt.Errorf("open Docker credential store: %w", err)
	}
	return chain{
		read:  credentials.NewStoreWithFallbacks(environmentStore{opts: opts}, ptahStore, dockerStore),
		write: ptahStore,
	}, nil
}

// chain reads from three places and writes to one.
//
// oras-go's own NewStoreWithFallbacks sends every write to the PRIMARY store,
// which here is the environment -- so a caller that wrote through the chain
// would be refused even with no environment credential set, and the refusal
// would name variables it had not set. Reads fall through the three; writes go
// where `ptah oci login` puts them, which is the only store Ptah owns.
type chain struct {
	read  credentials.Store
	write credentials.Store
}

func (c chain) Get(ctx context.Context, serverAddress string) (auth.Credential, error) {
	return c.read.Get(ctx, serverAddress)
}

func (c chain) Put(ctx context.Context, serverAddress string, cred auth.Credential) error {
	return c.write.Put(ctx, serverAddress, cred)
}

func (c chain) Delete(ctx context.Context, serverAddress string) error {
	return c.write.Delete(ctx, serverAddress)
}

// environmentStore answers from PTAH_OCI_USERNAME/PASSWORD/TOKEN.
//
// It is read-only. Writing a credential into the environment of a process that
// has already started is not something a store can do, and pretending otherwise
// would make `ptah oci login` report a success that persisted nothing.
type environmentStore struct {
	opts Options
}

func (s environmentStore) Get(_ context.Context, serverAddress string) (auth.Credential, error) {
	cred := EnvironmentCredential(s.opts)
	if cred == auth.EmptyCredential {
		return auth.EmptyCredential, nil
	}
	if scope := strings.TrimSpace(s.opts.getenv(RegistryEnv)); scope != "" &&
		!strings.EqualFold(credentials.ServerAddressFromHostname(scope), serverAddress) &&
		!strings.EqualFold(scope, serverAddress) {
		return auth.EmptyCredential, nil
	}
	return cred, nil
}

func (s environmentStore) Put(_ context.Context, _ string, _ auth.Credential) error {
	return fmt.Errorf("%s and %s are read-only: unset them and run `ptah oci login` to store a credential",
		UsernameEnv, PasswordEnv)
}

func (s environmentStore) Delete(_ context.Context, _ string) error {
	return fmt.Errorf("%s and %s are read-only: unset them rather than running `ptah oci logout`",
		UsernameEnv, PasswordEnv)
}

// EnvironmentCredential returns the credential the environment describes, or
// the empty credential when it describes none.
func EnvironmentCredential(opts Options) auth.Credential {
	username := strings.TrimSpace(opts.getenv(UsernameEnv))
	password := opts.getenv(PasswordEnv)
	token := strings.TrimSpace(opts.getenv(TokenEnv))

	switch {
	case token != "":
		// An identity token authenticates on its own; a registry that issues
		// one does not want a username beside it.
		return auth.Credential{RefreshToken: token}
	case username != "" && password != "":
		return auth.Credential{Username: username, Password: password}
	default:
		// A username with no password, or a password with no username, is a
		// half-configured runner. Answering with it would send a credential the
		// registry cannot accept and report the failure as a registry problem.
		return auth.EmptyCredential
	}
}

// EnvironmentNames lists the variables that carry a secret, so a caller that
// publishes an environment can redact them.
func EnvironmentNames() []string {
	return []string{UsernameEnv, PasswordEnv, TokenEnv, RegistryEnv, ConfigEnv, StoreEnv}
}

// SecretEnvironmentNames lists the subset whose VALUE is a secret.
func SecretEnvironmentNames() []string {
	return []string{PasswordEnv, TokenEnv}
}

// FileStore opens Ptah's credential file WITHOUT consulting a platform helper.
//
// It exists as the fallback for [Save]: a container, a headless CI runner, or a
// desktop session that cannot reach its own keychain will have a helper
// detected and then refused by it, and refusing to store anything there would
// leave exactly the machines this feature is for with no way to authenticate.
func FileStore(opts Options) (*credentials.DynamicStore, error) {
	path, err := DefaultPath(opts)
	if err != nil {
		return nil, err
	}
	store, err := credentials.NewStore(path, credentials.StoreOptions{
		AllowPlaintextPut:        true,
		DetectDefaultNativeStore: false,
	})
	if err != nil {
		return nil, fmt.Errorf("open the Ptah credential store at %s: %w", path, err)
	}
	return store, nil
}

// Storage reports where a credential was written.
type Storage struct {
	// Path is Ptah's credential file.
	Path string
	// Plaintext reports that the credential is in the file rather than in a
	// platform credential helper.
	Plaintext bool
	// HelperError is why the platform helper was not used, when one was
	// detected and refused. It is reported rather than swallowed: a keychain
	// that stopped accepting writes is worth saying out loud, even though the
	// credential was stored anyway.
	HelperError error
}

// Save stores a credential, preferring a platform credential helper.
//
// The helper is tried first and the file is the fallback. Both outcomes are
// acceptable on the machines this feature exists for, and they are not the same
// thing to the person running the command, so which one happened is returned
// rather than left for them to discover by opening the file.
func Save(ctx context.Context, opts Options, serverAddress string, cred auth.Credential) (Storage, error) {
	path, err := DefaultPath(opts)
	if err != nil {
		return Storage{}, err
	}

	native, err := PtahStore(opts)
	if err != nil {
		return Storage{}, err
	}
	helperErr := native.Put(ctx, serverAddress, cred)
	if helperErr == nil {
		return Storage{Path: path, Plaintext: !usesHelper(path, serverAddress)}, nil
	}

	file, err := FileStore(opts)
	if err != nil {
		return Storage{}, err
	}
	if err := file.Put(ctx, serverAddress, cred); err != nil {
		return Storage{}, fmt.Errorf(
			"store the credential for %s: the platform credential helper refused (%w) and the file did not accept it either: %w",
			serverAddress, helperErr, err)
	}
	return Storage{Path: path, Plaintext: true, HelperError: helperErr}, nil
}

// usesHelper reports whether the stored credential went to a helper rather than
// into the file, read back from what was actually written.
func usesHelper(path, serverAddress string) bool {
	raw, err := os.ReadFile(path) // #nosec G304 -- the path Save just wrote
	if err != nil {
		return false
	}
	var config struct {
		CredsStore  string            `json:"credsStore"`
		CredHelpers map[string]string `json:"credHelpers"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return false
	}
	return config.CredHelpers[serverAddress] != "" || config.CredsStore != ""
}
