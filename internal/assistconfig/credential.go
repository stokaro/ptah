package assistconfig

import (
	"fmt"
	"os"
	"strings"
)

// Credential reference kinds.
const (
	// CredentialEnv reads an environment variable: `env:OPENAI_API_KEY`.
	CredentialEnv = "env"
	// CredentialFile reads a file: `file:/run/secrets/openai`.
	CredentialFile = "file"
)

// credentialRef is a parsed reference.
type credentialRef struct {
	kind  string
	value string
}

// parseCredentialRef reads `kind:value`, refusing anything else.
//
// A bare value is refused rather than treated as a literal key. Accepting one
// would make `credential: sk-...` work, and the first person it works for has
// put their key in a file they will later paste into an issue.
func parseCredentialRef(raw string) (credentialRef, error) {
	kind, value, found := strings.Cut(raw, ":")
	if !found {
		return credentialRef{}, fmt.Errorf(
			"%w: %q is not a reference; write env:NAME or file:PATH. "+
				"Ptah does not accept a key in configuration",
			ErrCredential, raw)
	}
	switch kind {
	case CredentialEnv, CredentialFile:
	default:
		return credentialRef{}, fmt.Errorf(
			"%w: unknown reference kind %q; want env or file", ErrCredential, kind)
	}
	if value == "" {
		return credentialRef{}, fmt.Errorf("%w: %q names nothing", ErrCredential, raw)
	}
	return credentialRef{kind: kind, value: value}, nil
}

// resolveCredential reads the value a reference names.
//
// An empty reference is not an error: a model server on this machine usually
// wants no credential at all, and requiring a placeholder would be requiring a
// secret that does not exist.
func resolveCredential(raw string, opts Options) (string, error) {
	if raw == "" {
		return "", nil
	}
	ref, err := parseCredentialRef(raw)
	if err != nil {
		return "", err
	}
	switch ref.kind {
	case CredentialEnv:
		value, set := opts.lookup(ref.value)
		if !set {
			return "", fmt.Errorf("%w: %s is not set in this environment", ErrCredential, ref.value)
		}
		if value == "" {
			// An exported empty value is a configuration error rather than a
			// missing credential, for the reason internal/envbool gives about
			// boolean variables: the four states are distinguishable and a
			// typo in a CI environment file should not read as "unset".
			return "", fmt.Errorf("%w: %s is set to an empty value", ErrCredential, ref.value)
		}
		return value, nil
	case CredentialFile:
		return readCredentialFile(ref.value)
	}
	return "", fmt.Errorf("%w: unknown reference kind %q", ErrCredential, ref.kind)
}

// readCredentialFile reads a credential from disk, refusing one other users can
// read.
//
// The refusal is the same one ssh makes about a private key, and for the same
// reason: a file mode is the only thing standing between a shared machine and
// the key, and silently using it teaches nobody.
func readCredentialFile(path string) (string, error) {
	if err := refuseWorldReadable(path); err != nil {
		return "", fmt.Errorf("%w: %w", ErrCredential, err)
	}
	raw, err := os.ReadFile(path) // #nosec G304 -- the path is the operator's own credential file
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrCredential, err)
	}
	// A trailing newline is what every editor and every `echo` adds, and a
	// credential sent with one is rejected by the provider as an invalid key --
	// a failure whose message points at the key rather than at the newline.
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" {
		return "", fmt.Errorf("%w: %s is empty", ErrCredential, path)
	}
	return value, nil
}

// refuseWorldReadable rejects a file whose mode lets group or other read it.
//
// Windows file modes do not carry these bits, so the check is a no-op there
// rather than a tautology dressed as a check: os.Stat reports 0o666 or 0o444
// for every file, and asserting on that would be asserting nothing. Saying so
// here is the point -- a platform-conditional check that quietly passes is how
// a test comes to assert that a file holding a password is writable.
func refuseWorldReadable(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !modeBitsAreMeaningful() {
		return nil
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf("%w: %s is %#o; chmod 600 it", ErrInsecureFile, path, mode)
	}
	return nil
}
