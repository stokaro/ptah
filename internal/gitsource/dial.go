package gitsource

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// The variables a pull reads its credential from. An environment value
// appears in neither shell history nor the process list, and there is no flag
// for a credential.
const (
	// UsernameEnv and PasswordEnv authenticate over HTTPS. A token goes in
	// PasswordEnv; the username then defaults to "git".
	UsernameEnv = "PTAH_GIT_USERNAME"
	PasswordEnv = "PTAH_GIT_PASSWORD" // #nosec G101 -- the name of a variable, not a credential
	// SSHKeyFileEnv names an unencrypted private key file for SSH.
	SSHKeyFileEnv = "PTAH_GIT_SSH_KEY_FILE"
)

const defaultUser = "git"

// Credentials authenticate a pull. The zero value is anonymous.
type Credentials struct {
	Username string
	Password string
	// SSHKey is a PEM private key.
	SSHKey []byte
}

func (c Credentials) present() bool {
	return c.Username != "" || c.Password != "" || len(c.SSHKey) > 0
}

// CredentialsFromEnvironment reads the credential variables through getenv,
// and the key file they name through readFile.
func CredentialsFromEnvironment(getenv func(string) string, readFile func(string) ([]byte, error)) (Credentials, error) {
	credentials := Credentials{Username: getenv(UsernameEnv), Password: getenv(PasswordEnv)}
	if file := getenv(SSHKeyFileEnv); file != "" {
		key, err := readFile(file)
		if err != nil {
			return Credentials{}, fmt.Errorf("read %s: %w", SSHKeyFileEnv, err)
		}
		credentials.SSHKey = key
	}
	return credentials, nil
}

// Trust pins whom a pull talks to.
type Trust struct {
	// CAFile, for HTTPS, holds the only roots the server's certificate may
	// chain to. Empty, the system's roots are used.
	CAFile string
	// KnownHostsFile, for SSH, holds the host keys the server may present.
	// SSH requires it: a pull never trusts a key on first use.
	KnownHostsFile string
}

// Dial returns the transport and authentication that reach a reference with
// the given credentials and trust.
func Dial(reference Reference, credentials Credentials, trust Trust) (transport.Transport, transport.AuthMethod, error) {
	switch reference.URL.Scheme {
	case "https":
		if len(credentials.SSHKey) > 0 {
			return nil, nil, fmt.Errorf("%w: %s is HTTPS, and %s is set", ErrReference, reference.Raw, SSHKeyFileEnv)
		}
		if trust.KnownHostsFile != "" {
			return nil, nil, fmt.Errorf("%w: %s is HTTPS, and known hosts are for SSH", ErrReference, reference.Raw)
		}
		return dialHTTPS(reference, credentials, trust)
	case "ssh":
		if credentials.Username != "" || credentials.Password != "" {
			return nil, nil, fmt.Errorf("%w: %s is SSH, and %s or %s is set", ErrReference, reference.Raw, UsernameEnv, PasswordEnv)
		}
		if trust.CAFile != "" {
			return nil, nil, fmt.Errorf("%w: %s is SSH, and a CA file is for HTTPS", ErrReference, reference.Raw)
		}
		return dialSSH(reference, credentials, trust)
	}
	return nil, nil, fmt.Errorf("%w: %s", ErrReference, reference.Raw)
}

func dialHTTPS(reference Reference, credentials Credentials, trust Trust) (transport.Transport, transport.AuthMethod, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if trust.CAFile != "" {
		pem, err := os.ReadFile(trust.CAFile) // #nosec G304 -- the person names the file
		if err != nil {
			return nil, nil, fmt.Errorf("read the CA file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, nil, fmt.Errorf("the CA file %s holds no certificate", trust.CAFile)
		}
		tlsConfig.RootCAs = roots
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.TLSClientConfig = tlsConfig
	client := &http.Client{Transport: base, CheckRedirect: redirects(reference, credentials)}
	var auth transport.AuthMethod
	if credentials.present() {
		username := credentials.Username
		if username == "" {
			username = defaultUser
		}
		auth = &githttp.BasicAuth{Username: username, Password: credentials.Password}
	}
	return githttp.NewClientWithOptions(client, &githttp.ClientOptions{RedirectPolicy: githttp.FollowInitialRedirects}), auth, nil
}

// redirects follows a redirect within the repository's origin, and refuses
// one that leaves it while a credential is present: the credential would
// either reach another server or be dropped without a word.
func redirects(reference Reference, credentials Credentials) func(*http.Request, []*http.Request) error {
	origin := reference.Origin()
	credentialed := credentials.present()
	return func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("%w: %s redirected more than 10 times", ErrRedirect, reference.Raw)
		}
		target := request.URL.Scheme + "://" + request.URL.Host
		if credentialed && !strings.EqualFold(target, origin) {
			return fmt.Errorf("%w: %s redirected to %s, another origin, with a credential set", ErrRedirect, reference.Raw, target)
		}
		return nil
	}
}

func dialSSH(reference Reference, credentials Credentials, trust Trust) (transport.Transport, transport.AuthMethod, error) {
	if trust.KnownHostsFile == "" {
		return nil, nil, fmt.Errorf("%w: %s; give the server's host keys with --known-hosts-file", ErrNoKnownHosts, reference.Raw)
	}
	if len(credentials.SSHKey) == 0 {
		return nil, nil, fmt.Errorf("%w: %s needs a key; name its file in %s", ErrReference, reference.Raw, SSHKeyFileEnv)
	}
	hostKeys, err := knownhosts.New(trust.KnownHostsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read the known hosts file: %w", err)
	}
	user := reference.URL.User.Username()
	if user == "" {
		user = defaultUser
	}
	auth, err := gitssh.NewPublicKeys(user, credentials.SSHKey, "")
	if err != nil {
		if _, missing := errors.AsType[*ssh.PassphraseMissingError](err); missing {
			return nil, nil, fmt.Errorf("%w: the key in %s is encrypted; give an unencrypted deploy key", ErrReference, SSHKeyFileEnv)
		}
		return nil, nil, fmt.Errorf("read the key in %s: %w", SSHKeyFileEnv, err)
	}
	auth.HostKeyCallback = hostKeys
	return gitssh.NewClient(nil), auth, nil
}
