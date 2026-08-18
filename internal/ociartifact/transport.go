package ociartifact

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

const (
	// CAFileEnv names a PEM bundle to trust in addition to the system roots.
	// This is what an internal Harbor or a private registry with its own
	// authority needs, and it is the one piece of transport configuration
	// whose absence turns every other OCI verb into a TLS error.
	CAFileEnv = "PTAH_OCI_CA_FILE"
	// ClientCertEnv names the PEM certificate presented to a registry that
	// authenticates its clients with mutual TLS.
	ClientCertEnv = "PTAH_OCI_CLIENT_CERT"
	// ClientKeyEnv names the private key for ClientCertEnv.
	ClientKeyEnv = "PTAH_OCI_CLIENT_KEY"
)

// TransportOptions configures how every OCI consumer reaches its registry.
//
// There is deliberately no password here, and there will not be one. A
// credential passed on a command line lands in shell history and in the process
// list of every user on the machine, and a registry password is exactly the
// secret that must not. Credentials come from the Docker credential store,
// which already exists on any machine that can pull an image.
type TransportOptions struct {
	// CAFile is a PEM bundle trusted in addition to the system roots.
	CAFile string
	// ClientCertFile and ClientKeyFile are the mutual-TLS pair. Naming one
	// without the other is refused rather than silently ignored: a run that
	// dropped half a credential would fail later, at the registry, with an
	// error about authorization rather than about configuration.
	ClientCertFile string
	ClientKeyFile  string
}

// TransportFromEnvironment reads the transport configuration a run inherits.
func TransportFromEnvironment() TransportOptions {
	return TransportOptions{
		CAFile:         strings.TrimSpace(os.Getenv(CAFileEnv)),
		ClientCertFile: strings.TrimSpace(os.Getenv(ClientCertEnv)),
		ClientKeyFile:  strings.TrimSpace(os.Getenv(ClientKeyEnv)),
	}
}

// configured reports whether anything was asked for.
func (t TransportOptions) configured() bool {
	return t.CAFile != "" || t.ClientCertFile != "" || t.ClientKeyFile != ""
}

// tlsConfig builds the TLS configuration, or returns nil when the defaults are
// what was asked for.
func (t TransportOptions) tlsConfig() (*tls.Config, error) {
	if !t.configured() {
		return nil, nil
	}
	if (t.ClientCertFile == "") != (t.ClientKeyFile == "") {
		missing, named := ClientKeyEnv, ClientCertEnv
		if t.ClientCertFile == "" {
			missing, named = ClientCertEnv, ClientKeyEnv
		}
		return nil, fmt.Errorf(
			"%s is set without %s: a client certificate and its key are one credential, "+
				"and half of one authenticates nothing", named, missing)
	}

	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CAFile != "" {
		pool, err := systemPoolWith(t.CAFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = pool
	}
	if t.ClientCertFile != "" {
		pair, err := tls.LoadX509KeyPair(t.ClientCertFile, t.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load the OCI client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return config, nil
}

// systemPoolWith adds a PEM bundle to a copy of the system roots.
//
// The additional authority is added to the system pool rather than replacing
// it, because an operator naming their internal authority is saying "trust this
// as well". Replacing the pool would break every other registry the same run
// talks to, and would do it as a TLS failure that names no cause.
func systemPoolWith(path string) (*x509.CertPool, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the OCI certificate authority bundle: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		// A platform with no system pool is not a reason to refuse the
		// operator's own authority; it is a reason to trust only that one.
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(contents) {
		return nil, fmt.Errorf(
			"the OCI certificate authority bundle %s contains no PEM certificate", path)
	}
	return pool, nil
}
