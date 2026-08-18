package ociartifact_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

// selfSignedPair writes a certificate and its key, and returns both paths. The
// certificate is real rather than a fixture string so the loader is exercised
// the way it will be in production: a PEM block that parses but is not a
// certificate would pass a substring check and fail here.
func selfSignedPair(c *qt.C) (certPath, keyPath string) {
	c.Helper()
	dir := c.TB.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.Assert(err, qt.IsNil)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ptah-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	c.Assert(err, qt.IsNil)

	certPath = filepath.Join(dir, "ca.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	c.Assert(os.WriteFile(certPath, certPEM, 0o600), qt.IsNil)

	keyDER, err := x509.MarshalECPrivateKey(key)
	c.Assert(err, qt.IsNil)
	keyPath = filepath.Join(dir, "key.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	c.Assert(os.WriteFile(keyPath, keyPEM, 0o600), qt.IsNil)
	return certPath, keyPath
}

func TestTransportFromEnvironment_ReadsAllThree(t *testing.T) {
	c := qt.New(t)
	t.Setenv(ociartifact.CAFileEnv, "  /etc/ptah/ca.pem  ")
	t.Setenv(ociartifact.ClientCertEnv, "/etc/ptah/client.pem")
	t.Setenv(ociartifact.ClientKeyEnv, "/etc/ptah/client.key")

	got := ociartifact.TransportFromEnvironment()

	c.Assert(got.CAFile, qt.Equals, "/etc/ptah/ca.pem")
	c.Assert(got.ClientCertFile, qt.Equals, "/etc/ptah/client.pem")
	c.Assert(got.ClientKeyFile, qt.Equals, "/etc/ptah/client.key")
}

func TestNewClient_AcceptsAnAdditionalAuthority(t *testing.T) {
	c := qt.New(t)
	certPath, _ := selfSignedPair(c)

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{
		Transport: ociartifact.TransportOptions{CAFile: certPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(client, qt.IsNotNil)
}

func TestNewClient_AcceptsAMutualTLSPair(t *testing.T) {
	c := qt.New(t)
	certPath, keyPath := selfSignedPair(c)

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{
		Transport: ociartifact.TransportOptions{ClientCertFile: certPath, ClientKeyFile: keyPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(client, qt.IsNotNil)
}

// TestNewClient_RefusesHalfACredential is the case worth pinning. Half a
// mutual-TLS credential authenticates nothing, and a run that dropped the
// missing half would fail later at the registry with an error about
// authorization rather than about the configuration that caused it.
func TestNewClient_RefusesHalfACredential(t *testing.T) {
	certPath, keyPath := "", ""
	t.Run("certificate without key", func(t *testing.T) {
		c := qt.New(t)
		certPath, _ = selfSignedPair(c)

		_, err := ociartifact.NewClient(ociartifact.ClientOptions{
			Transport: ociartifact.TransportOptions{ClientCertFile: certPath},
		})

		c.Assert(err, qt.ErrorMatches,
			`PTAH_OCI_CLIENT_CERT is set without PTAH_OCI_CLIENT_KEY: .*half of one authenticates nothing`)
	})

	t.Run("key without certificate", func(t *testing.T) {
		c := qt.New(t)
		_, keyPath = selfSignedPair(c)

		_, err := ociartifact.NewClient(ociartifact.ClientOptions{
			Transport: ociartifact.TransportOptions{ClientKeyFile: keyPath},
		})

		c.Assert(err, qt.ErrorMatches,
			`PTAH_OCI_CLIENT_KEY is set without PTAH_OCI_CLIENT_CERT: .*half of one authenticates nothing`)
	})
}

func TestNewClient_TransportFailurePath(t *testing.T) {
	t.Run("an absent authority bundle", func(t *testing.T) {
		c := qt.New(t)

		_, err := ociartifact.NewClient(ociartifact.ClientOptions{
			Transport: ociartifact.TransportOptions{CAFile: filepath.Join(c.TB.TempDir(), "missing.pem")},
		})

		c.Assert(err, qt.ErrorMatches, "read the OCI certificate authority bundle: .*")
	})

	t.Run("a bundle carrying no certificate", func(t *testing.T) {
		c := qt.New(t)
		path := filepath.Join(c.TB.TempDir(), "empty.pem")
		c.Assert(os.WriteFile(path, []byte("not a certificate\n"), 0o600), qt.IsNil)

		_, err := ociartifact.NewClient(ociartifact.ClientOptions{
			Transport: ociartifact.TransportOptions{CAFile: path},
		})

		c.Assert(err, qt.ErrorMatches, ".*contains no PEM certificate")
	})
}

// TestNewClient_UnconfiguredTransportIsTheDefault keeps the common path free of
// the new machinery: a run that configured nothing must not acquire a TLS
// configuration it did not ask for.
func TestNewClient_UnconfiguredTransportIsTheDefault(t *testing.T) {
	c := qt.New(t)
	t.Setenv(ociartifact.CAFileEnv, "")
	t.Setenv(ociartifact.ClientCertEnv, "")
	t.Setenv(ociartifact.ClientKeyEnv, "")

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(client, qt.IsNotNil)
}
