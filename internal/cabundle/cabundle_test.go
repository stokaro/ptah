package cabundle_test

import (
	"bytes"
	"crypto/x509"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cabundle"
	"ptah.run/internal/testpki"
)

// authority generates a certificate authority and a server certificate it
// issued for 127.0.0.1.
func authority(c *qt.C, name string) (*testpki.Authority, *x509.Certificate) {
	c.Helper()
	ca, err := testpki.NewAuthority(name)
	c.Assert(err, qt.IsNil)
	leaf, err := ca.Issue("127.0.0.1")
	c.Assert(err, qt.IsNil)
	return ca, leaf.Certificate
}

// writeBundle writes content to a file in a new temporary directory.
func writeBundle(c *qt.C, content []byte) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ca.pem")
	c.Assert(os.WriteFile(path, content, 0o600), qt.IsNil)
	return path
}

// verifies reports whether a pool accepts a certificate issued for 127.0.0.1.
func verifies(pool *x509.CertPool, leaf *x509.Certificate) error {
	_, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: "127.0.0.1"})
	return err
}

// TestLoad_HappyPath loads bundles in the shapes distributions ship them and
// asks the pool the question a TLS client asks: does it verify a server
// certificate the bundle's authority issued.
func TestLoad_HappyPath(t *testing.T) {
	t.Run("one certificate", func(t *testing.T) {
		c := qt.New(t)
		ca, leaf := authority(c, "cabundle one")

		pool, err := cabundle.Load(writeBundle(c, ca.CertificatePEM()))

		c.Assert(err, qt.IsNil)
		c.Assert(verifies(pool, leaf), qt.IsNil)
	})

	t.Run("every certificate of a bundle with comments between them", func(t *testing.T) {
		c := qt.New(t)
		first, firstLeaf := authority(c, "cabundle first")
		second, secondLeaf := authority(c, "cabundle second")
		content := bytes.Join([][]byte{
			[]byte("# Issuer: CN=cabundle first\n"),
			first.CertificatePEM(),
			[]byte("\n# Issuer: CN=cabundle second\n"),
			second.CertificatePEM(),
			[]byte("# trailing comment\n"),
		}, nil)

		pool, err := cabundle.Load(writeBundle(c, content))

		c.Assert(err, qt.IsNil)
		c.Assert(verifies(pool, firstLeaf), qt.IsNil)
		c.Assert(verifies(pool, secondLeaf), qt.IsNil)
	})

	t.Run("a relative path from the working directory", func(t *testing.T) {
		c := qt.New(t)
		ca, leaf := authority(c, "cabundle relative")
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "ca.pem"), ca.CertificatePEM(), 0o600), qt.IsNil)
		t.Chdir(dir)

		pool, err := cabundle.Load("ca.pem")

		c.Assert(err, qt.IsNil)
		c.Assert(verifies(pool, leaf), qt.IsNil)
	})
}

// TestLoad_TrustsTheBundleAlone is the control for the tests above: the pool
// holds the bundle's certificates and nothing else, so a server certificate
// of another authority does not verify against it.
func TestLoad_TrustsTheBundleAlone(t *testing.T) {
	c := qt.New(t)
	ca, _ := authority(c, "cabundle trusted")
	_, otherLeaf := authority(c, "cabundle other")

	pool, err := cabundle.Load(writeBundle(c, ca.CertificatePEM()))

	c.Assert(err, qt.IsNil)
	c.Assert(verifies(pool, otherLeaf), qt.ErrorAs, new(x509.UnknownAuthorityError))
}

// TestLoad_FailurePath refuses every file that is not a usable bundle, and
// names the absolute path in the refusal.
func TestLoad_FailurePath(t *testing.T) {
	c := qt.New(t)
	ca, _ := authority(c, "cabundle failure")
	leaf, err := ca.Issue("127.0.0.1")
	c.Assert(err, qt.IsNil)
	certificate := ca.CertificatePEM()
	// The body of a certificate block cut in half, so the block's base64
	// still decodes but its DER does not parse.
	halfCertificate := append([]byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"), certificate...)

	tests := []struct {
		name    string
		content []byte
		wantIs  error
		wantErr string
	}{
		{
			name:    "an empty file",
			content: nil,
			wantIs:  cabundle.ErrNoCertificate,
			wantErr: `CA bundle .*ca\.pem holds no PEM certificate; .*`,
		},
		{
			name:    "a certificate in DER rather than PEM",
			content: leaf.Certificate.Raw,
			wantIs:  cabundle.ErrNoCertificate,
			wantErr: `CA bundle .*ca\.pem holds no PEM certificate; .*`,
		},
		{
			name:    "a private key beside the certificate",
			content: slices.Concat(certificate, leaf.KeyPEM),
			wantIs:  cabundle.ErrUnexpectedBlock,
			wantErr: `CA bundle .*ca\.pem holds a PEM block that is not a certificate: block 2 is "PRIVATE KEY", and the bundle takes CA certificates only`,
		},
		{
			name:    "a certificate block that does not parse, before a good one",
			content: halfCertificate,
			wantIs:  cabundle.ErrMalformed,
			wantErr: `CA bundle .*ca\.pem holds a certificate block that cannot be read: block 1: x509: .*`,
		},
		{
			name:    "a block with no END line, before a good one",
			content: append([]byte("-----BEGIN CERTIFICATE-----\nMIIB\n"), certificate...),
			wantIs:  cabundle.ErrMalformed,
			wantErr: `CA bundle .*ca\.pem holds a certificate block that cannot be read: block 1 is not a complete PEM block`,
		},
		{
			name:    "a block whose base64 is damaged, after a good one",
			content: slices.Concat(certificate, []byte("-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n")),
			wantIs:  cabundle.ErrMalformed,
			wantErr: `CA bundle .*ca\.pem holds a certificate block that cannot be read: block 2 is not a complete PEM block`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pool, err := cabundle.Load(writeBundle(c, test.content))

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(pool, qt.IsNil)
		})
	}
}

// TestLoad_FailurePath_RepeatsNoKey keeps a private key placed in a bundle by
// mistake out of the refusal, which an operator pastes into a ticket.
func TestLoad_FailurePath_RepeatsNoKey(t *testing.T) {
	c := qt.New(t)
	ca, err := testpki.NewAuthority("cabundle key")
	c.Assert(err, qt.IsNil)
	leaf, err := ca.Issue("127.0.0.1")
	c.Assert(err, qt.IsNil)
	keyBody := bytes.Split(leaf.KeyPEM, []byte("\n"))[1]

	_, err = cabundle.Load(writeBundle(c, leaf.KeyPEM))

	c.Assert(err, qt.ErrorIs, cabundle.ErrUnexpectedBlock)
	c.Assert(err.Error(), qt.Not(qt.Contains), string(keyBody))
}

// TestLoad_FailurePath_File refuses a path that names no readable regular
// file, with the operating system's reason where it has one.
func TestLoad_FailurePath_File(t *testing.T) {
	t.Run("a missing file", func(t *testing.T) {
		c := qt.New(t)
		missing := filepath.Join(c.TempDir(), "missing.pem")

		pool, err := cabundle.Load(missing)

		c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
		c.Assert(err, qt.ErrorMatches, `read CA bundle: open .*missing\.pem: .*`)
		c.Assert(pool, qt.IsNil)
	})

	t.Run("a directory", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()

		pool, err := cabundle.Load(dir)

		c.Assert(err, qt.ErrorIs, cabundle.ErrNotRegularFile)
		c.Assert(pool, qt.IsNil)
	})

	t.Run("a file larger than the limit", func(t *testing.T) {
		c := qt.New(t)
		path := filepath.Join(c.TempDir(), "large.pem")
		c.Assert(os.WriteFile(path, bytes.Repeat([]byte("#"), cabundle.MaxSize+1), 0o600), qt.IsNil)

		pool, err := cabundle.Load(path)

		c.Assert(err, qt.ErrorIs, cabundle.ErrTooLarge)
		c.Assert(pool, qt.IsNil)
	})
}
