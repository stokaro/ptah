package testpki_test

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testpki"
)

// TestIssue_HappyPath verifies an issued certificate the way a TLS client
// does: against a pool of its authority, for each host it was issued for.
func TestIssue_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		hosts []string
		dial  string
	}{
		{name: "an IP address", hosts: []string{"127.0.0.1"}, dial: "127.0.0.1"},
		{name: "a DNS name", hosts: []string{"db.example.test"}, dial: "db.example.test"},
		{name: "the second of two hosts", hosts: []string{"db.example.test", "::1"}, dial: "::1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			authority, err := testpki.NewAuthority("testpki happy path")
			c.Assert(err, qt.IsNil)

			leaf, err := authority.Issue(test.hosts...)

			c.Assert(err, qt.IsNil)
			_, err = leaf.Certificate.Verify(x509.VerifyOptions{Roots: authority.Pool(), DNSName: test.dial})
			c.Assert(err, qt.IsNil)
			block, _ := pem.Decode(leaf.KeyPEM)
			c.Assert(block.Type, qt.Equals, "PRIVATE KEY")
			bundle, _ := pem.Decode(authority.CertificatePEM())
			c.Assert(bundle.Type, qt.Equals, "CERTIFICATE")
		})
	}
}

// TestIssue_FailurePath is the control for the test above: a certificate
// does not verify for a host it was not issued for, or against another
// authority.
func TestIssue_FailurePath(t *testing.T) {
	c := qt.New(t)
	authority, err := testpki.NewAuthority("testpki issuer")
	c.Assert(err, qt.IsNil)
	other, err := testpki.NewAuthority("testpki other")
	c.Assert(err, qt.IsNil)
	leaf, err := authority.Issue("127.0.0.1")
	c.Assert(err, qt.IsNil)

	_, err = leaf.Certificate.Verify(x509.VerifyOptions{Roots: authority.Pool(), DNSName: "127.0.0.2"})
	c.Assert(err, qt.ErrorAs, new(x509.HostnameError))
	_, err = leaf.Certificate.Verify(x509.VerifyOptions{Roots: other.Pool(), DNSName: "127.0.0.1"})
	c.Assert(err, qt.ErrorAs, new(x509.UnknownAuthorityError))

	empty, err := authority.Issue()
	c.Assert(err, qt.ErrorMatches, "issue a server certificate: name at least one host")
	c.Assert(empty, qt.DeepEquals, testpki.Leaf{})
}
