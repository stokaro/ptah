// Package testpki generates certificate authorities and server certificates
// for tests that exercise TLS verification.
//
// Every key is generated when a test asks for one and lives only in its
// memory and its temporary directory, so the repository carries no private
// key. The unit tests of internal/cabundle and internal/connector/mysqlconnect
// use it, and so does the live MySQL test under integration/, which installs a
// server certificate into a disposable server.
package testpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

// validity is how long every certificate here is valid, starting an hour in
// the past so a clock a little behind the test's still accepts it.
const validity = 24 * time.Hour

// Authority is a self-signed certificate authority.
type Authority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	der         []byte
}

// NewAuthority generates an authority whose subject is commonName.
func NewAuthority(commonName string) (*Authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate authority key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create authority certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse authority certificate: %w", err)
	}
	return &Authority{certificate: certificate, key: key, der: der}, nil
}

// CertificatePEM is the authority's certificate in PEM, the content of a CA
// bundle that trusts it.
func (a *Authority) CertificatePEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.der})
}

// Pool is a pool holding the authority's certificate alone.
func (a *Authority) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(a.certificate)
	return pool
}

// Leaf is a server certificate and its key.
type Leaf struct {
	// Certificate is the parsed certificate.
	Certificate *x509.Certificate
	// CertificatePEM is the certificate in PEM.
	CertificatePEM []byte
	// KeyPEM is the private key in PKCS #8 PEM.
	KeyPEM []byte
	// TLS is the pair as a TLS server presents it.
	TLS tls.Certificate
}

// Issue signs a server certificate valid for hosts: each is an IP address or
// a DNS name, and becomes a subject alternative name of that kind. The
// subject's common name is the first host, which no current verifier reads
// as a host name.
func (a *Authority) Issue(hosts ...string) (Leaf, error) {
	if len(hosts) == 0 {
		return Leaf{}, errors.New("issue a server certificate: name at least one host")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Leaf{}, fmt.Errorf("generate server key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return Leaf{}, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: hosts[0]},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
			continue
		}
		template.DNSNames = append(template.DNSNames, host)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.certificate, &key.PublicKey, a.key)
	if err != nil {
		return Leaf{}, fmt.Errorf("create server certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return Leaf{}, fmt.Errorf("parse server certificate: %w", err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Leaf{}, fmt.Errorf("encode server key: %w", err)
	}
	return Leaf{
		Certificate:    certificate,
		CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:         pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}),
		TLS:            tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: certificate},
	}, nil
}

// serialNumber is a random 128-bit serial, so two certificates of one test
// never share one.
func serialNumber() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}
	return serial, nil
}
