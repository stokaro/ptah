// Package cabundle reads a PEM file of certificate authorities into a pool a
// TLS client verifies a server against.
//
// It is the loader behind the `ssl-ca` parameter of a MySQL-family database
// URL (internal/connector/mysqlconnect), and it knows nothing about databases:
// it reads one file, checks every block in it, and returns a pool that holds
// those certificates and nothing else. The process trust store is neither read
// nor changed.
//
// A bundle is refused rather than read in part. x509.CertPool.AppendCertsFromPEM
// skips what it cannot parse and reports only whether anything was added, so a
// bundle whose second certificate is damaged would verify against the first
// alone, and the operator would learn about it from a handshake failure against
// a server signed by the second.
package cabundle

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// MaxSize is the largest bundle Load reads. A bundle of every public root
// authority is a few hundred kilobytes, so the limit only stops a path that
// names a device or a file that is not a bundle at all.
const MaxSize = 4 << 20

// The errors Load returns for a file that is not a usable bundle. Each is
// wrapped with the path it is about; an error opening or reading the file is
// the operating system's, wrapped the same way.
var (
	// ErrNotRegularFile reports a path that names a directory, a device or
	// a pipe.
	ErrNotRegularFile = errors.New("is not a regular file")
	// ErrTooLarge reports a file larger than [MaxSize].
	ErrTooLarge = fmt.Errorf("is larger than %d bytes", MaxSize)
	// ErrNoCertificate reports a file that holds no PEM block at all, such
	// as an empty file or a certificate in DER form.
	ErrNoCertificate = errors.New(`holds no PEM certificate; a bundle is one or more "-----BEGIN CERTIFICATE-----" blocks`)
	// ErrUnexpectedBlock reports a PEM block that is not a certificate, such
	// as a private key.
	ErrUnexpectedBlock = errors.New("holds a PEM block that is not a certificate")
	// ErrMalformed reports a certificate block that does not parse, or a
	// block with no end line.
	ErrMalformed = errors.New("holds a certificate block that cannot be read")
)

// certificateBlock is the PEM type of a certificate.
const certificateBlock = "CERTIFICATE"

// beginMarker opens every PEM block.
var beginMarker = []byte("-----BEGIN")

// Load reads the PEM bundle at path and returns a pool of its certificates.
//
// A relative path is read from the working directory. Every PEM block must be a
// certificate that parses, and at least one must be present; text between
// blocks, such as the comments a distribution bundle carries, is ignored. The
// error names the path as an absolute one and never quotes the file's
// contents, which a misplaced private key would put in a log.
func Load(path string) (*x509.CertPool, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve CA bundle path: %w", err)
	}
	data, err := readBundle(absolute)
	if err != nil {
		return nil, err
	}
	return parseBundle(absolute, data)
}

// readBundle reads at most MaxSize bytes of a regular file.
func readBundle(path string) ([]byte, error) {
	// #nosec G304 -- the path is the operator's own CA bundle; reading it is the purpose.
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("CA bundle %s %w", path, ErrNotRegularFile)
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxSize+1))
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	if len(data) > MaxSize {
		return nil, fmt.Errorf("CA bundle %s %w", path, ErrTooLarge)
	}
	return data, nil
}

// parseBundle adds every certificate in data to a new pool, refusing the
// bundle at the first block that is not one.
//
// The blocks are found by their BEGIN lines rather than by pem.Decode alone.
// pem.Decode passes over a block it cannot decode, one with no END line or
// with damaged base64, and returns the next block it can: decoding from the
// start of the file would load the rest of the bundle without the damaged
// certificate and say nothing.
func parseBundle(path string, data []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	count := 0
	rest := data
	for {
		start := bytes.Index(rest, beginMarker)
		if start < 0 {
			break
		}
		count++
		rest = rest[start:]
		block, remaining := pem.Decode(rest)
		consumed := rest[:len(rest)-len(remaining)]
		if block == nil || bytes.Contains(consumed[len(beginMarker):], beginMarker) {
			return nil, fmt.Errorf("CA bundle %s %w: block %d is not a complete PEM block", path, ErrMalformed, count)
		}
		rest = remaining
		if block.Type != certificateBlock {
			return nil, fmt.Errorf("CA bundle %s %w: block %d is %q, and the bundle takes CA certificates only",
				path, ErrUnexpectedBlock, count, block.Type)
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("CA bundle %s %w: block %d: %w", path, ErrMalformed, count, err)
		}
		pool.AddCert(certificate)
	}
	if count == 0 {
		return nil, fmt.Errorf("CA bundle %s %w", path, ErrNoCertificate)
	}
	return pool, nil
}
