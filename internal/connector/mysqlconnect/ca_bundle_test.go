package mysqlconnect_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/cabundle"
	"ptah.run/internal/testpki"
)

// The server here speaks just enough of the MySQL protocol for go-sql-driver
// to negotiate TLS with it: a handshake offering TLS, the TLS handshake, and
// an access-denied answer to the login that follows. A connection that gets
// that answer has verified the server's certificate, so the error each test
// reads says how far the connection got. The live server is in integration/.

// password is the fixture URL's password, which no refusal may repeat.
const password = "Pa5wordX9" // #nosec G101 -- a made-up password for a fake server

// accessDenied is the error number the fake answers a login with.
const accessDenied = 1045

// fakeServer is a loopback server that presents one certificate.
type fakeServer struct {
	address string
	// accepted counts the connections the server accepted.
	accepted atomic.Int32
	// handshakes counts the TLS handshakes a client completed.
	handshakes atomic.Int32
}

// startFakeServer starts a server presenting certificate until the test ends.
func startFakeServer(c *qt.C, certificate tls.Certificate) *fakeServer {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	server := &fakeServer{address: listener.Addr().String()}
	config := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	var served sync.WaitGroup
	served.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.accepted.Add(1)
			served.Go(func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				server.serve(conn, config)
			})
		}
	})
	c.Cleanup(func() {
		_ = listener.Close()
		served.Wait()
	})
	return server
}

// MySQL capability flags the handshake announces, in its two 16-bit halves.
const (
	clientMySQL       uint16 = 1 << 0
	clientProtocol41  uint16 = 1 << 9
	clientSSL         uint16 = 1 << 11
	clientSecureConn  uint16 = 1 << 15
	clientPluginAuth  uint16 = 1 << (19 - 16)
	sslRequestPayload        = 32
)

// serve offers TLS, completes the handshake, and refuses the login.
func (s *fakeServer) serve(conn net.Conn, config *tls.Config) {
	if writePacket(conn, 0, handshakePacket()) != nil {
		return
	}
	request := make([]byte, 4+sslRequestPayload)
	if _, err := io.ReadFull(conn, request); err != nil {
		return
	}
	server := tls.Server(conn, config)
	if server.Handshake() != nil {
		return
	}
	s.handshakes.Add(1)
	header := make([]byte, 4)
	if _, err := io.ReadFull(server, header); err != nil {
		return
	}
	length := binary.LittleEndian.Uint32(append(header[:3:3], 0))
	if _, err := io.CopyN(io.Discard, server, int64(length)); err != nil {
		return
	}
	if writePacket(server, header[3]+1, errorPacket(accessDenied)) != nil {
		return
	}
	_, _ = io.Copy(io.Discard, server)
}

// handshakePacket is a protocol 10 initial handshake that offers TLS.
func handshakePacket() []byte {
	payload := []byte{10}
	payload = append(payload, "8.4.0-fake"...)
	payload = append(payload, 0)
	payload = binary.LittleEndian.AppendUint32(payload, 1)
	payload = append(payload, "abcdefgh"...)
	payload = append(payload, 0)
	payload = binary.LittleEndian.AppendUint16(payload, clientMySQL|clientProtocol41|clientSecureConn|clientSSL)
	payload = append(payload, 255)
	payload = binary.LittleEndian.AppendUint16(payload, 2)
	payload = binary.LittleEndian.AppendUint16(payload, clientPluginAuth)
	payload = append(payload, 21)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, "ijklmnopqrst"...)
	payload = append(payload, 0)
	payload = append(payload, "mysql_native_password"...)
	return append(payload, 0)
}

// errorPacket is an error packet with number and a placeholder SQLSTATE.
func errorPacket(number uint16) []byte {
	payload := []byte{0xff}
	payload = binary.LittleEndian.AppendUint16(payload, number)
	payload = append(payload, "#28000"...)
	return append(payload, "refused by the fake server"...)
}

// writePacket frames a payload of a few dozen bytes.
func writePacket(conn io.Writer, sequence byte, payload []byte) error {
	length := uint32(len(payload)) // #nosec G115 -- every payload here is a few dozen bytes
	if length >= 1<<24 {
		return errors.New("packet too large")
	}
	header := binary.LittleEndian.AppendUint32(nil, length|uint32(sequence)<<24)
	_, err := conn.Write(append(header, payload...))
	return err
}

// newAuthority generates an authority and writes its certificate as a bundle.
func newAuthority(c *qt.C, name string) (authority *testpki.Authority, bundle string) {
	c.Helper()
	authority, err := testpki.NewAuthority(name)
	c.Assert(err, qt.IsNil)
	bundle = filepath.Join(c.TempDir(), "ca.pem")
	c.Assert(os.WriteFile(bundle, authority.CertificatePEM(), 0o600), qt.IsNil)
	return authority, bundle
}

// issue signs a server certificate for hosts.
func issue(c *qt.C, authority *testpki.Authority, hosts ...string) tls.Certificate {
	c.Helper()
	leaf, err := authority.Issue(hosts...)
	c.Assert(err, qt.IsNil)
	return leaf.TLS
}

// connect opens a connection the way every command does.
func connect(c *qt.C, rawURL string) error {
	c.Helper()
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	if conn != nil {
		_ = conn.Close()
	}
	return err
}

// caURL is a URL of the fake server at address with ssl-ca naming bundle.
func caURL(scheme, address, bundle, extra string) string {
	return scheme + "://app:" + password + "@" + address + "/shop?" + extra + "ssl-ca=" + url.QueryEscape(bundle)
}

// TestConnect_CABundle_HappyPath connects with the bundle that signed the
// server's certificate. The server's refusal of the login is the outcome: it
// arrives over the TLS connection the client verified.
func TestConnect_CABundle_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		scheme string
		form   func(address string) string
		extra  string
	}{
		{name: "URL form", scheme: "mysql", form: func(address string) string { return address }},
		{name: "URL form beside tls=true", scheme: "mariadb", form: func(address string) string { return address }, extra: "tls=true&"},
		{name: "driver form", scheme: "maria", form: func(address string) string { return "tcp(" + address + ")" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			authority, bundle := newAuthority(c, "mysqlconnect happy path")
			server := startFakeServer(c, issue(c, authority, "127.0.0.1"))

			err := connect(c, caURL(test.scheme, test.form(server.address), bundle, test.extra))

			var refused *mysqldriver.MySQLError
			c.Assert(err, qt.ErrorAs, &refused)
			c.Assert(refused.Number, qt.Equals, uint16(accessDenied))
			c.Assert(server.handshakes.Load(), qt.Equals, int32(1))
		})
	}
}

// TestConnect_CABundle_FailurePath_Verification refuses a server the bundle
// does not vouch for, so the connection ends in the TLS handshake and the
// client never logs in.
func TestConnect_CABundle_FailurePath_Verification(t *testing.T) {
	t.Run("a certificate another authority signed", func(t *testing.T) {
		c := qt.New(t)
		_, unrelated := newAuthority(c, "mysqlconnect unrelated")
		signer, _ := newAuthority(c, "mysqlconnect signer")
		server := startFakeServer(c, issue(c, signer, "127.0.0.1"))

		err := connect(c, caURL("mysql", server.address, unrelated, ""))

		c.Assert(err, qt.ErrorAs, new(x509.UnknownAuthorityError))
		c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*`+password+`.*`)
		c.Assert(server.handshakes.Load(), qt.Equals, int32(0))
	})

	t.Run("a certificate for another host", func(t *testing.T) {
		c := qt.New(t)
		authority, bundle := newAuthority(c, "mysqlconnect mismatch")
		server := startFakeServer(c, issue(c, authority, "mysql.invalid"))

		err := connect(c, caURL("mysql", server.address, bundle, ""))

		c.Assert(err, qt.ErrorAs, new(x509.HostnameError))
		c.Assert(server.handshakes.Load(), qt.Equals, int32(0))
	})
}

// TestConnect_CABundle_FailurePath_Bundle refuses a bundle that cannot be used
// before a connection is dialed: the server accepts no connection at all.
func TestConnect_CABundle_FailurePath_Bundle(t *testing.T) {
	tests := []struct {
		name string
		// names is the file the URL names. The test writes content to
		// ca.pem.
		names   string
		content []byte
		wantIs  error
		wantErr string
	}{
		{
			name:    "a missing file",
			names:   "absent.pem",
			content: []byte("not read\n"),
			wantIs:  fs.ErrNotExist,
			wantErr: `(?s).*the ssl-ca parameter names a CA bundle Ptah cannot use: read CA bundle: open .*absent\.pem: .*`,
		},
		{
			name:    "a file with no certificate",
			names:   "ca.pem",
			content: []byte("not a certificate\n"),
			wantIs:  cabundle.ErrNoCertificate,
			wantErr: `(?s).*the ssl-ca parameter names a CA bundle Ptah cannot use: CA bundle .*ca\.pem holds no PEM certificate.*`,
		},
		{
			name:    "a malformed certificate",
			names:   "ca.pem",
			content: []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
			wantIs:  cabundle.ErrMalformed,
			wantErr: `(?s).*the ssl-ca parameter names a CA bundle Ptah cannot use: CA bundle .*ca\.pem holds a certificate block that cannot be read.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			authority, _ := newAuthority(c, "mysqlconnect bundle")
			server := startFakeServer(c, issue(c, authority, "127.0.0.1"))
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "ca.pem"), test.content, 0o600), qt.IsNil)

			err := connect(c, caURL("mysql", server.address, filepath.Join(dir, test.names), ""))

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*`+password+`.*`)
			c.Assert(server.accepted.Load(), qt.Equals, int32(0))
		})
	}
}

// TestConnect_CABundle_ConnectionsAreIsolated opens connections with
// different bundles at once. Each verifies its own server against its own
// bundle alone: none trusts the other's authority, and a connection that
// names no bundle trusts neither, so loading a bundle added nothing to what
// the process trusts.
func TestConnect_CABundle_ConnectionsAreIsolated(t *testing.T) {
	c := qt.New(t)
	first, firstBundle := newAuthority(c, "mysqlconnect first")
	second, secondBundle := newAuthority(c, "mysqlconnect second")
	firstServer := startFakeServer(c, issue(c, first, "127.0.0.1"))
	secondServer := startFakeServer(c, issue(c, second, "127.0.0.1"))

	const rounds = 8
	results := make([]error, 2*rounds)
	var opened sync.WaitGroup
	for round := range rounds {
		opened.Go(func() {
			results[2*round] = connect(c, caURL("mysql", firstServer.address, firstBundle, ""))
		})
		opened.Go(func() {
			results[2*round+1] = connect(c, caURL("mysql", secondServer.address, secondBundle, ""))
		})
	}
	opened.Wait()

	for _, err := range results {
		var refused *mysqldriver.MySQLError
		c.Assert(err, qt.ErrorAs, &refused)
		c.Assert(refused.Number, qt.Equals, uint16(accessDenied))
	}
	c.Assert(firstServer.handshakes.Load(), qt.Equals, int32(rounds))
	c.Assert(secondServer.handshakes.Load(), qt.Equals, int32(rounds))

	crossed := connect(c, caURL("mysql", firstServer.address, secondBundle, ""))
	c.Assert(crossed, qt.ErrorAs, new(x509.UnknownAuthorityError))
	systemRoots := connect(c, "mysql://app:"+password+"@"+firstServer.address+"/shop?tls=true")
	c.Assert(systemRoots, qt.ErrorAs, new(*tls.CertificateVerificationError))
	c.Assert(firstServer.handshakes.Load(), qt.Equals, int32(rounds))
}
