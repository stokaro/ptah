package errorkind_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The servers here speak just enough of each wire protocol for a real driver
// to produce the error it produces against a real server, so the classifier is
// tested against the drivers' own types and wrapping rather than against
// errors a test built by hand. The live servers are in integration/.

// serverBehavior is what a fake server does with a connection.
type serverBehavior int

const (
	// answerError answers the client with a server error at once.
	answerError serverBehavior = iota
	// stayQuiet accepts the connection and never writes to it.
	stayQuiet
	// refuseTLS answers a TLS request with "not supported".
	refuseTLS
	// presentUntrustedCertificate completes the TLS request with a
	// certificate no client trusts.
	presentUntrustedCertificate
	// presentExpiredCertificate completes the TLS request with a certificate
	// that expired, which a client trusting it as a root still refuses.
	presentExpiredCertificate
	// requireClientCertificate completes the TLS request and then refuses the
	// client for sending no certificate of its own, with a TLS alert.
	requireClientCertificate
	// answerTLSInPlaintext accepts the TLS request and then answers the
	// client's TLS handshake with a plaintext packet.
	answerTLSInPlaintext
)

// fakeServer is one listening fake and what it does with each connection.
type fakeServer struct {
	listener net.Listener
	serve    func(net.Conn)
	done     sync.WaitGroup
}

// startServer listens on a loopback port and hands every connection to serve
// until the test ends.
func startServer(c *qt.C, serve func(net.Conn)) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	server := &fakeServer{listener: listener, serve: serve}
	server.done.Go(server.accept)
	c.Cleanup(func() {
		_ = listener.Close()
		server.done.Wait()
	})
	return listener.Addr().String()
}

func (s *fakeServer) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.done.Go(func() {
			defer func() { _ = conn.Close() }()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			s.serve(conn)
		})
	}
}

// refusedAddress is a loopback address nothing listens on.
func refusedAddress(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return address
}

// serverCertificate is a server configuration whose certificate is valid for
// the loopback address until notAfter and signed by itself, so no client
// trusts it unless it is handed the certificate as a root. rootFile holds that
// certificate in PEM.
func serverCertificate(c *qt.C, notAfter time.Time) (config *tls.Config, rootFile string) {
	c.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.Assert(err, qt.IsNil)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ptah-errorkind"},
		NotBefore:             notAfter.Add(-2 * time.Hour),
		NotAfter:              notAfter,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	c.Assert(err, qt.IsNil)
	rootFile = filepath.Join(c.TempDir(), "root.pem")
	c.Assert(os.WriteFile(rootFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600), qt.IsNil)
	config = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	return config, rootFile
}

// tlsFor is the server configuration behavior completes a TLS request with,
// and nil for a behavior that refuses or never sees one.
func tlsFor(c *qt.C, behavior serverBehavior) (config *tls.Config, rootFile string) {
	c.Helper()
	switch behavior {
	case presentUntrustedCertificate:
		return serverCertificate(c, time.Now().Add(time.Hour))
	case presentExpiredCertificate:
		return serverCertificate(c, time.Now().Add(-time.Hour))
	case requireClientCertificate:
		config, rootFile = serverCertificate(c, time.Now().Add(time.Hour))
		config.ClientAuth = tls.RequireAnyClientCert
		return config, rootFile
	default:
		return nil, ""
	}
}

// completeTLS runs the server's half of a handshake and holds the connection
// open, so an alert the server sends after it reaches the client.
func completeTLS(conn net.Conn, config *tls.Config) {
	server := tls.Server(conn, config)
	if server.Handshake() != nil {
		return
	}
	waitForClose(server)
}

// waitForClose holds a connection open without writing to it.
func waitForClose(conn net.Conn) {
	_, _ = io.Copy(io.Discard, conn)
}

// startPostgres starts a PostgreSQL fake and returns its address and the
// file holding the certificate it presents, if it presents one. answerError
// answers the startup message with a FATAL error carrying sqlstate.
func startPostgres(c *qt.C, behavior serverBehavior, sqlstate string) (address, rootFile string) {
	c.Helper()
	certificate, rootFile := tlsFor(c, behavior)
	return startServer(c, func(conn net.Conn) {
		if behavior == stayQuiet {
			waitForClose(conn)
			return
		}
		backend := pgproto3.NewBackend(conn, conn)
		for {
			message, err := backend.ReceiveStartupMessage()
			if err != nil {
				return
			}
			switch message.(type) {
			case *pgproto3.SSLRequest:
				if certificate == nil {
					_, _ = conn.Write([]byte{'N'})
					continue
				}
				_, _ = conn.Write([]byte{'S'})
				completeTLS(conn, certificate)
				return
			case *pgproto3.GSSEncRequest:
				_, _ = conn.Write([]byte{'N'})
			case *pgproto3.StartupMessage:
				backend.Send(&pgproto3.ErrorResponse{
					Severity:            "FATAL",
					SeverityUnlocalized: "FATAL",
					Code:                sqlstate,
					Message:             "refused by the fake server",
				})
				_ = backend.Flush()
				return
			}
		}
	}), rootFile
}

// MySQL capability flags the fake server announces, in the two 16-bit halves
// the handshake carries them in.
const (
	mysqlClientMySQL       uint16 = 1 << 0
	mysqlClientProtocol41  uint16 = 1 << 9
	mysqlClientSSL         uint16 = 1 << 11
	mysqlClientSecureConn  uint16 = 1 << 15
	mysqlClientPluginAuth  uint16 = 1 << (19 - 16)
	mysqlSSLRequestPayload        = 32
)

// mysqlLowerCapabilities are the lower-half flags every fake handshake
// announces; mysqlClientSSL is added where the server offers TLS.
const mysqlLowerCapabilities = mysqlClientMySQL | mysqlClientProtocol41 | mysqlClientSecureConn

// startMySQL starts a MySQL fake. answerError sends an error packet carrying
// number as the server's first packet, which is how a MySQL server refuses a
// connection before its handshake; the driver reads it with the code that
// reads the refusal of a handshake response.
func startMySQL(c *qt.C, behavior serverBehavior, number uint16) string {
	c.Helper()
	certificate, _ := tlsFor(c, behavior)
	return startServer(c, func(conn net.Conn) {
		switch behavior {
		case stayQuiet:
			waitForClose(conn)
		case answerError:
			_ = writeMySQLPacket(conn, 0, mysqlErrorPacket(number))
		case refuseTLS:
			_ = writeMySQLPacket(conn, 0, mysqlHandshake(mysqlLowerCapabilities, mysqlClientPluginAuth))
			waitForClose(conn)
		case presentUntrustedCertificate, answerTLSInPlaintext:
			if writeMySQLPacket(conn, 0, mysqlHandshake(mysqlLowerCapabilities|mysqlClientSSL, mysqlClientPluginAuth)) != nil {
				return
			}
			request := make([]byte, 4+mysqlSSLRequestPayload)
			if _, err := io.ReadFull(conn, request); err != nil {
				return
			}
			if certificate == nil {
				_ = writeMySQLPacket(conn, 2, mysqlErrorPacket(1045))
				waitForClose(conn)
				return
			}
			completeTLS(conn, certificate)
		}
	})
}

// mysqlHandshake is a protocol 10 initial handshake announcing the lower and
// upper halves of its capability flags.
func mysqlHandshake(lower, upper uint16) []byte {
	payload := []byte{10}
	payload = append(payload, "8.4.0-fake"...)
	payload = append(payload, 0)
	payload = binary.LittleEndian.AppendUint32(payload, 1)
	payload = append(payload, "abcdefgh"...)
	payload = append(payload, 0)
	payload = binary.LittleEndian.AppendUint16(payload, lower)
	payload = append(payload, 255)
	payload = binary.LittleEndian.AppendUint16(payload, 2)
	payload = binary.LittleEndian.AppendUint16(payload, upper)
	payload = append(payload, 21)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, "ijklmnopqrst"...)
	payload = append(payload, 0)
	payload = append(payload, "mysql_native_password"...)
	return append(payload, 0)
}

// mysqlErrorPacket is an error packet with number and a placeholder SQLSTATE.
func mysqlErrorPacket(number uint16) []byte {
	payload := []byte{0xff}
	payload = binary.LittleEndian.AppendUint16(payload, number)
	payload = append(payload, "#HY000"...)
	return append(payload, "refused by the fake server"...)
}

// writeMySQLPacket frames payload, which a fake keeps far below the 16 MiB a
// packet's three length bytes hold.
func writeMySQLPacket(conn net.Conn, sequence byte, payload []byte) error {
	length := uint32(len(payload)) // #nosec G115 -- every payload here is a few dozen bytes
	if length >= 1<<24 {
		return errors.New("packet too large")
	}
	header := binary.LittleEndian.AppendUint32(nil, length|uint32(sequence)<<24)
	_, err := conn.Write(append(header, payload...))
	return err
}
