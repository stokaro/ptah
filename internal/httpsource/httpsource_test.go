package httpsource_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/httpsource"
	"ptah.run/internal/sourcearchive"
	"ptah.run/internal/sourcecontent"
)

const (
	user     = "deploy"
	password = "s3cret"
	token    = "t0ken"
)

// server is a TLS server whose requests the test reads afterwards.
type server struct {
	*httptest.Server
	caFile string
	mu     sync.Mutex
	seen   []http.Header
}

func newServer(c *qt.C, handler http.Handler) *server {
	s := &server{}
	s.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.seen = append(s.seen, r.Header.Clone())
		s.mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	c.Cleanup(s.Close)
	// Every httptest TLS server presents the same self-signed certificate, so
	// this file trusts each server a test starts.
	s.caFile = filepath.Join(c.TempDir(), "ca.pem")
	c.Assert(os.WriteFile(s.caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0o600), qt.IsNil)
	return s
}

func (s *server) requests() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func reference(c *qt.C, url string) httpsource.Reference {
	parsed, err := httpsource.ParseReference(url)
	c.Assert(err, qt.IsNil)
	return parsed
}

// options fetch url as the schema file schema.hcl, pinned to want's digest.
func options(c *qt.C, s *server, url string, want []byte) httpsource.Options {
	return httpsource.Options{
		Reference: reference(c, url), CAFile: s.caFile, ExpectDownloadDigest: digest(want),
		Archive: sourcearchive.Options{Format: sourcearchive.FormatFile, Path: "schema.hcl", Kind: sourcearchive.File},
		TempDir: c.TempDir(),
	}
}

func fetch(o httpsource.Options) (httpsource.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return httpsource.Fetch(ctx, o)
}

// leftBehind lists what a fetch left in its temporary directory.
func leftBehind(c *qt.C, o httpsource.Options) []string {
	entries, err := os.ReadDir(o.TempDir)
	c.Assert(err, qt.IsNil)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

var schema = []byte("table \"orders\" {\n  schema = schema.public\n}\r\n")

func serves(body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	})
}

func basic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// redirects sends /old to target and serves next everywhere else.
func redirects(target string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/old" {
			http.Redirect(w, r, target, http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func status(code int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte("an error page Ptah must not print"))
	})
}

func tarGzip(c *qt.C, files map[string]string) []byte {
	var archive bytes.Buffer
	compressed := gzip.NewWriter(&archive)
	writer := tar.NewWriter(compressed)
	for _, name := range []string{"orders-1.4.0/db/migrations/0001_create.up.sql", "orders-1.4.0/db/migrations/0001_create.down.sql", "../../etc/cron.d/x"} {
		data, ok := files[name]
		if !ok {
			continue
		}
		c.Assert(writer.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(data))}), qt.IsNil)
		_, err := writer.Write([]byte(data))
		c.Assert(err, qt.IsNil)
	}
	c.Assert(writer.Close(), qt.IsNil)
	c.Assert(compressed.Close(), qt.IsNil)
	return archive.Bytes()
}

func zipOf(c *qt.C, name, data string) []byte {
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	file, err := writer.Create(name)
	c.Assert(err, qt.IsNil)
	_, err = file.Write([]byte(data))
	c.Assert(err, qt.IsNil)
	c.Assert(writer.Close(), qt.IsNil)
	return archive.Bytes()
}

var migrationFiles = map[string]string{
	"orders-1.4.0/db/migrations/0001_create.up.sql":   "CREATE TABLE orders (id int);\n",
	"orders-1.4.0/db/migrations/0001_create.down.sql": "DROP TABLE orders;\n",
}

func TestFetch_HappyPath(t *testing.T) {
	c := qt.New(t)
	archive := tarGzip(c, migrationFiles)
	zipped := zipOf(c, "db/schema.hcl", string(schema))
	elsewhere := newServer(c, serves(schema))
	migrations := sourcearchive.Options{Format: sourcearchive.FormatTarGzip, Path: "orders-1.4.0/db/migrations", Kind: sourcearchive.Directory}
	schemaInZip := sourcearchive.Options{Format: sourcearchive.FormatZip, Path: "db/schema.hcl", Kind: sourcearchive.File}
	migrationResult := []sourcecontent.File{
		{Path: "0001_create.up.sql", Data: []byte("CREATE TABLE orders (id int);\n")},
		{Path: "0001_create.down.sql", Data: []byte("DROP TABLE orders;\n")},
	}
	tests := []struct {
		name        string
		handler     http.Handler
		path        string
		body        []byte
		archive     sourcearchive.Options
		credentials httpsource.Credentials
		want        []sourcecontent.File
		// authorization is the header the server has to have received.
		authorization string
	}{
		{name: "a file, anonymously", handler: serves(schema), path: "/inventory/1.4.0/schema", body: schema,
			want: []sourcecontent.File{{Path: "schema.hcl", Data: schema}}},
		{name: "a tar.gz with Basic", handler: basic(serves(archive)), path: "/orders/orders-1.4.0.tar.gz", body: archive, archive: migrations,
			credentials: httpsource.Credentials{Username: user, Password: password}, want: migrationResult,
			authorization: "Basic ZGVwbG95OnMzY3JldA=="},
		{name: "a zip with Bearer", handler: bearer(serves(zipped)), path: "/inventory/1.4.0/schema.zip", body: zipped, archive: schemaInZip,
			credentials: httpsource.Credentials{Token: token}, want: []sourcecontent.File{{Path: "schema.hcl", Data: schema}},
			authorization: "Bearer " + token},
		{name: "a redirect within the origin, with a credential", handler: redirects("/new", basic(serves(schema))), path: "/old", body: schema,
			credentials: httpsource.Credentials{Username: user, Password: password}, want: []sourcecontent.File{{Path: "schema.hcl", Data: schema}},
			authorization: "Basic ZGVwbG95OnMzY3JldA=="},
		{name: "a redirect to another origin, without a credential", handler: redirects(elsewhere.URL+"/schema.hcl", status(http.StatusTeapot)), path: "/old",
			body: schema, want: []sourcecontent.File{{Path: "schema.hcl", Data: schema}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newServer(c, test.handler)
			o := options(c, s, s.URL+test.path, test.body)
			o.Credentials = test.credentials
			o.Archive = cmpArchive(test.archive, o.Archive)
			content, err := sourcecontent.Digest(test.want)
			c.Assert(err, qt.IsNil)
			o.ExpectDigest = content

			result, err := fetch(o)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, httpsource.Result{
				DownloadDigest: digest(test.body), DownloadSize: int64(len(test.body)), ContentDigest: content, Files: test.want,
			})
			c.Assert(leftBehind(c, o), qt.HasLen, 0, qt.Commentf("the download is deleted"))
			first := s.requests()[0]
			c.Assert(first.Get("Accept-Encoding"), qt.Equals, "identity")
			c.Assert(first.Get("Authorization"), qt.Equals, test.authorization)
			for _, header := range []string{"Range", "If-None-Match", "If-Modified-Since", "If-Match", "If-Range"} {
				c.Assert(first.Values(header), qt.HasLen, 0, qt.Commentf("%s sent", header))
			}
		})
	}
}

// cmpArchive is the row's archive options, or the default when it gives none.
func cmpArchive(row, fallback sourcearchive.Options) sourcearchive.Options {
	if row.Format == "" {
		return fallback
	}
	return row
}

func TestFetch_FailurePath(t *testing.T) {
	c := qt.New(t)
	plain := httptest.NewServer(serves(schema))
	c.Cleanup(plain.Close)
	elsewhere := newServer(c, serves(schema))
	hostile := tarGzip(c, map[string]string{"../../etc/cron.d/x": "* * * * * root sh -c 'id'\n"})
	tests := []struct {
		name        string
		handler     http.Handler
		path        string
		body        []byte
		credentials httpsource.Credentials
		is          error
		says        string
	}{
		{name: "401", handler: basic(serves(schema)), path: "/schema", body: schema,
			is: httpsource.ErrStatus, says: `HTTPS server refused the download: GET https://127.0.0.1:\d+/schema answered 401 Unauthorized`},
		{name: "404", handler: status(http.StatusNotFound), path: "/schema", body: schema,
			is: httpsource.ErrStatus, says: `.* answered 404 Not Found`},
		{name: "503", handler: status(http.StatusServiceUnavailable), path: "/schema", body: schema,
			is: httpsource.ErrStatus, says: `.* answered 503 Service Unavailable`},
		{name: "a body that ends short", handler: endsShort(schema), path: "/schema", body: schema,
			is: httpsource.ErrTruncated, says: `download ended early: .* declared \d+ bytes and served \d+`},
		{name: "a declared length past the limit", handler: declaresTooMuch(), path: "/schema", body: schema,
			is: httpsource.ErrTooLarge, says: `download too large: .* declares 67108865 bytes, more than 67108864`},
		{name: "a body past the limit without a length", handler: streamsTooMuch(), path: "/schema", body: schema,
			is: httpsource.ErrTooLarge, says: `download too large: .* served more than 67108864 bytes`},
		{name: "a gzip content encoding", handler: gzipEncoded(schema), path: "/schema", body: schema,
			is: httpsource.ErrEncoding, says: `content encoding refused: .* answered with Content-Encoding "gzip".*`},
		{name: "a redirect to another origin, with a credential", handler: redirects(elsewhere.URL+"/schema", serves(schema)), path: "/old", body: schema,
			credentials: httpsource.Credentials{Token: token},
			is:          httpsource.ErrRedirect, says: `redirect refused: .* redirected to https://127.0.0.1:\d+, another origin, with a credential set`},
		{name: "a redirect to http://", handler: redirects(plain.URL+"/schema", serves(schema)), path: "/old", body: schema,
			is: httpsource.ErrRedirect, says: `redirect refused: .* redirected to http://127.0.0.1:\d+, which is not https://`},
		{name: "more than ten redirects", handler: redirectsForever(), path: "/0", body: schema,
			is: httpsource.ErrRedirect, says: `redirect refused: .* redirected more than 10 times`},
		// The archive would be refused for its entry names, and is refused
		// for its digest first: nothing is parsed before the digest matches.
		{name: "a hostile archive under another digest", handler: serves(hostile), path: "/orders.tar.gz", body: schema,
			is: httpsource.ErrDownloadDigest, says: `download digest differs: .* served ` + digest(hostile) + `, not ` + digest(schema)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newServer(c, test.handler)
			o := options(c, s, s.URL+test.path, test.body)
			o.Credentials = test.credentials
			o.Archive = sourcearchive.Options{Format: sourcearchive.FormatTarGzip, Path: "db/migrations", Kind: sourcearchive.Directory}

			result, err := fetch(o)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(err, qt.Not(qt.ErrorMatches), `.*an error page Ptah must not print.*`)
			c.Assert(result, qt.DeepEquals, httpsource.Result{})
			c.Assert(leftBehind(c, o), qt.HasLen, 0, qt.Commentf("the download is deleted"))
		})
	}
}

// A pull follows ten redirects and refuses the eleventh: the server answers
// eleven requests.
func TestFetch_FollowsTenRedirects(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, redirectsForever())
	o := options(c, s, s.URL+"/0", schema)

	result, err := fetch(o)

	c.Assert(err, qt.ErrorIs, httpsource.ErrRedirect)
	c.Assert(result, qt.DeepEquals, httpsource.Result{})
	c.Assert(s.requests(), qt.HasLen, 11)
}

// The selected files are held to the content digest once the download digest
// matched.
func TestFetch_RefusesAnotherContentDigest(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, serves(schema))
	o := options(c, s, s.URL+"/schema", schema)
	o.ExpectDigest = "sha256:" + hex.EncodeToString(make([]byte, 32))

	result, err := fetch(o)

	want, digestErr := sourcecontent.Digest([]sourcecontent.File{{Path: "schema.hcl", Data: schema}})
	c.Assert(digestErr, qt.IsNil)
	c.Assert(err, qt.ErrorIs, httpsource.ErrDigest)
	c.Assert(err, qt.ErrorMatches, `content digest differs: schema.hcl in https://.* has `+want+`, not sha256:0{64}`)
	c.Assert(result, qt.DeepEquals, httpsource.Result{})
}

// What the options alone refuse is refused before any request.
func TestFetch_RefusesBeforeAnyRequest(t *testing.T) {
	tests := []struct {
		name   string
		change func(*httpsource.Options)
		is     error
		says   string
	}{
		{name: "no download digest", change: func(o *httpsource.Options) { o.ExpectDownloadDigest = "" },
			is: httpsource.ErrExpectation, says: `invalid expected digest: the download digest "" is not sha256: and 64 lowercase hex digits`},
		{name: "an uppercase download digest", change: func(o *httpsource.Options) {
			o.ExpectDownloadDigest = "sha256:" + string(bytes.Repeat([]byte("A"), 64))
		},
			is: httpsource.ErrExpectation, says: `.*the download digest .* is not sha256: and 64 lowercase hex digits`},
		{name: "a content digest of another kind", change: func(o *httpsource.Options) { o.ExpectDigest = "md5:0123" },
			is: httpsource.ErrExpectation, says: `invalid expected digest: the content digest "md5:0123" is not sha256: and 64 lowercase hex digits`},
		{name: "a directory from a file", change: func(o *httpsource.Options) { o.Archive.Kind = sourcearchive.Directory },
			is: sourcearchive.ErrSelection, says: `.*a directory is read from an archive.*`},
		{name: "an unreadable CA file", change: func(o *httpsource.Options) { o.CAFile = filepath.Join(filepath.Dir(o.CAFile), "missing.pem") },
			is: os.ErrNotExist, says: `read the CA file: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := newServer(c, serves(schema))
			o := options(c, s, s.URL+"/schema", schema)
			test.change(&o)

			result, err := fetch(o)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(result, qt.DeepEquals, httpsource.Result{})
			c.Assert(s.requests(), qt.HasLen, 0)
		})
	}
}

// A CA file that did not sign the server's certificate replaces the roots that
// would have, and the server is refused.
func TestFetch_RefusesAServerTheCAFileDidNotSign(t *testing.T) {
	c := qt.New(t)
	s := newServer(c, serves(schema))
	o := options(c, s, s.URL+"/schema", schema)
	o.CAFile = otherCA(c)

	result, err := fetch(o)

	c.Assert(err, qt.ErrorMatches, `download https://127.0.0.1:\d+/schema: .*certificate signed by unknown authority.*`)
	c.Assert(result, qt.DeepEquals, httpsource.Result{})
}

// otherCA writes a CA certificate that signed nothing a test serves.
func otherCA(c *qt.C) string {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.Assert(err, qt.IsNil)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "another CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	c.Assert(err, qt.IsNil)
	file := filepath.Join(c.TempDir(), "other-ca.pem")
	c.Assert(os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600), qt.IsNil)
	return file
}

// endsShort declares body's length and sends half of it; the server then
// closes the connection.
func endsShort(body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:len(body)/2])
	})
}

func declaresTooMuch() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(httpsource.MaxDownloadBytes+1))
		_, _ = w.Write([]byte("x"))
	})
}

// streamsTooMuch sends one byte more than the limit, chunked, with no length.
func streamsTooMuch() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chunk := make([]byte, 1<<20)
		for range httpsource.MaxDownloadBytes / len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		_, _ = w.Write([]byte("x"))
	})
}

func gzipEncoded(body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		compressed := gzip.NewWriter(w)
		_, _ = compressed.Write(body)
		_ = compressed.Close()
	})
}

// redirectsForever sends /n to /n+1.
func redirectsForever() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Path[1:])
		http.Redirect(w, r, "/"+strconv.Itoa(n+1), http.StatusFound)
	})
}

func TestParseReference_HappyPath(t *testing.T) {
	tests := []struct {
		raw, origin string
	}{
		{raw: "https://artifacts.example.com/inventory/1.4.0/schema", origin: "https://artifacts.example.com:443"},
		{raw: "https://Artifacts.Example.com:8443/orders.tar.gz", origin: "https://artifacts.example.com:8443"},
		{raw: "https://[2001:db8::1]/orders.zip", origin: "https://[2001:db8::1]:443"},
	}
	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			c := qt.New(t)
			got, err := httpsource.ParseReference(test.raw)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Raw, qt.Equals, test.raw)
			c.Assert(got.URL.String(), qt.Equals, test.raw)
			c.Assert(got.Origin(), qt.Equals, test.origin)
			c.Assert(httpsource.IsReference(test.raw), qt.IsTrue)
		})
	}
}

func TestParseReference_FailurePath(t *testing.T) {
	tests := []struct {
		name, raw, says string
	}{
		{name: "http", raw: "http://artifacts.example.com/schema.hcl", says: `.*http:// is refused; give https://`},
		{name: "another scheme", raw: "ftp://artifacts.example.com/schema.hcl", says: `.*it is not https://`},
		{name: "a credential", raw: "https://deploy:s3cret@artifacts.example.com/schema.hcl", // #nosec G101 -- the URL the test requires refused
			says: `invalid HTTPS reference "https://artifacts.example.com/schema.hcl": put the credential in PTAH_HTTPS_USERNAME and PTAH_HTTPS_PASSWORD, or a token in PTAH_HTTPS_TOKEN, not in the URL`},
		{name: "a user alone", raw: "https://deploy@artifacts.example.com/schema.hcl", says: `.*put the credential in .*`},
		{name: "no host", raw: "https:///schema.hcl", says: `.*no host`},
		{name: "a port and no host", raw: "https://:443/schema.hcl", says: `.*no host`},
		{name: "no path", raw: "https://artifacts.example.com", says: `.*no path`},
		{name: "the root", raw: "https://artifacts.example.com/", says: `.*no path`},
		{name: "a query", raw: "https://artifacts.example.com/schema.hcl?X-Amz-Signature=abc", says: `.*no query or fragment`},
		{name: "an empty query", raw: "https://artifacts.example.com/schema.hcl?", says: `.*no query or fragment`},
		{name: "a fragment", raw: "https://artifacts.example.com/schema.hcl#v2", says: `.*no query or fragment`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := httpsource.ParseReference(test.raw)
			c.Assert(err, qt.ErrorIs, httpsource.ErrReference)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(err, qt.Not(qt.ErrorMatches), `.*s3cret.*`)
			c.Assert(got, qt.DeepEquals, httpsource.Reference{})
		})
	}
}

func TestCredentialsFromEnvironment_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want httpsource.Credentials
	}{
		{name: "none", env: nil, want: httpsource.Credentials{}},
		{name: "Basic", env: map[string]string{httpsource.UsernameEnv: user, httpsource.PasswordEnv: password},
			want: httpsource.Credentials{Username: user, Password: password}},
		{name: "Bearer", env: map[string]string{httpsource.TokenEnv: token}, want: httpsource.Credentials{Token: token}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := httpsource.CredentialsFromEnvironment(func(name string) string { return test.env[name] })
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestCredentialsFromEnvironment_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		says string
	}{
		{name: "a token beside Basic", env: map[string]string{httpsource.UsernameEnv: user, httpsource.PasswordEnv: password, httpsource.TokenEnv: token},
			says: `invalid HTTPS credential: PTAH_HTTPS_TOKEN is set beside PTAH_HTTPS_USERNAME or PTAH_HTTPS_PASSWORD; give Basic or Bearer, not both`},
		{name: "a token beside a password", env: map[string]string{httpsource.PasswordEnv: password, httpsource.TokenEnv: token},
			says: `.*PTAH_HTTPS_TOKEN is set beside.*`},
		{name: "a username alone", env: map[string]string{httpsource.UsernameEnv: user},
			says: `invalid HTTPS credential: PTAH_HTTPS_USERNAME is set without PTAH_HTTPS_PASSWORD`},
		{name: "a password alone", env: map[string]string{httpsource.PasswordEnv: password},
			says: `invalid HTTPS credential: PTAH_HTTPS_PASSWORD is set without PTAH_HTTPS_USERNAME; a Bearer token goes in PTAH_HTTPS_TOKEN`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := httpsource.CredentialsFromEnvironment(func(name string) string { return test.env[name] })
			c.Assert(err, qt.ErrorIs, httpsource.ErrCredential)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(got, qt.DeepEquals, httpsource.Credentials{})
		})
	}
}
