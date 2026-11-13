// Package httpsource fetches a schema file or a migration directory from an
// https:// URL whose bytes are pinned by their SHA-256.
//
// One GET downloads the object to a private temporary file, hashing it as it
// writes, and the digest is compared before any byte is read as an archive.
// [sourcearchive] then reads the selection, and the result carries the content
// digest stokaro/ptah-operator defines in its internal/sourcecontent, which
// [sourcecontent] computes here, as a Git pull's does.
package httpsource

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/sourcearchive"
	"ptah.run/internal/sourcecontent"
)

// MaxDownloadBytes bounds what one pull downloads.
const MaxDownloadBytes = 64 << 20

// maxRedirects is the most redirects one pull follows.
const maxRedirects = 10

// The variables a pull reads its credential from. An environment value
// appears in neither shell history nor the process list, and there is no flag
// for a credential.
const (
	// UsernameEnv and PasswordEnv send Basic authentication.
	UsernameEnv = "PTAH_HTTPS_USERNAME"
	PasswordEnv = "PTAH_HTTPS_PASSWORD" // #nosec G101 -- the name of a variable, not a credential
	// TokenEnv sends a Bearer token.
	TokenEnv = "PTAH_HTTPS_TOKEN" // #nosec G101 -- the name of a variable, not a credential
)

// The refusals, matched with errors.Is. Each message says which reference and,
// for a digest, both digests. An archive's own refusals are sourcearchive's.
var (
	ErrReference      = errors.New("invalid HTTPS reference")
	ErrCredential     = errors.New("invalid HTTPS credential")
	ErrExpectation    = errors.New("invalid expected digest")
	ErrStatus         = errors.New("HTTPS server refused the download")
	ErrEncoding       = errors.New("content encoding refused")
	ErrRedirect       = errors.New("redirect refused")
	ErrTooLarge       = errors.New("download too large")
	ErrTruncated      = errors.New("download ended early")
	ErrDownloadDigest = errors.New("download digest differs")
	ErrDigest         = errors.New("content digest differs")
)

var sha256Digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Reference is a parsed https:// reference.
type Reference struct {
	// Raw is the reference as given.
	Raw string
	// URL is what the pull requests.
	URL *url.URL
}

// IsReference reports whether s is an http:// or https:// reference rather
// than another kind of source. ParseReference refuses http://.
func IsReference(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "http://")
}

// ParseReference reads an https://host[:port]/path reference. It names the
// object and nothing else: no credential, query or fragment, since a password
// in a URL lands in shell history and process lists, and a query is where a
// signed URL keeps one.
func ParseReference(raw string) (Reference, error) {
	parsed, err := dburldisplay.ParseURL(raw)
	if err != nil {
		return Reference{}, fmt.Errorf("%w %q: %w", ErrReference, dburldisplay.Format(raw), err)
	}
	shown := *parsed
	shown.User = nil
	switch {
	case parsed.Scheme == "http":
		return Reference{}, fmt.Errorf("%w %q: http:// is refused; give https://", ErrReference, shown.String())
	case parsed.Scheme != "https":
		return Reference{}, fmt.Errorf("%w %q: it is not https://", ErrReference, shown.String())
	case parsed.User != nil:
		return Reference{}, fmt.Errorf("%w %q: put the credential in %s and %s, or a token in %s, not in the URL",
			ErrReference, shown.String(), UsernameEnv, PasswordEnv, TokenEnv)
	case parsed.Host == "" || parsed.Hostname() == "":
		return Reference{}, fmt.Errorf("%w %q: no host", ErrReference, shown.String())
	case parsed.Path == "" || parsed.Path == "/":
		return Reference{}, fmt.Errorf("%w %q: no path", ErrReference, shown.String())
	case parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery:
		return Reference{}, fmt.Errorf("%w %q: no query or fragment", ErrReference, shown.String())
	}
	return Reference{Raw: raw, URL: parsed}, nil
}

// Origin is the scheme, host and port the reference requests, with the port
// written out.
func (r Reference) Origin() string {
	return origin(r.URL)
}

func origin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	return strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

// Credentials authenticate a pull: a username and a password for Basic, or a
// token for Bearer. The zero value is anonymous.
type Credentials struct {
	Username string
	Password string
	Token    string
}

func (c Credentials) present() bool {
	return c.Username != "" || c.Password != "" || c.Token != ""
}

// CredentialsFromEnvironment reads the credential variables through getenv,
// refusing a combination that is neither Basic, Bearer nor nothing.
func CredentialsFromEnvironment(getenv func(string) string) (Credentials, error) {
	c := Credentials{Username: getenv(UsernameEnv), Password: getenv(PasswordEnv), Token: getenv(TokenEnv)}
	switch {
	case c.Token != "" && (c.Username != "" || c.Password != ""):
		return Credentials{}, fmt.Errorf("%w: %s is set beside %s or %s; give Basic or Bearer, not both",
			ErrCredential, TokenEnv, UsernameEnv, PasswordEnv)
	case c.Username != "" && c.Password == "":
		return Credentials{}, fmt.Errorf("%w: %s is set without %s", ErrCredential, UsernameEnv, PasswordEnv)
	case c.Password != "" && c.Username == "":
		return Credentials{}, fmt.Errorf("%w: %s is set without %s; a Bearer token goes in %s",
			ErrCredential, PasswordEnv, UsernameEnv, TokenEnv)
	}
	return c, nil
}

// Options say what to fetch and how.
type Options struct {
	Reference   Reference
	Credentials Credentials
	// CAFile holds the only roots the server's certificate may chain to.
	// Empty, the system's roots are used.
	CAFile string
	// ExpectDownloadDigest is required: sha256: and the SHA-256 of the exact
	// bytes the URL serves.
	ExpectDownloadDigest string
	// Archive says how the selected files are carried and which they are.
	Archive sourcearchive.Options
	// ExpectDigest, when set, is the content digest the files have to have.
	ExpectDigest string
	// TempDir holds the download while it is read; empty, the system's
	// temporary directory.
	TempDir string
}

// Result is what a pull read.
type Result struct {
	// DownloadDigest and DownloadSize describe the bytes the URL served.
	DownloadDigest string
	DownloadSize   int64
	// ContentDigest is the digest of Files, as sourcecontent defines it.
	ContentDigest string
	// Files are the selected files, with paths relative to the selected
	// directory, or the file's own name for a selected file.
	Files []sourcecontent.File
}

// Fetch downloads the reference, holds it to the download digest, and reads
// the selected files from it.
func Fetch(ctx context.Context, o Options) (Result, error) {
	if !sha256Digest.MatchString(o.ExpectDownloadDigest) {
		return Result{}, fmt.Errorf("%w: the download digest %q is not sha256: and 64 lowercase hex digits", ErrExpectation, o.ExpectDownloadDigest)
	}
	if o.ExpectDigest != "" && !sha256Digest.MatchString(o.ExpectDigest) {
		return Result{}, fmt.Errorf("%w: the content digest %q is not sha256: and 64 lowercase hex digits", ErrExpectation, o.ExpectDigest)
	}
	if err := sourcearchive.Check(o.Archive); err != nil {
		return Result{}, err
	}
	client, err := newClient(o)
	if err != nil {
		return Result{}, err
	}
	body, err := fetchToFile(ctx, client, o)
	if err != nil {
		return Result{}, err
	}
	// The download goes once the selection is in memory, so the temporary
	// directory never holds it beside the output.
	files, err := func() ([]sourcecontent.File, error) {
		defer remove(body.file)
		if body.digest != o.ExpectDownloadDigest {
			return nil, fmt.Errorf("%w: %s served %s, not %s", ErrDownloadDigest, o.Reference.Raw, body.digest, o.ExpectDownloadDigest)
		}
		return sourcearchive.Read(body.file, body.size, o.Archive)
	}()
	if err != nil {
		return Result{}, err
	}
	content, err := sourcecontent.Digest(files)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %s in %s: %w", sourcearchive.ErrSelection, o.Archive.Path, o.Reference.Raw, err)
	}
	if o.ExpectDigest != "" && content != o.ExpectDigest {
		return Result{}, fmt.Errorf("%w: %s in %s has %s, not %s", ErrDigest, o.Archive.Path, o.Reference.Raw, content, o.ExpectDigest)
	}
	return Result{DownloadDigest: body.digest, DownloadSize: body.size, ContentDigest: content, Files: files}, nil
}

func remove(file *os.File) {
	_ = file.Close()
	_ = os.Remove(file.Name())
}

// newClient is a client that verifies the server with TLS 1.2 or later and
// follows only the redirects [redirects] admits.
func newClient(o Options) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile) // #nosec G304 -- the person names the file
		if err != nil {
			return nil, fmt.Errorf("read the CA file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("the CA file %s holds no certificate", o.CAFile)
		}
		tlsConfig.RootCAs = roots
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Transport: transport, CheckRedirect: redirects(o.Reference, o.Credentials)}, nil
}

// redirects follows at most maxRedirects redirects, never to http://, and
// refuses one that leaves the reference's origin while a credential is set:
// Go would keep the Authorization header on a redirect to a subdomain.
func redirects(reference Reference, credentials Credentials) func(*http.Request, []*http.Request) error {
	from := reference.Origin()
	credentialed := credentials.present()
	return func(request *http.Request, via []*http.Request) error {
		target := request.URL.Scheme + "://" + request.URL.Host
		switch {
		case len(via) > maxRedirects:
			return fmt.Errorf("%w: %s redirected more than %d times", ErrRedirect, reference.Raw, maxRedirects)
		case request.URL.Scheme != "https":
			return fmt.Errorf("%w: %s redirected to %s, which is not https://", ErrRedirect, reference.Raw, target)
		case credentialed && origin(request.URL) != from:
			return fmt.Errorf("%w: %s redirected to %s, another origin, with a credential set", ErrRedirect, reference.Raw, target)
		}
		return nil
	}
}

// download is a body written to a temporary file, with its size and digest.
type download struct {
	file   *os.File
	size   int64
	digest string
}

// fetchToFile downloads the reference into a new temporary file. The caller
// removes the file.
func fetchToFile(ctx context.Context, client *http.Client, o Options) (download, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, o.Reference.URL.String(), nil)
	if err != nil {
		return download{}, fmt.Errorf("%w %q: %w", ErrReference, o.Reference.Raw, err)
	}
	// Without it Go's transport asks for gzip and decodes a gzip-encoded
	// .tar.gz on the fly, and the digest would be the tar's.
	request.Header.Set("Accept-Encoding", "identity")
	switch c := o.Credentials; {
	case c.Token != "":
		request.Header.Set("Authorization", "Bearer "+c.Token)
	case c.Username != "":
		request.SetBasicAuth(c.Username, c.Password)
	}
	response, err := client.Do(request)
	if err != nil {
		// The client wraps a refused redirect in the request it was making.
		if refused, ok := errors.AsType[*url.Error](err); ok && errors.Is(refused.Err, ErrRedirect) {
			return download{}, refused.Err
		}
		return download{}, fmt.Errorf("download %s: %w", o.Reference.Raw, err)
	}
	defer func() { _ = response.Body.Close() }()
	switch encoding := response.Header.Get("Content-Encoding"); {
	case response.StatusCode != http.StatusOK:
		return download{}, fmt.Errorf("%w: GET %s answered %s", ErrStatus, o.Reference.Raw, response.Status)
	case encoding != "" && !strings.EqualFold(encoding, "identity"):
		return download{}, fmt.Errorf("%w: %s answered with Content-Encoding %q; the digest names the bytes as served, so they must not be encoded",
			ErrEncoding, o.Reference.Raw, encoding)
	case response.ContentLength > MaxDownloadBytes:
		return download{}, fmt.Errorf("%w: %s declares %d bytes, more than %d", ErrTooLarge, o.Reference.Raw, response.ContentLength, MaxDownloadBytes)
	}
	file, err := os.CreateTemp(o.TempDir, "ptah-https-*")
	if err != nil {
		return download{}, fmt.Errorf("create the download's temporary file: %w", err)
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, MaxDownloadBytes+1))
	switch {
	case written > MaxDownloadBytes:
		err = fmt.Errorf("%w: %s served more than %d bytes", ErrTooLarge, o.Reference.Raw, MaxDownloadBytes)
	case errors.Is(err, io.ErrUnexpectedEOF), err == nil && response.ContentLength >= 0 && written != response.ContentLength:
		err = fmt.Errorf("%w: %s declared %d bytes and served %d", ErrTruncated, o.Reference.Raw, response.ContentLength, written)
	case err != nil:
		err = fmt.Errorf("download %s: %w", o.Reference.Raw, err)
	}
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		remove(file)
		return download{}, err
	}
	return download{file: file, size: written, digest: "sha256:" + hex.EncodeToString(hash.Sum(nil))}, nil
}
