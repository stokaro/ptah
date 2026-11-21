// Package advisorysource supplies the reviewed advisory snapshots that
// `ptah db vulnerabilities` matches when no --advisories file is selected. It
// fetches each engine's snapshot from an advisory source over HTTPS, keeps
// the copy in the user cache directory, and replaces a copy it cannot use
// with the snapshot built into the binary, saying why. It implements
// vulnerability.SnapshotSource; the matching itself stays in
// internal/vulnerability and makes no request.
package advisorysource

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ptah.run/internal/vulnerability"
)

// DefaultLocation is the advisory source a released binary reads: the
// reviewed snapshot files on the repository's master branch, which the daily
// advisory refresh keeps current. Released binaries read this path, so it is
// a contract: the files must stay at internal/vulnerability/<engine file> on
// master. Nothing here relies on how a change reaches master: the scan merges
// a copy read from it with the bundled snapshot, so the copy can add findings
// but cannot remove one (vulnerability.EvaluateVersionedWithFloor).
const DefaultLocation = "https://raw.githubusercontent.com/stokaro/ptah/master/internal/vulnerability/"

// Repository is the location that reads DefaultLocation, and the default of
// --advisory-source.
const Repository = "repository"

// Bundled is the location that reads only the snapshots built into the
// binary and makes no request.
const Bundled = "bundled"

// Freshness is how long a cached copy is used without asking the advisory
// source again, and how long a request that failed is remembered. After it,
// the copy is revalidated with If-None-Match and a failed source is asked
// again.
const Freshness = time.Hour

// MaxSnapshotBytes bounds a fetched snapshot. The largest bundled snapshot is
// a small fraction of it.
const MaxSnapshotBytes = 8 << 20

// requestTimeout bounds one fetch, connection, response headers and body
// included, so a stalled source costs a run at most this per engine, once an
// hour.
const requestTimeout = 15 * time.Second

// responseHeaderTimeout bounds the wait for the response headers once the
// request is sent.
const responseHeaderTimeout = 10 * time.Second

// dialTimeout bounds opening one connection. A network that drops packets
// rather than refusing them costs this, not requestTimeout, and the failure is
// then remembered for Freshness.
const dialTimeout = 5 * time.Second

// ParseLocation reads an --advisory-source value: Repository for
// DefaultLocation, Bundled, or the absolute URL of a directory that serves
// each engine's snapshot under the bundled file name, such as a mirror. The
// URL must use https; http is accepted only for a loopback host, so a
// snapshot that decides what a scan reports is never read over a network an
// attacker can rewrite. It returns nil for Bundled.
//
// A refusal never prints credentials, a query or a fragment, which may carry
// a token, and does not print a value it cannot read as a URL at all.
func ParseLocation(value string) (*url.URL, error) {
	switch value {
	case Bundled:
		return nil, nil
	case Repository:
		value = DefaultLocation
	}
	location, err := url.Parse(value)
	if err != nil || !location.IsAbs() || location.Host == "" {
		return nil, fmt.Errorf("advisory source is neither %q, %q nor an absolute URL", Repository, Bundled)
	}
	if location.User != nil || location.RawQuery != "" || location.ForceQuery || location.Fragment != "" {
		return nil, fmt.Errorf("advisory source %s carries credentials, a query or a fragment, which are not printed", redacted(location))
	}
	if err := checkScheme(location); err != nil {
		return nil, fmt.Errorf("advisory source %s: %w", location, err)
	}
	if !strings.HasSuffix(location.Path, "/") {
		location.Path += "/"
	}
	return location, nil
}

// redacted returns location without the parts that may carry a secret.
func redacted(location *url.URL) string {
	clean := *location
	clean.User = nil
	clean.RawQuery, clean.ForceQuery = "", false
	clean.Fragment, clean.RawFragment = "", ""
	return clean.String()
}

func checkScheme(location *url.URL) error {
	switch location.Scheme {
	case "https":
		return nil
	case "http":
		if loopback(location.Hostname()) {
			return nil
		}
		return errors.New("http is accepted only for a loopback host; use https")
	default:
		return fmt.Errorf("scheme %q is not https", location.Scheme)
	}
}

func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Config configures a Source.
type Config struct {
	// Location is the advisory source: nil reads the bundled snapshots only.
	// ParseLocation reads the flag value into one.
	Location *url.URL
	// CacheDir holds the cached copies. Empty disables the cache, so every
	// run fetches.
	CacheDir string
	// Client sends the requests. Nil uses a client with Timeout that follows
	// redirects only to locations ParseLocation accepts.
	Client *http.Client
	// Timeout bounds one request, connection, response headers and body
	// included, when Client is nil. Zero means 15 seconds.
	Timeout time.Duration
	// Now reads the clock the freshness window is measured with. Nil reads
	// time.Now.
	Now func() time.Time
}

// DefaultCacheDir returns os.UserCacheDir()/ptah/advisories, or empty when
// the platform names no user cache directory.
func DefaultCacheDir() string {
	cache, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(cache, "ptah", "advisories")
}

// Source supplies each product's snapshot once per Source, so the targets of
// one run that share an engine share one fetch. It is safe for concurrent
// use.
type Source struct {
	config Config
	mu     sync.Mutex
	loaded map[string]loaded
}

type loaded struct {
	data   []byte
	origin vulnerability.Origin
}

// New returns a Source over config.
func New(config Config) *Source {
	if config.Client == nil {
		config.Client = newClient(cmp.Or(config.Timeout, requestTimeout))
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Source{config: config, loaded: make(map[string]loaded)}
}

// Snapshot implements vulnerability.SnapshotSource. The request names only
// the snapshot file, so the advisory source learns which engines a run scans
// and nothing else about the databases.
//
// A copy is used only when it passes the validators the bundled snapshot
// passes, was retrieved no earlier than both the bundled snapshot and the copy
// already cached, and is dated no more than vulnerability.MaxFutureRetrieval
// ahead of the clock. The scan merges it with the bundled snapshot, so what it
// may change is decided there. When the source cannot give one, a cached copy
// that passes the same checks and is newer than the bundled snapshot is used,
// and the bundled snapshot otherwise; either way the origin's Fallback says
// why, and why the cached copy was not used when it was not.
func (s *Source) Snapshot(ctx context.Context, product string) ([]byte, vulnerability.Origin) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if result, ok := s.loaded[product]; ok {
		return result.data, result.origin
	}
	data, origin := s.load(ctx, product)
	s.loaded[product] = loaded{data: data, origin: origin}
	return data, origin
}

func (s *Source) load(ctx context.Context, product string) ([]byte, vulnerability.Origin) {
	name, data, ok := vulnerability.BundledSnapshot(product)
	if !ok {
		return nil, vulnerability.Origin{}
	}
	if s.config.Location == nil {
		return data, vulnerability.Origin{Kind: "bundled"}
	}
	retrieved, err := vulnerability.CheckSnapshot(product, data)
	if err != nil {
		panic(fmt.Sprintf("bundled %s snapshot is invalid: %v", product, err)) // the vulnerability package's tests validate every bundled snapshot
	}
	base := baseline{product: product, data: data, retrieved: retrieved}
	return s.fetch(ctx, base, s.config.Location.JoinPath(name).String())
}

// baseline is the bundled snapshot every copy of one product is held to.
type baseline struct {
	product   string
	data      []byte
	retrieved time.Time
}

// check validates a copy as the bundled snapshot is validated, refuses one
// dated more than vulnerability.MaxFutureRetrieval after now, and refuses one
// retrieved before floor, which is the retrieval time of what the copy would
// replace. It returns the copy's retrieval time.
//
// The future bound keeps one misdated copy out of the cache: cached, its
// retrieval time would become the floor, and every correct copy after it
// would be refused as older.
func (b baseline) check(body []byte, floor time.Time, replaced string, now time.Time) (time.Time, error) {
	retrieved, err := vulnerability.CheckSnapshot(b.product, body)
	if schema, ok := errors.AsType[*vulnerability.UnsupportedSchemaError](err); ok {
		return time.Time{}, schema
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("the snapshot is invalid: %w", err)
	}
	if retrieved.After(now.Add(vulnerability.MaxFutureRetrieval)) {
		return time.Time{}, fmt.Errorf("the snapshot was retrieved at %s, more than %d hours after this machine's clock at %s",
			stamp(retrieved), int(vulnerability.MaxFutureRetrieval.Hours()), stamp(now))
	}
	if retrieved.Before(floor) {
		return time.Time{}, fmt.Errorf("the snapshot was retrieved at %s, before %s at %s",
			stamp(retrieved), replaced, stamp(floor))
	}
	return retrieved, nil
}

func stamp(moment time.Time) string { return moment.UTC().Format(time.RFC3339Nano) }

// within reports whether moment is in the Freshness window that ends now. A
// moment after now is not: a clock that moved back, or an entry written with
// a later time, must not keep a copy or a failure current.
func within(now, moment time.Time) bool {
	return !now.Before(moment) && now.Sub(moment) < Freshness
}

// fetch returns the copy of address to use and its origin: the cached copy
// while it is fresh, otherwise what the source answers, and when the source
// gives no usable copy, the fallback.
func (s *Source) fetch(ctx context.Context, base baseline, address string) ([]byte, vulnerability.Origin) {
	now := s.config.Now()
	entry, cachedAt, refusal := s.readCache(base, address, now)
	if !cachedAt.IsZero() && within(now, entry.FetchedAt) {
		return entry.Body, remoteOrigin(address, entry.FetchedAt, "fresh")
	}
	fallback := func(reason string) ([]byte, vulnerability.Origin) {
		if cachedAt.After(base.retrieved) {
			origin := remoteOrigin(address, entry.FetchedAt, "unconfirmed")
			origin.Fallback = reason
			return entry.Body, origin
		}
		if refusal != nil {
			reason += "; the cached copy was not used: " + refusal.Error()
		}
		return base.data, vulnerability.Origin{Kind: "bundled", URL: address, Fallback: reason}
	}
	if within(now, entry.FailedAt) {
		return fallback(fmt.Sprintf("%s, at %s; the source is asked again from %s",
			entry.Failure, stamp(entry.FailedAt), stamp(entry.FailedAt.Add(Freshness))))
	}
	body, origin, err := s.request(ctx, base, address, entry, cachedAt, now)
	if err != nil {
		// A run the caller canceled says nothing about the source.
		if ctx.Err() == nil {
			entry.URL, entry.FailedAt, entry.Failure = address, now, err.Error()
			s.writeCache(entry)
		}
		return fallback(err.Error())
	}
	return body, origin
}

// request asks the source for address, revalidating the cached copy in
// entry when cachedAt says there is one.
func (s *Source) request(
	ctx context.Context,
	base baseline,
	address string,
	entry cacheEntry,
	cachedAt, now time.Time,
) ([]byte, vulnerability.Origin, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, http.NoBody)
	if err != nil {
		return nil, vulnerability.Origin{}, fmt.Errorf("build the request: %w", err)
	}
	if !cachedAt.IsZero() && entry.ETag != "" {
		request.Header.Set("If-None-Match", entry.ETag)
	}
	response, err := s.config.Client.Do(request)
	if err != nil {
		return nil, vulnerability.Origin{}, fmt.Errorf("fetch failed: %w", withoutURL(err))
	}
	defer func() { _ = response.Body.Close() }()
	switch {
	case response.StatusCode == http.StatusNotModified && !cachedAt.IsZero():
		entry.FetchedAt = now
		s.writeCache(entry)
		return entry.Body, remoteOrigin(address, now, "revalidated"), nil
	case response.StatusCode != http.StatusOK:
		return nil, vulnerability.Origin{}, fmt.Errorf("the advisory source answered %s", statusText(response.StatusCode))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSnapshotBytes+1))
	if err != nil {
		return nil, vulnerability.Origin{}, fmt.Errorf("read the response: %w", withoutURL(err))
	}
	if len(body) > MaxSnapshotBytes {
		return nil, vulnerability.Origin{}, fmt.Errorf("the response is larger than %d bytes", MaxSnapshotBytes)
	}
	floor, replaced := base.retrieved, "the bundled snapshot"
	if cachedAt.After(floor) {
		floor, replaced = cachedAt, "the cached copy"
	}
	if _, err := base.check(body, floor, replaced, now); err != nil {
		return nil, vulnerability.Origin{}, err
	}
	digest := sha256.Sum256(body)
	s.writeCache(cacheEntry{URL: address, ETag: response.Header.Get("ETag"), FetchedAt: now,
		Body: body, BodySHA256: hex.EncodeToString(digest[:])})
	return body, remoteOrigin(address, now, "fetched"), nil
}

// statusText names an HTTP status by its code and the standard text for it.
// The status line the server sent is not printed: its reason phrase is
// whatever bytes the server chose, terminal control characters included.
func statusText(code int) string {
	if text := http.StatusText(code); text != "" {
		return fmt.Sprintf("HTTP %d %s", code, text)
	}
	return fmt.Sprintf("HTTP %d", code)
}

// withoutURL drops the URL a client error names. The source's own address is
// in the origin already, and a redirect target may carry a token in its query.
func withoutURL(err error) error {
	if failure, ok := errors.AsType[*url.Error](err); ok {
		return failure.Err
	}
	return err
}

func remoteOrigin(address string, fetched time.Time, cache string) vulnerability.Origin {
	return vulnerability.Origin{Kind: "remote", URL: address, FetchedAt: fetched.UTC(), Cache: cache}
}

// cacheEntry is one cached copy and the last failure to fetch it. The file is
// named for a hash of the URL, so two sources never share an entry; URL is
// kept for a person reading the cache. BodySHA256 ties the body to the ETag
// the source confirms: a body changed without it is not used.
type cacheEntry struct {
	URL        string    `json:"url"`
	ETag       string    `json:"etag,omitempty"`
	FetchedAt  time.Time `json:"fetched_at,omitzero"`
	Body       []byte    `json:"body,omitempty"`
	BodySHA256 string    `json:"body_sha256,omitempty"`
	FailedAt   time.Time `json:"failed_at,omitzero"`
	Failure    string    `json:"failure,omitempty"`
}

func (s *Source) cachePath(address string) string {
	digest := sha256.Sum256([]byte(address))
	return filepath.Join(s.config.CacheDir, hex.EncodeToString(digest[:16])+".json")
}

// readCache returns the cache entry of address and the retrieval time of its
// copy. The time is zero when there is no copy to use: none was cached, its
// body does not match its hash, or it fails the checks a fetched copy passes.
// The entry is returned either way, for the failure it remembers; its copy,
// ETag and fetch time count only with a time. A cache directory or entry that
// is not private to the user is not read at all. The error says why a cache
// that exists was not used, and is nil when there is none.
func (s *Source) readCache(base baseline, address string, now time.Time) (cacheEntry, time.Time, error) {
	if s.config.CacheDir == "" {
		return cacheEntry{}, time.Time{}, nil
	}
	if err := s.checkCacheDir(); err != nil {
		return cacheEntry{}, time.Time{}, absentIsNil(err)
	}
	path := s.cachePath(address)
	info, err := os.Lstat(path)
	switch {
	case err != nil:
		return cacheEntry{}, time.Time{}, absentIsNil(err)
	case !info.Mode().IsRegular():
		return cacheEntry{}, time.Time{}, fmt.Errorf("%s is not a regular file", path)
	}
	if err := checkPrivate(path, info); err != nil {
		return cacheEntry{}, time.Time{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cacheEntry{}, time.Time{}, err
	}
	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return cacheEntry{}, time.Time{}, fmt.Errorf("%s is not a cache entry: %w", path, err)
	}
	if len(entry.Body) == 0 {
		return entry, time.Time{}, nil
	}
	digest := sha256.Sum256(entry.Body)
	if entry.BodySHA256 != hex.EncodeToString(digest[:]) {
		return entry, time.Time{}, fmt.Errorf("the copy in %s does not match its SHA-256", path)
	}
	retrieved, err := base.check(entry.Body, base.retrieved, "the bundled snapshot", now)
	if err != nil {
		return entry, time.Time{}, err
	}
	return entry, retrieved, nil
}

// absentIsNil returns nil for a path that does not exist, which is a cache
// nothing has written yet rather than one that was refused.
func absentIsNil(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// checkCacheDir refuses a cache directory that is not a directory private to
// the user.
func (s *Source) checkCacheDir() error {
	info, err := os.Lstat(s.config.CacheDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is a symbolic link or not a directory", s.config.CacheDir)
	}
	return checkPrivate(s.config.CacheDir, info)
}

// writeCache stores entry atomically. A cache that cannot be written costs
// the next run a request and changes nothing this run reports, so the error
// is dropped. A directory that is not private to the user is not written.
func (s *Source) writeCache(entry cacheEntry) {
	if s.config.CacheDir == "" {
		return
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	if err := os.MkdirAll(s.config.CacheDir, 0o700); err != nil || s.checkCacheDir() != nil {
		return
	}
	temp, err := os.CreateTemp(s.config.CacheDir, ".entry-*")
	if err != nil {
		return
	}
	_, writeErr := temp.Write(data)
	closeErr := temp.Close()
	if writeErr != nil || closeErr != nil || os.Rename(temp.Name(), s.cachePath(entry.URL)) != nil {
		_ = os.Remove(temp.Name())
	}
}

// newClient follows a redirect only to a location ParseLocation accepts, and
// gives up on a connection that does not open within dialTimeout. It reads the
// proxy settings of the environment, as http.DefaultTransport does.
func newClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the default transport is an *http.Transport
	transport.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = responseHeaderTimeout
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("stopped after 5 redirects")
			}
			return checkScheme(request.URL)
		},
	}
}
