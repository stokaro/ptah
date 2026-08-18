package ociartifact

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"time"

	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/retry"
)

const (
	defaultOperationTimeout      = 2 * time.Minute
	defaultDialTimeout           = 10 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultResponseHeaderTimeout = 30 * time.Second
	defaultIdleConnectionTimeout = 90 * time.Second
)

// ClientOptions configures remote registry access.
type ClientOptions struct {
	// PlainHTTP explicitly permits unencrypted registry traffic. Leave false
	// for every production registry.
	PlainHTTP bool
	// Limits bounds untrusted manifest and layer data read from registries.
	Limits Limits
	// OperationTimeout bounds each registry operation and credential-helper
	// lookup. The default is two minutes.
	OperationTimeout time.Duration
	// Transport configures how this client reaches the registry: an additional
	// certificate authority and a mutual-TLS client credential. The zero value
	// is read from the environment, so every OCI consumer in a run shares one
	// configuration without each command growing its own flags.
	Transport TransportOptions
	// ReferrerPolicy decides how attachments made through this client are made
	// discoverable. It sits here rather than on each attachment because it is
	// a property of how this run talks to its registry, like PlainHTTP, and
	// threading it through every publish signature would put the same decision
	// in three places that must not disagree. An attachment may still override
	// it. The zero value is [ReferrerPolicyAuto].
	ReferrerPolicy ReferrerPolicy
}

// Client stores and retrieves Ptah artifacts from OCI repositories.
type Client struct {
	options ClientOptions
	store   credentials.Store
	tls     *tls.Config
}

// NewClient creates a client backed by Docker's credential configuration.
// DOCKER_CONFIG, credsStore, and per-registry credHelpers are honored.
func NewClient(opts ClientOptions) (*Client, error) {
	store, err := credentials.NewStoreFromDocker(credentials.StoreOptions{})
	if err != nil {
		return nil, fmt.Errorf("open Docker credential store: %w", err)
	}
	if !opts.Transport.configured() {
		opts.Transport = TransportFromEnvironment()
	}
	tlsConfig, err := opts.Transport.tlsConfig()
	if err != nil {
		return nil, err
	}
	if opts.ReferrerPolicy == "" {
		policy, err := ParseReferrerPolicy(os.Getenv(ReferrerPolicyEnv))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", ReferrerPolicyEnv, err)
		}
		opts.ReferrerPolicy = policy
	}
	opts.Limits = opts.Limits.normalized()
	if opts.OperationTimeout <= 0 {
		opts.OperationTimeout = defaultOperationTimeout
	}
	return &Client{options: opts, store: store, tls: tlsConfig}, nil
}

// NewRepository creates an authenticated ORAS repository client using Docker
// credentials.
func NewRepository(ref Reference, opts ClientOptions) (*remote.Repository, error) {
	client, err := NewClient(opts)
	if err != nil {
		return nil, err
	}
	return client.repository(ref)
}

// Push stores fsys as an OCI artifact and returns its immutable manifest
// descriptor. The manifest is tagged latest, with the reference tag when
// present, and with every additional tag in opts.
func Push(ctx context.Context, rawRef string, fsys fs.FS, opts PushOptions) (PushResult, error) {
	client, err := NewClient(ClientOptions{})
	if err != nil {
		return PushResult{}, err
	}
	return client.Push(ctx, rawRef, fsys, opts)
}

// Pull retrieves an OCI artifact into an immutable in-memory filesystem.
func Pull(ctx context.Context, rawRef string, opts PullOptions) (Artifact, error) {
	client, err := NewClient(ClientOptions{})
	if err != nil {
		return Artifact{}, err
	}
	return client.Pull(ctx, rawRef, opts)
}

// Push stores fsys using this client's transport and credentials.
func (c *Client) Push(ctx context.Context, rawRef string, fsys fs.FS, opts PushOptions) (PushResult, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return PushResult{}, err
	}
	if ref.IsDigest() {
		return PushResult{}, fmt.Errorf("%w: %s", ErrDigestPush, ref)
	}
	repository, err := c.repository(ref)
	if err != nil {
		return PushResult{}, err
	}
	opts.Annotations = withProvenance(opts.Annotations)
	opts.Tags = append(opts.Tags, ref.Selector(), DefaultTag)
	opts.Limits = mergeLimits(opts.Limits, c.options.Limits)
	result, err := PushTo(ctx, repository, fsys, opts)
	result.Reference = ref
	if err != nil {
		return result, fmt.Errorf("push %s: %w", ref, err)
	}
	return result, nil
}

// Pull retrieves an artifact using this client's transport and credentials.
func (c *Client) Pull(ctx context.Context, rawRef string, opts PullOptions) (Artifact, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return Artifact{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return Artifact{}, err
	}
	opts.Limits = mergeLimits(opts.Limits, c.options.Limits)
	artifact, err := PullFrom(ctx, repository, ref.Selector(), opts)
	if err != nil {
		return Artifact{}, fmt.Errorf("pull %s: %w", ref, err)
	}
	artifact.Reference = ref
	return artifact, nil
}

func (c *Client) repository(ref Reference) (*remote.Repository, error) {
	repository, err := remote.NewRepository(ref.repositoryName())
	if err != nil {
		return nil, fmt.Errorf("create OCI repository client: %w", err)
	}
	repository.PlainHTTP = c.options.PlainHTTP
	repository.MaxMetadataBytes = c.options.Limits.ManifestBytes
	repository.ReferrerListMaxPages = c.options.Limits.ReferrerPages
	repository.TagListMaxPages = c.options.Limits.ReferrerPages
	repository.Client = &auth.Client{
		Client:     newHTTPClient(c.options.OperationTimeout, c.tls),
		Cache:      auth.NewCache(),
		Credential: c.credential,
	}
	return repository, nil
}

func (c *Client) attachmentTarget(ref Reference) (*remote.Repository, error) {
	repository, err := c.repository(ref)
	if err != nil {
		return nil, err
	}
	// ORAS maintains the standard referrers tag schema when a registry lacks
	// the native API. Keep superseded index manifests because many registries,
	// including registry:2, disable manifest deletion. Ptah's durable tags
	// recover entries lost by concurrent cross-process index updates.
	repository.SkipReferrersGC = true
	return repository, nil
}

func (c *Client) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.options.OperationTimeout)
}

func (c *Client) credential(ctx context.Context, hostport string) (auth.Credential, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	return credentials.Credential(c.store)(ctx, hostport)
}

// newHTTPClient builds the transport every registry request travels on.
//
// A configured TLS setting always takes the second branch. The first exists so
// a test that replaced the shared retry transport keeps its substitution, and
// honoring that ahead of an operator's certificate authority would drop the one
// piece of configuration whose absence they would then see only as an
// unexplained TLS failure.
func newHTTPClient(operationTimeout time.Duration, tlsConfig *tls.Config) *http.Client {
	if _, standardRetryTransport := retry.DefaultClient.Transport.(*retry.Transport); !standardRetryTransport && tlsConfig == nil {
		client := *retry.DefaultClient
		client.Timeout = operationTimeout
		return &client
	}

	dialer := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: 30 * time.Second,
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext
	transport.TLSHandshakeTimeout = defaultTLSHandshakeTimeout
	transport.ResponseHeaderTimeout = defaultResponseHeaderTimeout
	transport.IdleConnTimeout = defaultIdleConnectionTimeout
	transport.ExpectContinueTimeout = time.Second
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{
		Transport: retry.NewTransport(transport),
		Timeout:   operationTimeout,
	}
}

func mergeLimits(specific, fallback Limits) Limits {
	if specific == (Limits{}) {
		return fallback
	}
	return specific.normalized()
}
