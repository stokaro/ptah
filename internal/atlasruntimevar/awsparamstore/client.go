package awsparamstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	sigv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// client sends GetParameter. It is assembled from the SDK's own middleware
// (retry, payload hash, request ID, response error wrapping, clock skew
// timing, user agent) in the order the SDK's SSM client uses, with the request
// codec, the endpoint rules and the SigV4 step written here, because those are
// the parts the SDK generates per service.
type client struct {
	region                     string
	credentials                aws.CredentialsProvider
	authSchemePreference       []string
	httpClient                 aws.HTTPClient
	retryer                    aws.Retryer
	signer                     *sigv4.Signer
	logger                     logging.Logger
	logMode                    aws.ClientLogMode
	appID                      string
	disableClockSkewCorrection bool
	endpoints                  endpointOptions

	// timeOffset is the clock skew the last call measured, the starting point
	// of the next call's signing time.
	timeOffset atomic.Int64
}

// newClient reads cfg the way the SDK's ssm.NewFromConfig does. The URL
// opener's configuration never carries APIOptions, Interceptors or
// ServiceOptions, which only a program can set, so they are not read.
func newClient(cfg aws.Config) *client {
	region := cfg.Region
	mode := resolvedDefaultsMode(cfg)
	logger := cfg.Logger
	if logger == nil {
		logger = logging.Nop{}
	}
	credentials := cfg.Credentials
	if aws.IsCredentialsProvider(credentials, (*aws.AnonymousCredentials)(nil)) {
		credentials = nil
	}
	return &client{
		region:                     region,
		credentials:                credentials,
		authSchemePreference:       cfg.AuthSchemePreference,
		httpClient:                 resolveHTTPClient(cfg.HTTPClient, mode),
		retryer:                    resolveRetryer(cfg, mode),
		signer:                     sigv4.NewSigner(func(o *sigv4.SignerOptions) { o.Logger = logger; o.LogSigning = cfg.ClientLogMode.IsSigning() }),
		logger:                     logger,
		logMode:                    cfg.ClientLogMode,
		appID:                      cfg.AppID,
		disableClockSkewCorrection: cfg.DisableClockSkewCorrection,
		endpoints:                  newEndpointOptions(cfg),
	}
}

func resolvedDefaultsMode(cfg aws.Config) aws.DefaultsMode {
	var mode aws.DefaultsMode
	mode.SetFromString(string(cfg.DefaultsMode))
	if mode == aws.DefaultsModeAuto {
		mode = defaults.ResolveDefaultsModeAuto(cfg.Region, cfg.RuntimeEnvironment)
	}
	return mode
}

// ssmReadTimeout is the inactivity timeout the SDK gives SSM responses when
// AWS_ENABLE_DEFAULT_SOCKET_TIMEOUT_2026 is true.
const ssmReadTimeout = 15 * time.Minute

func resolveHTTPClient(configured aws.HTTPClient, mode aws.DefaultsMode) aws.HTTPClient {
	var buildable *awshttp.BuildableClient
	if configured != nil {
		var ok bool
		if buildable, ok = configured.(*awshttp.BuildableClient); !ok {
			return configured
		}
	} else {
		buildable = awshttp.NewBuildableClient()
	}
	if modeConfig, err := defaults.GetModeConfiguration(mode); err == nil {
		buildable = buildable.WithDialerOptions(func(dialer *net.Dialer) {
			if timeout, ok := modeConfig.GetConnectTimeout(); ok {
				dialer.Timeout = timeout
			}
		})
		buildable = buildable.WithTransportOptions(func(transport *http.Transport) {
			if timeout, ok := modeConfig.GetTLSNegotiationTimeout(); ok {
				transport.TLSHandshakeTimeout = timeout
			}
		})
	}
	if _, ok := buildable.GetReadTimeout(); !ok && os.Getenv("AWS_ENABLE_DEFAULT_SOCKET_TIMEOUT_2026") == "true" {
		buildable = buildable.WithReadTimeout(ssmReadTimeout)
	}
	return buildable
}

// resolveRetryer is the configuration's retryer, or the defaults mode's when
// it has none, bounded by the configuration's attempt limit. The URL opener's
// configuration always carries the standard retryer gocloud.dev installs.
func resolveRetryer(cfg aws.Config, mode aws.DefaultsMode) aws.Retryer {
	var retryer aws.Retryer
	if cfg.Retryer != nil {
		retryer = cfg.Retryer()
	} else {
		retryer = defaultRetryer(cfg, mode)
	}
	if cfg.RetryMaxAttempts != 0 {
		retryer = retry.AddWithMaxAttempts(retryer, cfg.RetryMaxAttempts)
	}
	return retryer
}

func defaultRetryer(cfg aws.Config, mode aws.DefaultsMode) aws.Retryer {
	retryMode := cfg.RetryMode
	if modeConfig, err := defaults.GetModeConfiguration(mode); retryMode == "" && err == nil {
		retryMode = modeConfig.RetryMode
	}
	var standard []func(*retry.StandardOptions)
	if attempts := cfg.RetryMaxAttempts; attempts != 0 {
		standard = append(standard, func(o *retry.StandardOptions) { o.MaxAttempts = attempts })
	}
	if retryMode == aws.RetryModeAdaptive {
		return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
			o.StandardOptions = append(o.StandardOptions, standard...)
		})
	}
	return retry.NewStandard(standard...)
}

// getParameter sends one GetParameter call, with retries, and returns the
// error the SDK's client would: an *smithy.OperationError naming SSM and
// GetParameter around whatever the attempt failed with.
func (c *client) getParameter(ctx context.Context, name string) (*GetParameterOutput, error) {
	ctx = middleware.ClearStackValues(ctx)
	ctx = middleware.WithServiceID(ctx, serviceID)
	ctx = middleware.WithOperationName(ctx, operationName)
	ctx = middleware.SetLogger(ctx, c.logger)
	ctx = awsmiddleware.SetServiceID(ctx, serviceID)
	if c.region != "" {
		ctx = awsmiddleware.SetRegion(ctx, c.region)
	}
	ctx = awsmiddleware.SetOperationName(ctx, operationName)
	if c.endpoints.legacy != nil {
		ctx = awsmiddleware.SetRequiresLegacyEndpoints(ctx, true)
	}

	stack := middleware.NewStack(operationName, smithyhttp.NewStackRequest)
	if err := c.addMiddleware(stack); err != nil {
		return nil, &smithy.OperationError{ServiceID: serviceID, OperationName: operationName, Err: err}
	}
	handler := middleware.DecorateHandler(smithyhttp.NewClientHandlerWithOptions(c.httpClient), stack)
	result, metadata, err := handler.Handle(ctx, &getParameterInput{name: name})
	if err != nil {
		return nil, &smithy.OperationError{ServiceID: serviceID, OperationName: operationName, Err: err}
	}
	out, ok := result.(*GetParameterOutput)
	if !ok {
		return nil, &smithy.OperationError{
			ServiceID: serviceID, OperationName: operationName,
			Err: fmt.Errorf("unexpected result type %T", result),
		}
	}
	out.ResultMetadata = metadata
	return out, nil
}

func (c *client) addMiddleware(stack *middleware.Stack) error {
	output := &GetParameterOutput{}
	userAgent := c.userAgent()
	return errors.Join(
		stack.Serialize.Add(&legacyEndpointMiddleware{resolver: c.endpoints.legacy}, middleware.After),
		stack.Serialize.Add(serializeMiddleware{}, middleware.After),

		stack.Build.Add(&awsmiddleware.ClientRequestID{}, middleware.After),
		stack.Build.Add(userAgent, middleware.After),
		stack.Build.Add(&awsmiddleware.RecursionDetection{}, middleware.After),

		stack.Finalize.Add(retry.NewAttemptMiddleware(c.retryer, smithyhttp.RequestCloner, func(m *retry.Attempt) {
			m.LogAttempts = c.logMode.IsRetries()
			m.ClientSkew = &c.timeOffset
			m.DisableClockSkewCorrection = c.disableClockSkewCorrection
		}), middleware.After),
		stack.Finalize.Add(&attemptSkewMiddleware{clientSkew: &c.timeOffset, disabled: c.disableClockSkewCorrection}, middleware.After),
		stack.Finalize.Add(&authSchemeMiddleware{client: c}, middleware.After),
		stack.Finalize.Add(&identityMiddleware{client: c}, middleware.After),
		stack.Finalize.Add(&endpointMiddleware{client: c}, middleware.After),
		stack.Finalize.Add(&sigv4.ComputePayloadSHA256{}, middleware.After),
		stack.Finalize.Add(legacySigningMiddleware{}, middleware.After),
		stack.Finalize.Add(&signMiddleware{client: c}, middleware.After),

		stack.Deserialize.Add(&awsmiddleware.AddRawResponse{}, middleware.After),
		stack.Deserialize.Add(&awshttp.ResponseErrorWrapper{}, middleware.After),
		stack.Deserialize.Add(&awsmiddleware.RequestIDRetriever{}, middleware.After),
		stack.Deserialize.Add(&deserializeMiddleware{output: output}, middleware.After),
		stack.Deserialize.Add(&awsmiddleware.RecordResponseTiming{DisableClockSkewCorrection: c.disableClockSkewCorrection}, middleware.After),
		stack.Deserialize.Add(&smithyhttp.RequestResponseLogger{
			LogRequest:          c.logMode.IsRequest(),
			LogRequestWithBody:  c.logMode.IsRequestWithBody(),
			LogResponse:         c.logMode.IsResponse(),
			LogResponseWithBody: c.logMode.IsResponseWithBody(),
		}, middleware.After),
	)
}

func (c *client) userAgent() *awsmiddleware.RequestUserAgent {
	userAgent := awsmiddleware.NewRequestUserAgent()
	if c.appID != "" {
		userAgent.AddSDKAgentKey(awsmiddleware.ApplicationIdentifier, c.appID)
	}
	switch c.retryer.(type) {
	case *retry.Standard:
		userAgent.AddUserAgentFeature(awsmiddleware.UserAgentFeatureRetryModeStandard)
	case *retry.AdaptiveMode:
		userAgent.AddUserAgentFeature(awsmiddleware.UserAgentFeatureRetryModeAdaptive)
	}
	if sources, ok := c.credentials.(aws.CredentialProviderSource); ok {
		for _, source := range sources.ProviderSources() {
			userAgent.AddCredentialsSource(source)
		}
	}
	return userAgent
}

// legacyEndpointMiddleware asks an endpoint resolver the configuration
// carries, as the gocloud.dev endpoint URL parameter installs one. A resolver
// that answers aws.EndpointNotFoundError hands the request to the rule set.
type legacyEndpointMiddleware struct {
	resolver legacyResolver
}

type legacySigningKey struct{}

// legacySigning is the signing name and region a configured resolver chose.
type legacySigning struct {
	name   string
	region string
}

func (*legacyEndpointMiddleware) ID() string { return "ResolveEndpoint" }

func (m *legacyEndpointMiddleware) HandleSerialize(ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler) (
	middleware.SerializeOutput, middleware.Metadata, error,
) {
	if !awsmiddleware.GetRequiresLegacyEndpoints(ctx) {
		return next.HandleSerialize(ctx, in)
	}
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return middleware.SerializeOutput{}, middleware.Metadata{}, fmt.Errorf("unknown transport type %T", in.Request)
	}
	endpoint, err := m.resolver(serviceID, awsmiddleware.GetRegion(ctx))
	if err != nil {
		if _, notFound := errors.AsType[*aws.EndpointNotFoundError](err); notFound {
			return next.HandleSerialize(awsmiddleware.SetRequiresLegacyEndpoints(ctx, false), in)
		}
		return middleware.SerializeOutput{}, middleware.Metadata{}, fmt.Errorf("failed to resolve service endpoint, %w", err)
	}
	if req.URL, err = url.Parse(endpoint.url); err != nil {
		return middleware.SerializeOutput{}, middleware.Metadata{}, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}
	signing := &legacySigning{name: endpoint.signingName, region: endpoint.signingRegion}
	if signing.name == "" {
		signing.name = signingName
	}
	return next.HandleSerialize(middleware.WithStackValue(ctx, legacySigningKey{}, signing), in)
}

// serializeMiddleware writes the awsJson1_1 request.
type serializeMiddleware struct{}

func (serializeMiddleware) ID() string { return "OperationSerializer" }

func (serializeMiddleware) HandleSerialize(ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler) (
	middleware.SerializeOutput, middleware.Metadata, error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return middleware.SerializeOutput{}, middleware.Metadata{}, fmt.Errorf("unexpected transport type %T", in.Request)
	}
	input, ok := in.Parameters.(smithy.Serializable)
	if !ok {
		return middleware.SerializeOutput{}, middleware.Metadata{}, fmt.Errorf("input %T is not Serializable", in.Parameters)
	}
	s := schemas()
	if err := s.protocol.SerializeRequest(ctx, s.operation, input, req); err != nil {
		return middleware.SerializeOutput{}, middleware.Metadata{}, err
	}
	return next.HandleSerialize(ctx, in)
}

// deserializeMiddleware reads the awsJson1_1 response, or the error it
// carries, through the type registry of modeled errors.
type deserializeMiddleware struct {
	output *GetParameterOutput
}

func (*deserializeMiddleware) ID() string { return "OperationDeserializer" }

func (m *deserializeMiddleware) HandleDeserialize(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
	out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	resp, ok := out.RawResponse.(*smithyhttp.Response)
	if !ok {
		if err != nil {
			return out, metadata, err
		}
		return out, metadata, &smithy.DeserializationError{Err: fmt.Errorf("unknown transport type %T", out.RawResponse)}
	}
	defer func() {
		smithyhttp.CloseResponseBody(ctx, resp, false, err)
	}()
	if err != nil {
		return out, metadata, err
	}
	s := schemas()
	err = s.protocol.DeserializeResponse(ctx, s.operation, s.registry, resp, m.output)
	out.Result = m.output
	return out, metadata, err
}

type attemptSkewKey struct{}

// attemptSkewMiddleware carries the clock skew into each attempt's signing
// time: the client's last measurement for the first attempt, and the skew the
// previous attempt's response measured for every retry. The SDK's retry
// middleware hands the same value to its own signer through a context key no
// other module can read, so this step tracks it alongside.
type attemptSkewMiddleware struct {
	clientSkew *atomic.Int64
	disabled   bool
	started    bool
	skew       time.Duration
}

func (*attemptSkewMiddleware) ID() string { return "AttemptClockSkew" }

func (m *attemptSkewMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error,
) {
	if !m.started {
		m.started = true
		if !m.disabled {
			m.skew = time.Duration(m.clientSkew.Load())
		}
	}
	out, metadata, err := next.HandleFinalize(context.WithValue(ctx, attemptSkewKey{}, m.skew), in)
	m.skew, _ = awsmiddleware.GetAttemptSkew(metadata)
	return out, metadata, err
}

type authSchemeKey struct{}

// authScheme is the selected scheme and the SigV4 signer properties.
type authScheme struct {
	anonymous     bool
	signingName   string
	signingRegion string
}

const (
	sigV4SchemeID     = "aws.auth#sigv4"
	anonymousSchemeID = "smithy.api#noAuth"
)

// authSchemeMiddleware selects SigV4 when the configuration has credentials,
// and no signature otherwise. A preference the configuration states
// (auth_scheme_preference) is honored before that order.
type authSchemeMiddleware struct {
	client *client
}

func (*authSchemeMiddleware) ID() string { return "ResolveAuthScheme" }

func (m *authSchemeMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error,
) {
	for _, scheme := range sortedSchemes(m.client.authSchemePreference) {
		if scheme == anonymousSchemeID {
			return next.HandleFinalize(middleware.WithStackValue(ctx, authSchemeKey{}, &authScheme{anonymous: true}), in)
		}
		if m.client.credentials != nil {
			selected := &authScheme{signingName: signingName, signingRegion: m.client.region}
			return next.HandleFinalize(middleware.WithStackValue(ctx, authSchemeKey{}, selected), in)
		}
	}
	return middleware.FinalizeOutput{}, middleware.Metadata{}, errors.New("could not select an auth scheme")
}

// sortedSchemes orders SigV4 and anonymous access by the preference list,
// which names schemes without their namespace; unnamed schemes keep their
// order after the named ones.
func sortedSchemes(preference []string) []string {
	schemes := []string{sigV4SchemeID, anonymousSchemeID}
	sorted := make([]string, 0, len(schemes))
	for _, preferred := range preference {
		for _, scheme := range schemes {
			if _, name, _ := strings.Cut(scheme, "#"); name == preferred && !slices.Contains(sorted, scheme) {
				sorted = append(sorted, scheme)
			}
		}
	}
	for _, scheme := range schemes {
		if !slices.Contains(sorted, scheme) {
			sorted = append(sorted, scheme)
		}
	}
	return sorted
}

type credentialsKey struct{}

// identityMiddleware retrieves the credentials the selected scheme signs
// with. The configuration's provider caches them.
type identityMiddleware struct {
	client *client
}

func (*identityMiddleware) ID() string { return "GetIdentity" }

func (m *identityMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error,
) {
	scheme, _ := middleware.GetStackValue(ctx, authSchemeKey{}).(*authScheme)
	if scheme == nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, errors.New("no resolved auth scheme")
	}
	if scheme.anonymous {
		return next.HandleFinalize(ctx, in)
	}
	credentials, err := m.client.credentials.Retrieve(ctx)
	if err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("get identity: get credentials: %w", err)
	}
	return next.HandleFinalize(middleware.WithStackValue(ctx, credentialsKey{}, credentials), in)
}

// endpointMiddleware applies the SSM endpoint rule set, unless an endpoint
// resolver the configuration carries already chose the endpoint.
type endpointMiddleware struct {
	client *client
}

func (*endpointMiddleware) ID() string { return "ResolveEndpointV2" }

func (m *endpointMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error,
) {
	if awsmiddleware.GetRequiresLegacyEndpoints(ctx) {
		return next.HandleFinalize(ctx, in)
	}
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("unknown transport type %T", in.Request)
	}
	params, err := m.client.endpoints.parameters(m.client.region)
	if err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("failed to bind endpoint params, %w", err)
	}
	endpoint, err := resolveEndpoint(params)
	if err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("failed to resolve service endpoint, %w", err)
	}
	if endpoint.RawPath == "" && req.URL.RawPath != "" {
		endpoint.RawPath = endpoint.Path
	}
	req.URL.Scheme = endpoint.Scheme
	req.URL.Host = endpoint.Host
	req.URL.Path = smithyhttp.JoinPath(endpoint.Path, req.URL.Path)
	req.URL.RawPath = smithyhttp.JoinPath(endpoint.RawPath, req.URL.RawPath)
	return next.HandleFinalize(ctx, in)
}

// legacySigningMiddleware lets an endpoint resolver the configuration carries
// set the signing name and region, as the SDK does for such a resolver: the
// name always, the region when the resolver named one.
type legacySigningMiddleware struct{}

func (legacySigningMiddleware) ID() string { return "setLegacyContextSigningOptions" }

func (legacySigningMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error,
) {
	scheme, _ := middleware.GetStackValue(ctx, authSchemeKey{}).(*authScheme)
	signing, _ := middleware.GetStackValue(ctx, legacySigningKey{}).(*legacySigning)
	if scheme != nil && !scheme.anonymous && signing != nil {
		scheme.signingName = signing.name
		if signing.region != "" {
			scheme.signingRegion = signing.region
		}
	}
	return next.HandleFinalize(ctx, in)
}

// signMiddleware signs the request with SigV4, at the local time corrected by
// the attempt's clock skew. Anonymous access sends it unsigned.
type signMiddleware struct {
	client *client
}

func (*signMiddleware) ID() string { return "Signing" }

func (m *signMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("unexpected transport type %T", in.Request)
	}
	scheme, _ := middleware.GetStackValue(ctx, authSchemeKey{}).(*authScheme)
	if scheme == nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, errors.New("no resolved auth scheme")
	}
	if scheme.anonymous {
		return next.HandleFinalize(ctx, in)
	}
	credentials, _ := middleware.GetStackValue(ctx, credentialsKey{}).(aws.Credentials)
	skew, _ := ctx.Value(attemptSkewKey{}).(time.Duration)
	signingTime := time.Now().Add(skew)
	err := m.client.signer.SignHTTP(ctx, credentials, req.Request, sigv4.GetPayloadHash(ctx),
		scheme.signingName, scheme.signingRegion, signingTime)
	if err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("sign request: sign http: %w", err)
	}
	return next.HandleFinalize(ctx, in)
}
