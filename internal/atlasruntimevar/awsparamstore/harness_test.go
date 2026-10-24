package awsparamstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/smithy-go"
	qt "github.com/frankban/quicktest"
	"gocloud.dev/gcerrors"
	"gocloud.dev/runtimevar"
	gcparamstore "gocloud.dev/runtimevar/awsparamstore"

	"ptah.run/internal/atlasruntimevar/awsparamstore"
)

// driver is one implementation of the awsparamstore contract, opened either
// from a URL, the way atlas.hcl reaches it, or from an aws.Config, which lets
// a test give both implementations the same transport and retry backoff.
type driver struct {
	name       string
	openURL    func(context.Context, *url.URL) (*runtimevar.Variable, error)
	openConfig func(aws.Config, string, *runtimevar.Decoder, time.Duration) (*runtimevar.Variable, error)
}

// drivers are the Go CDK driver, which links the SDK's SSM client, and
// Ptah's. Every comparison runs the first as the reference.
var drivers = []driver{
	{
		name:    "gocloud.dev",
		openURL: (&gcparamstore.URLOpener{}).OpenVariableURL,
		openConfig: func(cfg aws.Config, name string, decoder *runtimevar.Decoder, wait time.Duration) (*runtimevar.Variable, error) {
			return gcparamstore.OpenVariable(ssm.NewFromConfig(cfg), name, decoder, &gcparamstore.Options{WaitDuration: wait})
		},
	},
	{
		name:    "ptah",
		openURL: (&awsparamstore.URLOpener{}).OpenVariableURL,
		openConfig: func(cfg aws.Config, name string, decoder *runtimevar.Decoder, wait time.Duration) (*runtimevar.Variable, error) {
			return awsparamstore.OpenVariable(cfg, name, decoder, &awsparamstore.Options{WaitDuration: wait})
		},
	},
}

// reply is one scripted SSM response.
type reply struct {
	status    int
	errorType string        // the X-Amzn-ErrorType header, when set
	body      string        // the response body
	dateShift time.Duration // when set, a Date header this far from the local clock
}

const testParameterARN = "arn:aws:ssm:us-east-1:123456789012:parameter/"

// found answers GetParameter with a parameter.
func found(name, kind, value string, version int64) reply {
	return reply{status: http.StatusOK, body: fmt.Sprintf(
		`{"Parameter":{"ARN":%q,"DataType":"text","LastModifiedDate":1.700000123456E9,"Name":%q,"Type":%q,"Value":%q,"Version":%d}}`,
		testParameterARN+strings.TrimPrefix(name, "/"), name, kind, value, version,
	)}
}

// failed answers with an error document.
func failed(status int, code, message string) reply {
	return reply{status: status, body: fmt.Sprintf(`{"__type":%q,"message":%q}`, code, message)}
}

// failedUpper answers with an error document whose message member is
// capitalized, as most SSM error shapes name it.
func failedUpper(status int, code, message string) reply {
	return reply{status: status, body: fmt.Sprintf(`{"__type":%q,"Message":%q}`, code, message)}
}

// request is what the fake saw of one request, reduced to the parts both
// implementations must agree on. The signature itself, the date and the
// invocation ID differ on every request by design.
type request struct {
	Method          string
	URL             string
	Target          string
	ContentType     string
	ContentLength   int64
	Body            string
	Signed          bool
	CredentialScope string
	SignedHeaders   string
	SDKRequest      string
	InvocationID    bool
	SigningSkew     int64 // minutes between X-Amz-Date and the fake's clock
	UserAgent       []string
}

// fakeSSM is an SSM endpoint scripted with replies: request n gets reply n,
// and requests past the script get the last reply. With a gate it holds each
// request until the test releases it, and reports each arrival, so a watch
// can be stepped one poll at a time.
type fakeSSM struct {
	mu       sync.Mutex
	replies  []reply
	requests []request
	stepped  bool
	gate     chan struct{}
	arrived  chan struct{}
}

func newFakeSSM(replies ...reply) *fakeSSM {
	return &fakeSSM{replies: replies}
}

// gated makes the fake hold every request until the test releases it.
func (f *fakeSSM) gated() *fakeSSM {
	f.stepped = true
	f.reset()
	return f
}

// reset forgets the requests and the gate state, for the next driver.
func (f *fakeSSM) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = nil
	if f.stepped {
		f.gate = make(chan struct{})
		f.arrived = make(chan struct{}, 64)
	}
}

func (f *fakeSSM) recorded() []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeSSM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	index := len(f.requests)
	f.requests = append(f.requests, recordRequest(r, body))
	gate, arrived := f.gate, f.arrived
	f.mu.Unlock()
	if gate != nil {
		arrived <- struct{}{}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}
	answer := f.replies[min(index, len(f.replies)-1)]
	w.Header().Set("X-Amzn-Requestid", fmt.Sprintf("request-%d", index+1))
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	if answer.errorType != "" {
		w.Header().Set("X-Amzn-ErrorType", answer.errorType)
	}
	if answer.dateShift != 0 {
		w.Header().Set("Date", time.Now().Add(answer.dateShift).UTC().Format(http.TimeFormat))
	}
	w.WriteHeader(answer.status)
	_, _ = io.WriteString(w, answer.body)
}

// RoundTrip serves a request in process, for a client whose endpoint is an
// AWS host name: the host is recorded, and nothing leaves the process.
func (f *fakeSSM) RoundTrip(r *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	f.ServeHTTP(recorder, r)
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	response := recorder.Result()
	response.Request = r
	return response, nil
}

func recordRequest(r *http.Request, body []byte) request {
	location := *r.URL
	if location.Host == "" {
		location.Scheme, location.Host = "http", r.Host
	}
	scope, signedHeaders := parseAuthorization(r.Header.Get("Authorization"))
	return request{
		Method:          r.Method,
		URL:             location.String(),
		Target:          r.Header.Get("X-Amz-Target"),
		ContentType:     r.Header.Get("Content-Type"),
		ContentLength:   r.ContentLength,
		Body:            string(body),
		Signed:          r.Header.Get("Authorization") != "",
		CredentialScope: scope,
		SignedHeaders:   signedHeaders,
		SDKRequest:      r.Header.Get("Amz-Sdk-Request"),
		InvocationID:    r.Header.Get("Amz-Sdk-Invocation-Id") != "",
		SigningSkew:     signingSkew(r.Header.Get("X-Amz-Date")),
		UserAgent:       userAgentTokens(r.Header.Get("User-Agent")),
	}
}

// parseAuthorization returns the credential scope without its date, and the
// signed header list, of a SigV4 Authorization header.
func parseAuthorization(header string) (scope, signedHeaders string) {
	for field := range strings.SplitSeq(strings.TrimPrefix(header, "AWS4-HMAC-SHA256 "), ", ") {
		key, value, _ := strings.Cut(field, "=")
		switch key {
		case "Credential":
			parts := strings.Split(value, "/")
			scope = strings.Join(slices.Delete(parts, 1, 2), "/")
		case "SignedHeaders":
			signedHeaders = value
		}
	}
	return scope, signedHeaders
}

func signingSkew(header string) int64 {
	signed, err := time.Parse("20060102T150405Z", header)
	if err != nil {
		return 0
	}
	return int64(math.Round(time.Until(signed).Minutes()))
}

// userAgentTokens drops the api/ token: the SDK client names its own module
// version there, and Ptah's client has no SSM module to name.
func userAgentTokens(header string) []string {
	return slices.DeleteFunc(strings.Fields(header), func(token string) bool {
		return strings.HasPrefix(token, "api/")
	})
}

// isolateAWSEnvironment removes every AWS_ variable of the developer's
// environment and points the shared files at paths that do not exist, so the
// default chain sees only what the test sets.
func isolateAWSEnvironment(c *qt.C) {
	for _, entry := range os.Environ() {
		if name, _, _ := strings.Cut(entry, "="); strings.HasPrefix(name, "AWS_") {
			c.Unsetenv(name)
		}
	}
	dir := c.TempDir()
	c.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	c.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	c.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// staticCredentials are the credentials most scenarios sign with.
var staticCredentials = map[string]string{
	"AWS_ACCESS_KEY_ID":     "AKIDPTAHTEST",
	"AWS_SECRET_ACCESS_KEY": "ptah-test-secret",
}

func setEnvironment(c *qt.C, variables ...map[string]string) {
	for _, set := range variables {
		for name, value := range set {
			c.Setenv(name, value)
		}
	}
}

// parameterView is a snapshot's GetParameter output, read through As.
type parameterView struct {
	ARN, DataType, Name, Selector, SourceResult, Type, Value string
	LastModifiedDate                                         time.Time
	Version                                                  int64
	RequestID                                                string
}

// errorView is what a caller can learn from an error.
type errorView struct {
	Text          string
	Code          gcerrors.ErrorCode
	VariableAs    bool
	APICode       string
	APIMessage    string
	APIFault      smithy.ErrorFault
	Operation     string
	StatusCode    int
	RequestID     string
	MaxAttempts   int
	Deserializing bool
}

// observation is one state a variable reported, or that it reported none.
type observation struct {
	NoChange   bool
	Value      any
	UpdateTime time.Time
	Parameter  *parameterView
	Error      *errorView
}

// observe reads what Watch returned under ctx. Watch answers ctx's own error
// when no new state arrived in time.
func observe(ctx context.Context, variable *runtimevar.Variable, snapshot runtimevar.Snapshot, err error) observation {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return observation{NoChange: true}
	}
	if err != nil {
		return observation{Error: viewError(variable, err)}
	}
	return observation{Value: snapshot.Value, UpdateTime: snapshot.UpdateTime, Parameter: viewParameter(snapshot)}
}

func viewParameter(snapshot runtimevar.Snapshot) *parameterView {
	var sdk *ssm.GetParameterOutput
	if snapshot.As(&sdk) {
		if sdk.Parameter == nil {
			return nil
		}
		p := sdk.Parameter
		requestID, _ := awsmiddleware.GetRequestIDMetadata(sdk.ResultMetadata)
		return &parameterView{
			ARN: aws.ToString(p.ARN), DataType: aws.ToString(p.DataType), Name: aws.ToString(p.Name),
			Selector: aws.ToString(p.Selector), SourceResult: aws.ToString(p.SourceResult), Type: string(p.Type),
			Value: aws.ToString(p.Value), LastModifiedDate: aws.ToTime(p.LastModifiedDate), Version: p.Version,
			RequestID: requestID,
		}
	}
	var own *awsparamstore.GetParameterOutput
	if snapshot.As(&own) {
		if own.Parameter == nil {
			return nil
		}
		p := own.Parameter
		requestID, _ := awsmiddleware.GetRequestIDMetadata(own.ResultMetadata)
		return &parameterView{
			ARN: aws.ToString(p.ARN), DataType: aws.ToString(p.DataType), Name: aws.ToString(p.Name),
			Selector: aws.ToString(p.Selector), SourceResult: aws.ToString(p.SourceResult), Type: p.Type,
			Value: aws.ToString(p.Value), LastModifiedDate: aws.ToTime(p.LastModifiedDate), Version: p.Version,
			RequestID: requestID,
		}
	}
	return nil
}

func viewError(variable *runtimevar.Variable, err error) *errorView {
	view := &errorView{Text: err.Error(), Code: gcerrors.Code(err)}
	var target smithy.APIError
	view.VariableAs = variable.ErrorAs(err, &target)
	if apiError, ok := errors.AsType[smithy.APIError](err); ok {
		view.APICode, view.APIMessage, view.APIFault = apiError.ErrorCode(), apiError.ErrorMessage(), apiError.ErrorFault()
	}
	if operation, ok := errors.AsType[*smithy.OperationError](err); ok {
		view.Operation = operation.Service() + "." + operation.Operation()
	}
	if response, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		view.StatusCode, view.RequestID = response.HTTPStatusCode(), response.ServiceRequestID()
	}
	if attempts, ok := errors.AsType[*retry.MaxAttemptsError](err); ok {
		view.MaxAttempts = attempts.Attempt
	}
	_, view.Deserializing = errors.AsType[*smithy.DeserializationError](err)
	return view
}

// pollTimeout bounds a single observation that is expected to arrive.
const pollTimeout = 10 * time.Second

// noChangeWindow is how long a stepped watch waits to conclude that a poll
// produced no new state.
const noChangeWindow = 150 * time.Millisecond

// firstState opens the variable and returns its first state.
func firstState(c *qt.C, open func() (*runtimevar.Variable, error)) observation {
	variable, err := open()
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(variable.Close(), qt.IsNil) }()
	ctx, cancel := context.WithTimeout(context.Background(), pollTimeout)
	defer cancel()
	snapshot, err := variable.Watch(ctx)
	return observe(ctx, variable, snapshot, err)
}

// steppedStates opens a variable over a gated fake and releases one reply at
// a time. After each reply it waits for the next poll to arrive, which proves
// the previous one was processed, and then asks for a new state: a poll that
// produced none reads as NoChange.
func steppedStates(c *qt.C, fake *fakeSSM, open func() (*runtimevar.Variable, error)) []observation {
	variable, err := open()
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(variable.Close(), qt.IsNil) }()
	fake.mu.Lock()
	gate, arrived := fake.gate, fake.arrived
	fake.mu.Unlock()
	awaitArrival(c, arrived)
	observations := make([]observation, 0, len(fake.replies))
	for range fake.replies {
		gate <- struct{}{}
		awaitArrival(c, arrived)
		ctx, cancel := context.WithTimeout(context.Background(), noChangeWindow)
		snapshot, err := variable.Watch(ctx)
		observations = append(observations, observe(ctx, variable, snapshot, err))
		cancel()
	}
	return observations
}

func awaitArrival(c *qt.C, arrived <-chan struct{}) {
	select {
	case <-arrived:
	case <-time.After(pollTimeout):
		c.Fatal("the variable sent no request")
	}
}

// zeroBackoffRetryer is the SDK's standard retryer without its delay, so a
// retried call takes no wall time; the retry decision is unchanged.
func zeroBackoffRetryer() aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
	})
}

// testConfig loads the default configuration with the retryer above and the
// fake as transport.
func testConfig(c *qt.C, fake http.RoundTripper, options ...func(*config.LoadOptions) error) aws.Config {
	options = append(options, config.WithRetryer(zeroBackoffRetryer))
	cfg, err := config.LoadDefaultConfig(context.Background(), options...)
	c.Assert(err, qt.IsNil)
	cfg.HTTPClient = &http.Client{Transport: fake}
	return cfg
}
