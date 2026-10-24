package awsparamstore_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	qt "github.com/frankban/quicktest"
	"gocloud.dev/gcerrors"
	"gocloud.dev/runtimevar"
)

// outcome is what one driver produced for a scenario.
type outcome struct {
	States   []observation
	Requests []request
}

// compareDrivers runs a scenario once per driver against the same fake, and
// returns the reference outcome and Ptah's.
func compareDrivers(c *qt.C, fake *fakeSSM, run func(*qt.C, driver) []observation) (reference, candidate outcome) {
	outcomes := make([]outcome, 0, len(drivers))
	for _, d := range drivers {
		fake.reset()
		states := run(c, d)
		outcomes = append(outcomes, outcome{States: states, Requests: fake.recorded()})
	}
	return outcomes[0], outcomes[1]
}

// urlScenario opens an awsparamstore URL. {endpoint} in the URL is replaced by
// the fake's address, query-escaped; in env, unescaped.
type urlScenario struct {
	name          string
	url           string
	env           map[string]string
	noCredentials bool
	replies       []reply
	wantRequests  int
}

func openURLScenario(c *qt.C, row urlScenario, endpoint string, d driver) func() (*runtimevar.Variable, error) {
	isolateAWSEnvironment(c)
	environment := make(map[string]string, len(row.env))
	for name, value := range row.env {
		environment[name] = strings.ReplaceAll(value, "{endpoint}", endpoint)
	}
	setEnvironment(c, environment)
	if !row.noCredentials {
		setEnvironment(c, staticCredentials)
	}
	u, err := url.Parse(strings.ReplaceAll(row.url, "{endpoint}", url.QueryEscape(endpoint)))
	c.Assert(err, qt.IsNil)
	return func() (*runtimevar.Variable, error) {
		return d.openURL(context.Background(), u)
	}
}

func serve(c *qt.C, fake *fakeSSM) string {
	server := httptest.NewServer(fake)
	c.Cleanup(server.Close)
	return server.URL
}

const value = "postgres://ptah@db.internal/app"

// TestParameterStoreURL_HappyPath opens URLs that deliver a value, through
// both drivers, and requires the same value, snapshot and requests.
func TestParameterStoreURL_HappyPath(t *testing.T) {
	tests := []struct {
		urlScenario
		wantValue any
	}{
		{urlScenario: urlScenario{
			name:    "a String parameter reaches the bytes decoder",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{found("ptah-db", "String", value, 3)}, wantRequests: 1,
		}, wantValue: []byte(value)},
		{urlScenario: urlScenario{
			name:    "a leading-slash path names the parameter",
			url:     "awsparamstore:///ptah/prod/db?region=eu-west-1&endpoint={endpoint}&decoder=string&wait=1h",
			replies: []reply{found("/ptah/prod/db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "the host and path join into the name",
			url:     "awsparamstore://ptah/prod/db?region=eu-west-1&endpoint={endpoint}&decoder=string&wait=1h",
			replies: []reply{found("ptah/prod/db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "the jsonmap decoder decodes the value",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&decoder=jsonmap&wait=1h",
			replies: []reply{found("ptah-db", "String", `{"user":"ptah","port":5432}`, 1)}, wantRequests: 1,
		}, wantValue: &map[string]any{"user": "ptah", "port": float64(5432)}},
		{urlScenario: urlScenario{
			name:    "a SecureString arrives decrypted",
			url:     "awsparamstore://ptah-secret?region=us-east-1&endpoint={endpoint}&decoder=string&wait=1h",
			replies: []reply{found("ptah-secret", "SecureString", "s3cr3t", 7)}, wantRequests: 1,
		}, wantValue: "s3cr3t"},
		{urlScenario: urlScenario{
			name:    "an empty value is a value",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{found("ptah-db", "String", "", 1)}, wantRequests: 1,
		}, wantValue: make([]byte, 0)},
		{urlScenario: urlScenario{
			name:    "an endpoint path prefixes the request path",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}%2Fprefix&decoder=string&wait=1h",
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "the endpoint parameter is used as given beside fips",
			url:     "awsparamstore://ptah-db?region=us-east-1&fips=true&dualstack=true&endpoint={endpoint}&decoder=string&wait=1h",
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "the region comes from the environment",
			url:     "awsparamstore://ptah-db?endpoint={endpoint}&decoder=string&wait=1h",
			env:     map[string]string{"AWS_REGION": "ap-southeast-2"},
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "anonymous access sends no signature",
			url:     "awsparamstore://ptah-db?region=us-east-1&anonymous=true&endpoint={endpoint}&decoder=string&wait=1h",
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "an auth scheme preference for no signature",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&decoder=string&wait=1h",
			env:     map[string]string{"AWS_AUTH_SCHEME_PREFERENCE": "noAuth"},
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "a session token is signed",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&decoder=string&wait=1h",
			env:     map[string]string{"AWS_SESSION_TOKEN": "ptah-session"},
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "AWS_ENDPOINT_URL_SSM names the endpoint",
			url:     "awsparamstore://ptah-db?region=us-east-1&decoder=string&wait=1h",
			env:     map[string]string{"AWS_ENDPOINT_URL_SSM": "{endpoint}"},
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name:    "AWS_ENDPOINT_URL names the endpoint",
			url:     "awsparamstore://ptah-db?region=us-east-1&decoder=string&wait=1h",
			env:     map[string]string{"AWS_ENDPOINT_URL": "{endpoint}"},
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
		{urlScenario: urlScenario{
			name: "AWS_ENDPOINT_URL_SSM wins over AWS_ENDPOINT_URL",
			url:  "awsparamstore://ptah-db?region=us-east-1&decoder=string&wait=1h",
			env: map[string]string{
				"AWS_ENDPOINT_URL":     "http://127.0.0.1:9/never",
				"AWS_ENDPOINT_URL_SSM": "{endpoint}",
			},
			replies: []reply{found("ptah-db", "String", value, 1)}, wantRequests: 1,
		}, wantValue: value},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFakeSSM(test.replies...)
			endpoint := serve(c, fake)

			reference, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
				return []observation{firstState(c, openURLScenario(c, test.urlScenario, endpoint, d))}
			})

			c.Assert(candidate, qt.DeepEquals, reference)
			c.Assert(reference.States[0].Value, qt.DeepEquals, test.wantValue)
			c.Assert(reference.Requests, qt.HasLen, test.wantRequests)
		})
	}
}

// TestParameterStoreURL_FailurePath opens URLs whose first poll fails, and
// requires the same error, the same NotFound mapping and the same requests.
func TestParameterStoreURL_FailurePath(t *testing.T) {
	tests := []struct {
		urlScenario
		wantError string
		wantCode  gcerrors.ErrorCode
	}{
		{urlScenario: urlScenario{
			name:    "a missing parameter is NotFound",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{failed(http.StatusBadRequest, "ParameterNotFound", "")}, wantRequests: 1,
		}, wantError: `runtimevar \(code=NotFound\): operation error SSM: GetParameter, https response error StatusCode: 400, RequestID: request-1, ParameterNotFound: `, wantCode: gcerrors.NotFound},
		{urlScenario: urlScenario{
			name:    "a modeled error with a capitalized message member",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{failedUpper(http.StatusBadRequest, "AccessDeniedException", "User: ptah is not authorized")}, wantRequests: 1,
		}, wantError: `runtimevar \(code=Unknown\): operation error SSM: GetParameter, https response error StatusCode: 400, RequestID: request-1, AccessDeniedException: User: ptah is not authorized`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "a modeled error reads its message under the other spelling",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{failedUpper(http.StatusBadRequest, "ParameterNotFound", "capitalized")}, wantRequests: 1,
		}, wantError: `.*, ParameterNotFound: capitalized`, wantCode: gcerrors.NotFound},
		{urlScenario: urlScenario{
			name:    "an error code no shape models",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{failed(http.StatusBadRequest, "PtahUnmodeledException", "new in the service")}, wantRequests: 1,
		}, wantError: `.*, api error PtahUnmodeledException: new in the service`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name: "the error type header wins over the body",
			url:  "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{{
				status: http.StatusBadRequest, errorType: "ParameterNotFound:http://internal.amazon.com/coral/com.amazonaws.ssm/",
				body: `{"__type":"AccessDeniedException","message":"from the body"}`,
			}}, wantRequests: 1,
		}, wantError: `.*, ParameterNotFound: from the body`, wantCode: gcerrors.NotFound},
		{urlScenario: urlScenario{
			name:    "a namespaced error type",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{failed(http.StatusBadRequest, "com.amazonaws.ssm#ParameterVersionNotFound", "no version 9")}, wantRequests: 1,
		}, wantError: `.*, ParameterVersionNotFound: no version 9`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "a response without a parameter",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{{status: http.StatusOK, body: `{}`}}, wantRequests: 1,
		}, wantError: `runtimevar \(code=Unknown\): unable to get "ptah-db" parameter`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "a malformed success body",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{{status: http.StatusOK, body: `{"Parameter":`}}, wantRequests: 1,
		}, wantError: `.*deserialization failed.*`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "a malformed error body",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			replies: []reply{{status: http.StatusBadRequest, body: `not json`}}, wantRequests: 1,
		}, wantError: `.*failed to decode response body.*`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "the jsonmap decoder refuses a value that is not JSON",
			url:     "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&decoder=jsonmap&wait=1h",
			replies: []reply{found("ptah-db", "String", "not json", 1)}, wantRequests: 1,
		}, wantError: `runtimevar \(code=Unknown\): invalid character 'o' in literal null \(expecting 'u'\)`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "a region that is not a host name",
			url:     "awsparamstore://ptah-db?region=bad%20region&wait=1h",
			env:     map[string]string{"AWS_ENDPOINT_URL_SSM": "{endpoint}"},
			replies: []reply{found("ptah-db", "String", value, 1)},
		}, wantError: `runtimevar \(code=Unknown\): operation error SSM: GetParameter, failed to bind endpoint params, invalid input region bad region`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "no region and no endpoint",
			url:     "awsparamstore://ptah-db?wait=1h",
			replies: []reply{found("ptah-db", "String", value, 1)},
		}, wantError: `runtimevar \(code=Unknown\): operation error SSM: GetParameter, failed to resolve service endpoint, endpoint rule error, Invalid Configuration: Missing Region`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "FIPS beside a configured endpoint",
			url:     "awsparamstore://ptah-db?region=us-east-1&fips=true&wait=1h",
			env:     map[string]string{"AWS_ENDPOINT_URL_SSM": "{endpoint}"},
			replies: []reply{found("ptah-db", "String", value, 1)},
		}, wantError: `.*endpoint rule error, Invalid Configuration: FIPS and custom endpoint are not supported`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:    "dual stack beside a configured endpoint",
			url:     "awsparamstore://ptah-db?region=us-east-1&dualstack=true&wait=1h",
			env:     map[string]string{"AWS_ENDPOINT_URL": "{endpoint}"},
			replies: []reply{found("ptah-db", "String", value, 1)},
		}, wantError: `.*endpoint rule error, Invalid Configuration: Dualstack and custom endpoint are not supported`, wantCode: gcerrors.Unknown},
		{urlScenario: urlScenario{
			name:          "no credentials",
			url:           "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&wait=1h",
			noCredentials: true,
			replies:       []reply{found("ptah-db", "String", value, 1)},
		}, wantError: `runtimevar \(code=Unknown\): operation error SSM: GetParameter, get identity: get credentials: failed to refresh cached credentials, .*`, wantCode: gcerrors.Unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFakeSSM(test.replies...)
			endpoint := serve(c, fake)

			reference, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
				return []observation{firstState(c, openURLScenario(c, test.urlScenario, endpoint, d))}
			})

			c.Assert(candidate, qt.DeepEquals, reference)
			c.Assert(reference.States[0].Error, qt.IsNotNil)
			c.Assert(reference.States[0].Error.Text, qt.Matches, test.wantError)
			c.Assert(reference.States[0].Error.Code, qt.Equals, test.wantCode)
			c.Assert(reference.Requests, qt.HasLen, test.wantRequests)
		})
	}
}

// TestParameterStoreURL_OpenFailurePath compares the refusals that happen
// before any request: the URL's own parameters.
func TestParameterStoreURL_OpenFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantError string
	}{
		{name: "an unknown decoder", url: "awsparamstore://ptah-db?region=us-east-1&decoder=yaml",
			wantError: `open variable awsparamstore://ptah-db\?region=us-east-1&decoder=yaml: invalid decoder: unsupported decoder "yaml"`},
		{name: "a wait that is not a duration", url: "awsparamstore://ptah-db?region=us-east-1&wait=soon",
			wantError: `open variable awsparamstore://ptah-db\?region=us-east-1&wait=soon: invalid wait "soon": time: invalid duration "soon"`},
		{name: "an empty wait reaches the AWS parameters", url: "awsparamstore://ptah-db?region=us-east-1&wait=",
			wantError: `open variable awsparamstore://ptah-db\?region=us-east-1&wait=: unknown query parameter "wait"`},
		{name: "an unknown parameter", url: "awsparamstore://ptah-db?region=us-east-1&color=blue",
			wantError: `open variable awsparamstore://ptah-db\?region=us-east-1&color=blue: unknown query parameter "color"`},
		{name: "fips that is not a boolean", url: "awsparamstore://ptah-db?fips=maybe",
			wantError: `open variable awsparamstore://ptah-db\?fips=maybe: invalid value for fips: strconv.ParseBool: parsing "maybe": invalid syntax`},
		{name: "a role that is not an ARN", url: "awsparamstore://ptah-db?role=admin",
			wantError: `open variable awsparamstore://ptah-db\?role=admin: invalid value for role: must be a valid ARN \(starts with 'arn:'\): admin`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			isolateAWSEnvironment(c)
			u, err := url.Parse(test.url)
			c.Assert(err, qt.IsNil)

			_, referenceErr := drivers[0].openURL(context.Background(), u)
			_, candidateErr := drivers[1].openURL(context.Background(), u)

			c.Assert(candidateErr, qt.ErrorMatches, test.wantError)
			c.Assert(candidateErr.Error(), qt.Equals, referenceErr.Error())
		})
	}
}

// configScenario opens a variable from a loaded configuration over the fake
// transport, with the SDK's standard retryer minus its backoff, so a retry
// scenario takes no wall time.
type configScenario struct {
	name         string
	replies      []reply
	refused      bool // send to a closed port instead of the fake
	maxAttempts  int  // aws.Config.RetryMaxAttempts, which only a program sets
	noRetryer    bool // leave aws.Config.Retryer nil, so the client builds its own
	wantRequests int
}

// openConfigScenario opens row's variable; a refused row sends to
// refusedEndpoint, which the caller picks once so both drivers dial the same
// port.
func openConfigScenario(c *qt.C, row configScenario, fake *fakeSSM, refusedEndpoint string, d driver) func() (*runtimevar.Variable, error) {
	isolateAWSEnvironment(c)
	setEnvironment(c, staticCredentials)
	cfg := testConfig(c, fake, config.WithRegion("us-east-1"))
	cfg.RetryMaxAttempts = row.maxAttempts
	if row.noRetryer {
		cfg.Retryer = nil
	}
	if row.refused {
		cfg.HTTPClient = nil
		cfg.BaseEndpoint = new(refusedEndpoint)
	}
	return func() (*runtimevar.Variable, error) {
		return d.openConfig(cfg, "ptah-db", runtimevar.StringDecoder, time.Hour)
	}
}

// closedEndpoint is the address of a port nothing listens on.
func closedEndpoint(c *qt.C) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return "http://" + address
}

// TestRetries_HappyPath gives both drivers failures the standard retryer
// retries, then the value.
func TestRetries_HappyPath(t *testing.T) {
	tests := []configScenario{
		{name: "throttling, then the value", wantRequests: 2, replies: []reply{
			failedUpper(http.StatusBadRequest, "ThrottlingException", "Rate exceeded"), found("ptah-db", "String", value, 1),
		}},
		{name: "a modeled server error, then the value", wantRequests: 2, replies: []reply{
			failedUpper(http.StatusInternalServerError, "InternalServerError", "internal"), found("ptah-db", "String", value, 1),
		}},
		{name: "an unmodeled 503, then the value", wantRequests: 2, replies: []reply{
			failed(http.StatusServiceUnavailable, "ServiceUnavailable", "busy"), found("ptah-db", "String", value, 1),
		}},
		{name: "a clock skew, corrected on the retry", wantRequests: 2, replies: []reply{
			{status: http.StatusBadRequest, body: `{"__type":"InvalidSignatureException","message":"Signature expired"}`, dateShift: 10 * time.Minute},
			found("ptah-db", "String", value, 1),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFakeSSM(test.replies...)

			reference, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
				return []observation{firstState(c, openConfigScenario(c, test, fake, "", d))}
			})

			c.Assert(candidate, qt.DeepEquals, reference)
			c.Assert(reference.States[0].Value, qt.Equals, value)
			c.Assert(reference.Requests, qt.HasLen, test.wantRequests)
		})
	}
}

// TestRetryClockSkewShiftsTheSigningTime pins what the clock skew row above
// compares: the retry is signed ten minutes ahead, where the server's clock is.
func TestRetryClockSkewShiftsTheSigningTime(t *testing.T) {
	c := qt.New(t)
	fake := newFakeSSM(
		reply{status: http.StatusBadRequest, body: `{"__type":"InvalidSignatureException","message":"Signature expired"}`, dateShift: 10 * time.Minute},
		found("ptah-db", "String", value, 1),
	)
	row := configScenario{name: "skew", replies: fake.replies}

	_, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
		return []observation{firstState(c, openConfigScenario(c, row, fake, "", d))}
	})

	c.Assert(candidate.Requests, qt.HasLen, 2)
	c.Assert(candidate.Requests[0].SigningSkew, qt.Equals, int64(0))
	c.Assert(candidate.Requests[1].SigningSkew, qt.Equals, int64(10))
}

// TestRetries_FailurePath exhausts the attempts.
func TestRetries_FailurePath(t *testing.T) {
	tests := []struct {
		configScenario
		wantError string
	}{
		{configScenario: configScenario{name: "throttling on every attempt", wantRequests: 3, replies: []reply{
			failedUpper(http.StatusBadRequest, "ThrottlingException", "Rate exceeded"),
		}}, wantError: `runtimevar \(code=Unknown\): operation error SSM: GetParameter, exceeded maximum number of attempts, 3, https response error StatusCode: 400, RequestID: request-3, ThrottlingException: Rate exceeded`},
		{configScenario: configScenario{
			name: "a configured attempt limit bounds the retryer", maxAttempts: 2, wantRequests: 2,
			replies: []reply{failedUpper(http.StatusBadRequest, "ThrottlingException", "Rate exceeded")},
		}, wantError: `.*exceeded maximum number of attempts, 2, .*`},
		{configScenario: configScenario{
			name: "a configuration without a retryer", noRetryer: true, maxAttempts: 1, wantRequests: 1,
			replies: []reply{failedUpper(http.StatusBadRequest, "ThrottlingException", "Rate exceeded")},
		}, wantError: `.*exceeded maximum number of attempts, 1, .*`},
		{configScenario: configScenario{name: "a refused connection", refused: true, replies: []reply{
			found("ptah-db", "String", value, 1),
		}}, wantError: `runtimevar \(code=Unknown\): operation error SSM: GetParameter, exceeded maximum number of attempts, 3, https response error StatusCode: 0, RequestID: , request send failed, Post "http://127\.0\.0\.1:\d+/": dial tcp 127\.0\.0\.1:\d+: .*`},
		{configScenario: configScenario{name: "access denied is not retried", wantRequests: 1, replies: []reply{
			failedUpper(http.StatusBadRequest, "AccessDeniedException", "denied"),
		}}, wantError: `.*, AccessDeniedException: denied`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFakeSSM(test.replies...)
			refused := closedEndpoint(c)

			reference, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
				return []observation{firstState(c, openConfigScenario(c, test.configScenario, fake, refused, d))}
			})

			c.Assert(candidate, qt.DeepEquals, reference)
			c.Assert(reference.States[0].Error, qt.IsNotNil)
			c.Assert(reference.States[0].Error.Text, qt.Matches, test.wantError)
			c.Assert(reference.Requests, qt.HasLen, test.wantRequests)
		})
	}
}

// TestWatchReportsChanges steps a watch one poll at a time: a new version is a
// new value, the same version is no change, a repeated error is no change, and
// an error between two reads of one version makes the second read a value
// again.
func TestWatchReportsChanges(t *testing.T) {
	tests := []struct {
		name         string
		replies      []reply
		wantNoChange []bool
	}{
		{name: "a version change while watching", wantNoChange: []bool{false, true, false, true}, replies: []reply{
			found("ptah-db", "String", "one", 1), found("ptah-db", "String", "one", 1),
			found("ptah-db", "String", "two", 2), found("ptah-db", "String", "two", 2),
		}},
		{name: "a repeated error, then another error, then a value", wantNoChange: []bool{false, true, false, false}, replies: []reply{
			failed(http.StatusBadRequest, "ParameterNotFound", "first"), failed(http.StatusBadRequest, "ParameterNotFound", "second"),
			failedUpper(http.StatusBadRequest, "AccessDeniedException", "denied"), found("ptah-db", "String", "one", 1),
		}},
		{name: "an error between two reads of one version", wantNoChange: []bool{false, false, false}, replies: []reply{
			found("ptah-db", "String", "one", 1), failed(http.StatusBadRequest, "ParameterNotFound", "gone"),
			found("ptah-db", "String", "one", 1),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFakeSSM(test.replies...).gated()
			endpoint := serve(c, fake)
			row := urlScenario{url: "awsparamstore://ptah-db?region=us-east-1&endpoint={endpoint}&decoder=string&wait=1ms"}

			reference, candidate := compareDrivers(c, fake, func(c *qt.C, d driver) []observation {
				return steppedStates(c, fake, openURLScenario(c, row, endpoint, d))
			})

			c.Assert(candidate, qt.DeepEquals, reference)
			c.Assert(noChanges(reference.States), qt.DeepEquals, test.wantNoChange)
		})
	}
}

func noChanges(states []observation) []bool {
	flags := make([]bool, 0, len(states))
	for _, state := range states {
		flags = append(flags, state.NoChange)
	}
	return flags
}
