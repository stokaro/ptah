package aiprovider_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aiprovider"
)

// hello is the smallest request the classification tests send.
func hello() aiprovider.Request {
	return aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hello"}},
	}
}

func TestProviderErrors_AreClassifiedForTheCallerToBranchOn(t *testing.T) {
	// #1488 names the failure this table exists to prevent: turning every
	// provider failure into "could not complete the task". A person whose key
	// is wrong, whose model name is wrong, and whose provider is busy need three
	// different sentences.
	tests := []struct {
		name    string
		status  int
		body    string
		want    aiprovider.ErrorKind
		wantMsg string
	}{
		{
			name:    "a refused credential",
			status:  http.StatusUnauthorized,
			body:    `{"error":{"message":"Incorrect API key provided"}}`,
			want:    aiprovider.KindAuth,
			wantMsg: "Incorrect API key provided",
		},
		{
			name:    "a forbidden credential",
			status:  http.StatusForbidden,
			body:    `{"error":{"message":"you do not have access to this model"}}`,
			want:    aiprovider.KindAuth,
			wantMsg: "you do not have access to this model",
		},
		{
			name:    "a model the endpoint does not serve",
			status:  http.StatusNotFound,
			body:    `{"error":{"message":"The model does not exist"}}`,
			want:    aiprovider.KindUnknownModel,
			wantMsg: "The model does not exist",
		},
		{
			name:    "a provider asking for less traffic",
			status:  http.StatusTooManyRequests,
			body:    `{"error":{"message":"Rate limit reached"}}`,
			want:    aiprovider.KindRateLimited,
			wantMsg: "Rate limit reached",
		},
		{
			name:    "a conversation past the context window",
			status:  http.StatusBadRequest,
			body:    `{"error":{"message":"This model's maximum context length is 8192 tokens"}}`,
			want:    aiprovider.KindTooLarge,
			wantMsg: "maximum context length",
		},
		{
			name:    "an endpoint that will not take tools",
			status:  http.StatusBadRequest,
			body:    `{"error":{"message":"tools are not supported by this deployment"}}`,
			want:    aiprovider.KindToolsUnsupported,
			wantMsg: "tools are not supported",
		},
		{
			name:    "a provider that is not serving",
			status:  http.StatusServiceUnavailable,
			body:    `{"error":{"message":"overloaded"}}`,
			want:    aiprovider.KindUnavailable,
			wantMsg: "overloaded",
		},
		{
			name:    "a proxy answering in plain text",
			status:  http.StatusBadGateway,
			body:    "upstream connect error",
			want:    aiprovider.KindUnavailable,
			wantMsg: "upstream connect error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stub := &recorder{status: test.status, body: test.body}
			cfg := openAIConfig(serve(c, stub.handler()))
			cfg.MaxRetries = 0
			provider := aiprovider.NewOpenAICompatible(cfg)

			answer, err := provider.Chat(context.Background(), hello())

			c.Assert(answer, qt.IsNil)
			c.Assert(aiprovider.KindOf(err), qt.Equals, test.want)
			c.Assert(err, qt.ErrorMatches, ".*"+test.wantMsg+".*")
			c.Assert(err, qt.ErrorMatches, "local .*",
				qt.Commentf("the message names the profile, because a session may have two"))
		})
	}
}

func TestProviderErrors_NeverCarryTheCredential(t *testing.T) {
	// The absence is structural: nothing in the package puts the key into a
	// message. The test is here because that is a property somebody could
	// remove by echoing a request header into a diagnostic.
	c := qt.New(t)
	stub := &recorder{status: http.StatusUnauthorized, body: `{"error":{"message":"invalid key"}}`}
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.APIKey = "sk-secret-value-do-not-log"
	cfg.MaxRetries = 0
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), hello())

	c.Assert(err, qt.Not(qt.ErrorMatches), "(?s).*sk-secret-value-do-not-log.*")
}

func TestUnreachableEndpoint_IsNotAProviderError(t *testing.T) {
	// A base URL that answers nothing and a provider that answered with an
	// error are different problems with different remedies.
	c := qt.New(t)
	cfg := openAIConfig("http://127.0.0.1:1/v1")
	cfg.MaxRetries = 0
	cfg.Timeout = 2 * time.Second
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), hello())

	c.Assert(aiprovider.KindOf(err), qt.Equals, aiprovider.KindUnreachable)
}

func TestMalformedAnswer_IsItsOwnClassification(t *testing.T) {
	// The common shape of an endpoint that says it is OpenAI-compatible and is
	// not. Reporting it as a provider error would send the reader to the
	// provider's status page.
	c := qt.New(t)
	stub := &recorder{body: `<!doctype html><title>hello</title>`}
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.MaxRetries = 0
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), hello())

	c.Assert(aiprovider.KindOf(err), qt.Equals, aiprovider.KindMalformedResponse)
	c.Assert(err, qt.ErrorMatches, ".*could not read.*")
}

// flaky answers with a scripted status per call.
type flaky struct {
	statuses []int
	bodies   []string
	calls    int
}

func (f *flaky) handler() http.HandlerFunc {
	return func(writer http.ResponseWriter, _ *http.Request) {
		index := min(f.calls, len(f.statuses)-1)
		f.calls++
		writer.WriteHeader(f.statuses[index])
		_, _ = writer.Write([]byte(f.bodies[index]))
	}
}

func TestRetries_RepeatWhatIsWorthRepeating(t *testing.T) {
	c := qt.New(t)
	stub := &flaky{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
		bodies: []string{
			`{"error":{"message":"slow down"}}`,
			`{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]}`,
		},
	}
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.MaxRetries = 2
	provider := aiprovider.NewOpenAICompatible(cfg)

	answer, err := provider.Chat(context.Background(), hello())

	c.Assert(err, qt.IsNil)
	c.Assert(answer.Message.Content, qt.Equals, "ok")
	c.Assert(stub.calls, qt.Equals, 2)
}

func TestRetries_DoNotRepeatWhatCannotSucceed(t *testing.T) {
	// The control. Retrying a refused credential burns the operator's time and
	// the provider's rate limit to reach the same answer.
	c := qt.New(t)
	stub := &flaky{
		statuses: []int{http.StatusUnauthorized},
		bodies:   []string{`{"error":{"message":"invalid key"}}`},
	}
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.MaxRetries = 3
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), hello())

	c.Assert(aiprovider.KindOf(err), qt.Equals, aiprovider.KindAuth)
	c.Assert(stub.calls, qt.Equals, 1)
}

func TestRetries_StopAtTheConfiguredLimit(t *testing.T) {
	c := qt.New(t)
	stub := &flaky{
		statuses: []int{http.StatusServiceUnavailable},
		bodies:   []string{`{"error":{"message":"overloaded"}}`},
	}
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.MaxRetries = 2
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), hello())

	c.Assert(aiprovider.KindOf(err), qt.Equals, aiprovider.KindUnavailable)
	c.Assert(stub.calls, qt.Equals, 3, qt.Commentf("one attempt plus two retries"))
}

func TestRetries_HonorTheProvidersOwnRetryAfter(t *testing.T) {
	// A server saying how long to wait knows better than a formula, and a
	// client that ignored it would be the reason the next answer is another 429.
	c := qt.New(t)
	stub := &recorder{
		status:  http.StatusTooManyRequests,
		body:    `{"error":{"message":"slow down"}}`,
		headers: map[string]string{"Retry-After": "7"},
	}
	waits := make([]time.Duration, 0, 2)
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.MaxRetries = 1
	cfg.Sleep = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), hello())

	c.Assert(aiprovider.KindOf(err), qt.Equals, aiprovider.KindRateLimited)
	c.Assert(waits, qt.DeepEquals, []time.Duration{7 * time.Second})
}

func TestBaseURL_JoinsWhicheverWayTheOperatorSpelledIt(t *testing.T) {
	tests := []struct {
		name     string
		suffix   string
		wantPath string
	}{
		{name: "no trailing slash", suffix: "/v1", wantPath: "/v1/chat/completions?"},
		{name: "trailing slash", suffix: "/v1/", wantPath: "/v1/chat/completions?"},
		{name: "no version segment", suffix: "", wantPath: "/chat/completions?"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stub := &recorder{
				body: `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]}`,
			}
			base := serve(c, stub.handler())
			cfg := openAIConfig(base[:len(base)-len("/v1")] + test.suffix)
			provider := aiprovider.NewOpenAICompatible(cfg)

			_, err := provider.Chat(context.Background(), hello())

			c.Assert(err, qt.IsNil)
			c.Assert(stub.paths[0], qt.Equals, test.wantPath)
		})
	}
}

func TestBaseURL_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		base string
	}{
		{name: "no scheme", base: "localhost:8080/v1"},
		{name: "empty", base: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg := openAIConfig(test.base)
			provider := aiprovider.NewOpenAICompatible(cfg)

			_, err := provider.Chat(context.Background(), hello())

			c.Assert(err, qt.ErrorMatches, ".*invalid base URL.*")
		})
	}
}
